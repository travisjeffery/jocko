package broker

import (
	"bytes"
	"context"
	"io"
	"math/rand"
	"path"
	"sync"
	"time"

	"github.com/pkg/errors"
	"github.com/travisjeffery/jocko"
	"github.com/travisjeffery/jocko/commitlog"
	"github.com/travisjeffery/jocko/protocol"
	"github.com/travisjeffery/jocko/server"
	"github.com/travisjeffery/simplelog"
)

var (
	ErrTopicExists = errors.New("topic exists already")
)

// Broker represents a broker in a Jocko cluster, like a broker in a Kafka cluster.
type Broker struct {
	sync.RWMutex
	logger *simplelog.Logger

	id          int32
	topics      map[string][]*jocko.Partition
	replicators map[*jocko.Partition]*Replicator
	brokerAddr  string
	logDir      string

	raft jocko.Raft
	serf jocko.Serf

	shutdownCh   chan struct{}
	shutdown     bool
	shutdownLock sync.Mutex
}

// New is used to instantiate a new broker.
func New(id int32, opts ...BrokerFn) (*Broker, error) {
	b := &Broker{
		id:          id,
		topics:      make(map[string][]*jocko.Partition),
		replicators: make(map[*jocko.Partition]*Replicator),
		shutdownCh:  make(chan struct{}),
	}

	for _, o := range opts {
		o(b)
	}

	port, err := addrPort(b.brokerAddr)
	if err != nil {
		return nil, err
	}

	raftPort, err := addrPort(b.raft.Addr())
	if err != nil {
		return nil, err
	}

	conn := &jocko.ClusterMember{
		ID:       b.id,
		Port:     port,
		RaftPort: raftPort,
	}

	reconcileCh := make(chan *jocko.ClusterMember, 32)
	if err := b.serf.Bootstrap(conn, reconcileCh); err != nil {
		b.logger.Info("failed to start serf: %s", err)
		return nil, err
	}

	commandCh := make(chan jocko.RaftCommand, 16)
	if err := b.raft.Bootstrap(b.serf, reconcileCh, commandCh); err != nil {
		return nil, err
	}

	go b.handleRaftCommmands(commandCh)

	return b, nil
}

func (b *Broker) Run(ctx context.Context, requestc <-chan jocko.Request, responsec chan<- jocko.Response) {
	var conn io.ReadWriter
	var header *protocol.RequestHeader
	var resp protocol.ResponseBody

	for {
		select {
		case request := <-requestc:
			conn = request.Conn
			header = request.Header

			switch req := request.Request.(type) {
			case *protocol.APIVersionsRequest:
				resp = b.handleAPIVersions(header, req)
				goto respond
			case *protocol.ProduceRequest:
				resp = b.handleProduce(header, req)
				goto respond
			case *protocol.FetchRequest:
				resp = b.handleFetch(header, req)
				goto respond
			case *protocol.OffsetsRequest:
				resp = b.handleOffsets(header, req)
				goto respond
			case *protocol.MetadataRequest:
				resp = b.handleMetadata(header, req)
				goto respond
			case *protocol.CreateTopicRequests:
				resp = b.handleCreateTopic(header, req)
				goto respond
			case *protocol.DeleteTopicsRequest:
				resp = b.handleDeleteTopics(header, req)
				goto respond
			case *protocol.LeaderAndISRRequest:
				resp = b.handleLeaderAndISR(header, req)
				goto respond
			}
		case <-ctx.Done():
			return
		}

	respond:
		responsec <- jocko.Response{Conn: conn, Header: header, Response: &protocol.Response{
			CorrelationID: header.CorrelationID,
			Body:          resp,
		}}
	}
}

// Request handling.

func (b *Broker) handleAPIVersions(header *protocol.RequestHeader, req *protocol.APIVersionsRequest) *protocol.APIVersionsResponse {
	return &protocol.APIVersionsResponse{
		APIVersions: []protocol.APIVersion{
			{APIKey: protocol.ProduceKey, MinVersion: 2, MaxVersion: 2},
			{APIKey: protocol.FetchKey},
			{APIKey: protocol.OffsetsKey},
			{APIKey: protocol.MetadataKey},
			{APIKey: protocol.LeaderAndISRKey},
			{APIKey: protocol.StopReplicaKey},
			{APIKey: protocol.GroupCoordinatorKey},
			{APIKey: protocol.JoinGroupKey},
			{APIKey: protocol.HeartbeatKey},
			{APIKey: protocol.LeaveGroupKey},
			{APIKey: protocol.SyncGroupKey},
			{APIKey: protocol.DescribeGroupsKey},
			{APIKey: protocol.ListGroupsKey},
			{APIKey: protocol.APIVersionsKey},
			{APIKey: protocol.CreateTopicsKey},
			{APIKey: protocol.DeleteTopicsKey},
		},
	}
}

func (b *Broker) handleCreateTopic(header *protocol.RequestHeader, reqs *protocol.CreateTopicRequests) *protocol.CreateTopicsResponse {
	resp := new(protocol.CreateTopicsResponse)
	resp.TopicErrorCodes = make([]*protocol.TopicErrorCode, len(reqs.Requests))
	isController := b.IsController()
	for i, req := range reqs.Requests {
		if !isController {
			resp.TopicErrorCodes[i] = &protocol.TopicErrorCode{
				Topic:     req.Topic,
				ErrorCode: protocol.ErrNotController.Code(),
			}
			continue
		}
		if req.ReplicationFactor > int16(len(b.Cluster())) {
			resp.TopicErrorCodes[i] = &protocol.TopicErrorCode{
				Topic:     req.Topic,
				ErrorCode: protocol.ErrInvalidReplicationFactor.Code(),
			}
			continue
		}
		err := b.CreateTopic(req.Topic, req.NumPartitions, req.ReplicationFactor)
		resp.TopicErrorCodes[i] = &protocol.TopicErrorCode{
			Topic:     req.Topic,
			ErrorCode: err.Code(),
		}
	}
	return resp
}

func (b *Broker) handleDeleteTopics(header *protocol.RequestHeader, reqs *protocol.DeleteTopicsRequest) *protocol.DeleteTopicsResponse {
	resp := new(protocol.DeleteTopicsResponse)
	resp.TopicErrorCodes = make([]*protocol.TopicErrorCode, len(reqs.Topics))
	isController := b.IsController()
	for i, topic := range reqs.Topics {
		if !isController {
			resp.TopicErrorCodes[i] = &protocol.TopicErrorCode{
				Topic:     topic,
				ErrorCode: protocol.ErrNotController.Code(),
			}
			continue
		}
		if err := b.DeleteTopic(topic); err != protocol.ErrNone {
			resp.TopicErrorCodes[i] = &protocol.TopicErrorCode{
				Topic:     topic,
				ErrorCode: protocol.ErrUnknown.Code(),
			}
			continue
		}
		resp.TopicErrorCodes[i] = &protocol.TopicErrorCode{
			Topic:     topic,
			ErrorCode: protocol.ErrNone.Code(),
		}
	}
	return resp
}

func (b *Broker) handleLeaderAndISR(header *protocol.RequestHeader, req *protocol.LeaderAndISRRequest) *protocol.LeaderAndISRResponse {
	resp := &protocol.LeaderAndISRResponse{
		Partitions: make([]*protocol.LeaderAndISRPartition, len(req.PartitionStates)),
	}
	setErr := func(i int, p *protocol.PartitionState, err protocol.Error) {
		resp.Partitions[i] = &protocol.LeaderAndISRPartition{
			ErrorCode: err.Code(),
			Partition: p.Partition,
			Topic:     p.Topic,
		}
	}
	for i, p := range req.PartitionStates {
		partition, err := b.Partition(p.Topic, p.Partition)
		// TODO: seems ok to have protocol.ErrUnknownTopicOrPartition here?
		if err != protocol.ErrNone {
			setErr(i, p, err)
			continue
		}
		if partition == nil {
			partition = &jocko.Partition{
				Topic:                   p.Topic,
				ID:                      p.Partition,
				Replicas:                p.Replicas,
				ISR:                     p.ISR,
				Leader:                  p.Leader,
				PreferredLeader:         p.Leader,
				LeaderAndISRVersionInZK: p.ZKVersion,
			}
			if err := b.StartReplica(partition); err != protocol.ErrNone {
				setErr(i, p, err)
				continue
			}
		}
		if p.Leader == b.ID() && !partition.IsLeader(b.ID()) {
			// is command asking this broker to be the new leader for p and this broker is not already the leader for
			if err := b.BecomeLeader(partition.Topic, partition.ID, p); err != protocol.ErrNone {
				setErr(i, p, err)
				continue
			}
		} else if contains(p.Replicas, b.ID()) && !partition.IsFollowing(p.Leader) {
			// is command asking this broker to follow leader who it isn't a leader of already
			if err := b.BecomeFollower(partition.Topic, partition.ID, p); err != protocol.ErrNone {
				setErr(i, p, err)
				continue
			}
		}
	}
	return resp
}

func (b *Broker) handleOffsets(header *protocol.RequestHeader, req *protocol.OffsetsRequest) *protocol.OffsetsResponse {
	oResp := new(protocol.OffsetsResponse)
	oResp.Responses = make([]*protocol.OffsetResponse, len(req.Topics))
	for i, t := range req.Topics {
		oResp.Responses[i] = new(protocol.OffsetResponse)
		oResp.Responses[i].Topic = t.Topic
		oResp.Responses[i].PartitionResponses = make([]*protocol.PartitionResponse, len(t.Partitions))
		for j, p := range t.Partitions {
			pResp := new(protocol.PartitionResponse)
			pResp.Partition = p.Partition
			partition, err := b.Partition(t.Topic, p.Partition)
			if err != protocol.ErrNone {
				pResp.ErrorCode = err.Code()
				continue
			}
			var offset int64
			if p.Timestamp == -2 {
				offset = partition.LowWatermark()
			} else {
				offset = partition.HighWatermark()
			}
			pResp.Offsets = []int64{offset}
			oResp.Responses[i].PartitionResponses[j] = pResp
		}
	}
	return oResp
}

func (b *Broker) handleProduce(header *protocol.RequestHeader, req *protocol.ProduceRequest) *protocol.ProduceResponses {
	resp := new(protocol.ProduceResponses)
	resp.Responses = make([]*protocol.ProduceResponse, len(req.TopicData))
	for i, td := range req.TopicData {
		presps := make([]*protocol.ProducePartitionResponse, len(td.Data))
		for j, p := range td.Data {
			partition := jocko.NewPartition(td.Topic, p.Partition)
			presp := &protocol.ProducePartitionResponse{}
			partition, err := b.Partition(td.Topic, p.Partition)
			if err != protocol.ErrNone {
				presp.ErrorCode = err.Code()
			}
			if !b.IsLeaderOfPartition(partition.Topic, partition.ID, partition.LeaderID()) {
				presp.ErrorCode = protocol.ErrNotLeaderForPartition.Code()
				// break ?
			}
			offset, appendErr := partition.Append(p.RecordSet)
			if appendErr != nil {
				b.logger.Info("commitlog/append failed: %v", err)
				presp.ErrorCode = protocol.ErrUnknown.Code()
			}
			presp.Partition = p.Partition
			presp.BaseOffset = offset
			presp.Timestamp = time.Now().Unix()
			presps[j] = presp
		}
		resp.Responses[i] = &protocol.ProduceResponse{
			Topic:              td.Topic,
			PartitionResponses: presps,
		}
	}
	return resp
}

func (b *Broker) handleMetadata(header *protocol.RequestHeader, req *protocol.MetadataRequest) *protocol.MetadataResponse {
	brokers := make([]*protocol.Broker, 0, len(b.Cluster()))
	for _, b := range b.Cluster() {
		brokers = append(brokers, &protocol.Broker{
			NodeID: b.ID,
			Host:   b.IP,
			Port:   int32(b.Port),
		})
	}
	var topicMetadata []*protocol.TopicMetadata
	topicMetadataFn := func(topic string, partitions []*jocko.Partition, err protocol.Error) *protocol.TopicMetadata {
		partitionMetadata := make([]*protocol.PartitionMetadata, len(partitions))
		for i, p := range partitions {
			partitionMetadata[i] = &protocol.PartitionMetadata{
				ParititionID: p.ID,
			}
		}
		return &protocol.TopicMetadata{
			TopicErrorCode:    err.Code(),
			Topic:             topic,
			PartitionMetadata: partitionMetadata,
		}
	}
	if len(req.Topics) == 0 {
		// Respond with metadata for all topics
		topics := b.Topics()
		topicMetadata = make([]*protocol.TopicMetadata, len(topics))
		idx := 0
		for topic, partitions := range topics {
			topicMetadata[idx] = topicMetadataFn(topic, partitions, protocol.ErrNone)
			idx++
		}
	} else {
		topicMetadata = make([]*protocol.TopicMetadata, len(req.Topics))
		for i, topic := range req.Topics {
			partitions, err := b.TopicPartitions(topic)
			topicMetadata[i] = topicMetadataFn(topic, partitions, err)
		}
	}
	resp := &protocol.MetadataResponse{
		Brokers:       brokers,
		TopicMetadata: topicMetadata,
	}
	return resp
}

func (b *Broker) handleFetch(header *protocol.RequestHeader, r *protocol.FetchRequest) *protocol.FetchResponses {
	fresp := &protocol.FetchResponses{
		Responses: make([]*protocol.FetchResponse, len(r.Topics)),
	}
	received := time.Now()
	for i, topic := range r.Topics {
		fr := &protocol.FetchResponse{
			Topic:              topic.Topic,
			PartitionResponses: make([]*protocol.FetchPartitionResponse, len(topic.Partitions)),
		}

		for j, p := range topic.Partitions {
			partition, err := b.Partition(topic.Topic, p.Partition)
			if err != protocol.ErrNone {
				fr.PartitionResponses[j] = &protocol.FetchPartitionResponse{
					Partition: p.Partition,
					ErrorCode: err.Code(),
				}
				continue
			}
			if !b.IsLeaderOfPartition(partition.Topic, partition.ID, partition.LeaderID()) {
				fr.PartitionResponses[j] = &protocol.FetchPartitionResponse{
					Partition: p.Partition,
					ErrorCode: protocol.ErrNotLeaderForPartition.Code(),
				}
				continue
			}
			rdr, rdrErr := partition.NewReader(p.FetchOffset, p.MaxBytes)
			if rdrErr != nil {
				fr.PartitionResponses[j] = &protocol.FetchPartitionResponse{
					Partition: p.Partition,
					ErrorCode: protocol.ErrUnknown.Code(),
				}
				continue
			}
			b := new(bytes.Buffer)
			var n int32
			for n < r.MinBytes {
				if r.MaxWaitTime != 0 && int32(time.Since(received).Nanoseconds()/1e6) > r.MaxWaitTime {
					break
				}
				// TODO: copy these bytes to outer bytes
				nn, err := io.Copy(b, rdr)
				if err != nil && err != io.EOF {
					fr.PartitionResponses[j] = &protocol.FetchPartitionResponse{
						Partition: p.Partition,
						ErrorCode: protocol.ErrUnknown.Code(),
					}
					break
				}
				n += int32(nn)
				if err == io.EOF {
					break
				}
			}

			fr.PartitionResponses[j] = &protocol.FetchPartitionResponse{
				Partition:     p.Partition,
				ErrorCode:     protocol.ErrNone.Code(),
				HighWatermark: partition.HighWatermark(),
				RecordSet:     b.Bytes(),
			}
		}

		fresp.Responses[i] = fr
	}
	return fresp
}

// TODO: can probably remove a bunch of these APIs after the refactoring the
// server and broker.

// ID is used to get the broker's ID
func (b *Broker) ID() int32 {
	return b.id
}

// Cluster is used to get a list of members in the cluster.
func (b *Broker) Cluster() []*jocko.ClusterMember {
	return b.serf.Cluster()
}

// IsController returns true if this is the cluster controller.
func (b *Broker) IsController() bool {
	return b.raft.IsLeader()
}

// TopicPartitions is used to get the partitions for the given topic.
func (b *Broker) TopicPartitions(topic string) (found []*jocko.Partition, err protocol.Error) {
	b.RLock()
	defer b.RUnlock()
	if p, ok := b.topics[topic]; !ok {
		return nil, protocol.ErrUnknownTopicOrPartition
	} else {
		return p, protocol.ErrNone
	}
}

func (b *Broker) Topics() map[string][]*jocko.Partition {
	b.RLock()
	defer b.RUnlock()
	return b.topics
}

func (b *Broker) Partition(topic string, partition int32) (*jocko.Partition, protocol.Error) {
	found, err := b.TopicPartitions(topic)
	if err != protocol.ErrNone {
		return nil, err
	}
	for _, f := range found {
		if f.ID == partition {
			return f, protocol.ErrNone
		}
	}
	return nil, protocol.ErrUnknownTopicOrPartition
}

// AddPartition is used to add a partition across the cluster.
func (b *Broker) AddPartition(partition *jocko.Partition) error {
	return b.raftApply(addPartition, partition)
}

// ClusterMember is used to get a specific member in the cluster.
func (b *Broker) ClusterMember(id int32) *jocko.ClusterMember {
	return b.serf.Member(id)
}

// StartReplica is used to start a replica on this, including creating its commit log.
func (b *Broker) StartReplica(partition *jocko.Partition) protocol.Error {
	b.Lock()
	defer b.Unlock()
	if v, ok := b.topics[partition.Topic]; ok {
		b.topics[partition.Topic] = append(v, partition)
	} else {
		b.topics[partition.Topic] = []*jocko.Partition{partition}
	}
	isLeader := partition.Leader == b.id
	isFollower := false
	for _, r := range partition.Replicas {
		if r == b.id {
			isFollower = true
		}
	}
	if isLeader || isFollower {
		commitLog, err := commitlog.New(commitlog.Options{
			Path:            path.Join(b.logDir, partition.String()),
			MaxSegmentBytes: 1024,
			MaxLogBytes:     -1,
		})
		if err != nil {
			return protocol.ErrUnknown.WithErr(err)
		}
		partition.CommitLog = commitLog
		partition.Conn = b.serf.Member(partition.LeaderID())
	}
	return protocol.ErrNone
}

// IsLeaderPartitions returns true if the given leader is the leader of the given partition.
func (b *Broker) IsLeaderOfPartition(topic string, pid int32, lid int32) bool {
	b.RLock()
	defer b.RUnlock()
	result := false
	for _, p := range b.topics[topic] {
		if p.ID == pid {
			result = lid == p.LeaderID()
			break
		}
	}
	return result
}

// Join is used to have the broker join the gossip ring.
// The given address should be another broker listening on the Serf address.
func (b *Broker) Join(addrs ...string) protocol.Error {
	if _, err := b.serf.Join(addrs...); err != nil {
		return protocol.ErrUnknown.WithErr(err)
	}
	return protocol.ErrNone
}

// CreateTopic is used to create the topic across the cluster.
func (b *Broker) CreateTopic(topic string, partitions int32, replicationFactor int16) protocol.Error {
	for t, _ := range b.Topics() {
		if t == topic {
			return protocol.ErrTopicAlreadyExists
		}
	}

	c := b.Cluster()
	cLen := int32(len(c))

	for i := int32(0); i < partitions; i++ {
		leader := c[i%cLen].ID
		replicas := []int32{leader}
		for replica := rand.Int31n(cLen); len(replicas) < int(replicationFactor); replica++ {
			if replica != leader {
				replicas = append(replicas, replica)
			}
			if replica+1 == cLen {
				replica = -1
			}
		}
		partition := &jocko.Partition{
			Topic:           topic,
			ID:              i,
			Leader:          leader,
			PreferredLeader: leader,
			Replicas:        replicas,
			ISR:             replicas,
		}
		if err := b.AddPartition(partition); err != nil {
			return protocol.ErrUnknown.WithErr(err)
		}
	}
	return protocol.ErrNone
}

// DeleteTopic is used to delete the topic across the cluster.
func (b *Broker) DeleteTopic(topic string) protocol.Error {
	if err := b.raftApply(deleteTopic, &jocko.Partition{Topic: topic}); err != nil {
		return protocol.ErrUnknown.WithErr(err)
	}
	return protocol.ErrNone
}

// deleteTopic is used to delete the topic from this.
func (b *Broker) deleteTopic(tp *jocko.Partition) error {
	partitions, err := b.TopicPartitions(tp.Topic)
	if err != protocol.ErrNone {
		return err
	}
	for _, p := range partitions {
		if err := p.Delete(); err != nil {
			return err
		}
	}
	b.Lock()
	delete(b.topics, tp.Topic)
	b.Unlock()
	return nil
}

// Shutdown is used to shutdown the broker, its serf, its raft, and so on.
func (b *Broker) Shutdown() error {
	b.logger.Info("shutting down broker")
	b.shutdownLock.Lock()
	defer b.shutdownLock.Unlock()

	if b.shutdown {
		return nil
	}

	b.shutdown = true
	close(b.shutdownCh)

	if b.serf != nil {
		if err := b.serf.Shutdown(); err != nil {
			b.logger.Info("failed to shut down serf: %v", err)
			return err
		}
	}

	if b.raft != nil {
		if err := b.raft.Shutdown(); err != nil {
			b.logger.Info("failed to shut down raft: %v", err)
			return err
		}
	}

	return nil
}

// Replication.

func (b *Broker) BecomeFollower(topic string, partitionID int32, partitionState *protocol.PartitionState) protocol.Error {
	p, err := b.Partition(topic, partitionID)
	if err != protocol.ErrNone {
		return err
	}
	// stop replicator to current leader
	b.Lock()
	defer b.Unlock()
	if r, ok := b.replicators[p]; ok {
		if err := r.Close(); err != nil {
			return protocol.ErrUnknown.WithErr(err)
		}
	}
	delete(b.replicators, p)
	hw := p.HighWatermark()
	if err := p.TruncateTo(hw); err != nil {
		return protocol.ErrUnknown.WithErr(err)
	}
	p.Leader = partitionState.Leader
	p.Conn = b.ClusterMember(p.LeaderID())
	r := NewReplicator(p, b.ID(),
		ReplicatorLeader(server.NewClient(p.Conn)))
	b.replicators[p] = r
	return protocol.ErrNone
}

func (b *Broker) BecomeLeader(topic string, partitionID int32, partitionState *protocol.PartitionState) protocol.Error {
	p, err := b.Partition(topic, partitionID)
	if err != protocol.ErrNone {
		return err
	}
	b.Lock()
	defer b.Unlock()
	if r, ok := b.replicators[p]; ok {
		if err := r.Close(); err != nil {
			return protocol.ErrUnknown.WithErr(err)
		}
	}
	p.Leader = b.ID()
	p.Conn = b.ClusterMember(p.LeaderID())
	p.ISR = partitionState.ISR
	p.LeaderAndISRVersionInZK = partitionState.ZKVersion
	return protocol.ErrNone
}

func contains(rs []int32, r int32) bool {
	for _, ri := range rs {
		if ri == r {
			return true
		}
	}
	return false
}
