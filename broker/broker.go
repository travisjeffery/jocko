package broker

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"math/rand"
	"path"
	"sync"
	"time"

	"github.com/davecgh/go-spew/spew"
	"github.com/pkg/errors"
	"github.com/travisjeffery/jocko"
	"github.com/travisjeffery/jocko/commitlog"
	"github.com/travisjeffery/jocko/log"
	"github.com/travisjeffery/jocko/protocol"
	"github.com/travisjeffery/jocko/server"
)

var (
	ErrTopicExists     = errors.New("topic exists already")
	ErrInvalidArgument = errors.New("no logger set")
)

// Broker represents a broker in a Jocko cluster, like a broker in a Kafka cluster.
type Broker struct {
	sync.RWMutex
	logger log.Logger

	id          int32
	topicMap    map[string][]*Partition
	replicators map[*Partition]*Replicator
	controller  bool
	brokerAddr  string
	logDir      string
	// loner should only be used for tests. It makes it so cluster operations
	// don't go through raft.
	loner bool

	raft         jocko.Raft
	serf         jocko.Serf
	raftCommands chan jocko.RaftCommand

	shutdownCh   chan struct{}
	shutdown     bool
	shutdownLock sync.Mutex
}

// New is used to instantiate a new broker.
func New(conf jocko.Config) (*Broker, error) {
	b := &Broker{
		logDir:      conf.DataDir + "/logs", // TODO do this right
		brokerAddr:  conf.BindAddrLAN,
		raft:        conf.Raft,
		serf:        conf.Serf,
		id:          conf.ID,
		topicMap:    make(map[string][]*Partition),
		replicators: make(map[*Partition]*Replicator),
		shutdownCh:  make(chan struct{}),
	}

	if b.logger == nil {
		return nil, ErrInvalidArgument
	}

	b.logger = b.logger.With(log.String("ctx", "broker"), log.Int32("id", conf.ID), log.String("addr", b.brokerAddr))

	if b.raftCommands == nil {
		b.raftCommands = make(chan jocko.RaftCommand, 16)
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
		b.logger.Error("failed to start serf", log.Error("error", err))
		return nil, err
	}

	raftEvents := make(chan jocko.RaftEvent)
	if err := b.raft.Bootstrap(b.serf, reconcileCh, b.raftCommands); err != nil {
		return nil, err
	}

	go b.handleRaftCommmands(b.raftCommands)
	go b.handleRaftEvents(raftEvents)

	return b, nil
}

// jocko.Broker API.

func (b *Broker) ID() int32 {
	return b.id
}

// Run starts a loop to handle requests send back responses.
func (b *Broker) Run(ctx context.Context, requestc <-chan jocko.Request, responsec chan<- jocko.Response) {
	var conn io.ReadWriter
	var header *protocol.RequestHeader
	var resp protocol.ResponseBody

	for {
		select {
		case request := <-requestc:
			conn = request.Conn
			header = request.Header

			b.logger.Debug("request", log.Bool("controller", b.controller), log.Any("request", request))

			switch req := request.Request.(type) {
			case *protocol.APIVersionsRequest:
				resp = b.handleAPIVersions(header, req)
			case *protocol.ProduceRequest:
				resp = b.handleProduce(header, req)
			case *protocol.FetchRequest:
				resp = b.handleFetch(header, req)
			case *protocol.OffsetsRequest:
				resp = b.handleOffsets(header, req)
			case *protocol.MetadataRequest:
				resp = b.handleMetadata(header, req)
			case *protocol.CreateTopicRequests:
				resp = b.handleCreateTopic(header, req)
			case *protocol.DeleteTopicsRequest:
				resp = b.handleDeleteTopics(header, req)
			case *protocol.LeaderAndISRRequest:
				resp = b.handleLeaderAndISR(header, req)
			}
		case <-ctx.Done():
			return
		}

		responsec <- jocko.Response{Conn: conn, Header: header, Response: &protocol.Response{
			CorrelationID: header.CorrelationID,
			Body:          resp,
		}}
	}
}

// Join is used to have the broker join the gossip ring.
// The given address should be another broker listening on the Serf address.
func (b *Broker) Join(addrs ...string) protocol.Error {
	if _, err := b.serf.Join(addrs...); err != nil {
		return protocol.ErrUnknown.WithErr(err)
	}
	return protocol.ErrNone
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
	fmt.Println("raft leader:", b.raft.LeaderID())
	spew.Dump("broker header:", header)
	spew.Dump("broker reqs:", reqs)
	spew.Dump(reqs)
	fmt.Println("cluster member:", b.clusterMembers())
	isController := b.isController()
	if !isController {
		for _, cl := range b.clusterMembers() {
			if b.raft.LeaderID() == fmt.Sprintf("%s:%d", cl.Addr().IP.String(), cl.RaftPort) {
				conn := b.clusterMember(cl.ID)
				client := server.NewClient(conn)
				resp, err := client.CreateTopics(fmt.Sprintf("broker-%d", b.id), reqs)
				if err != nil {
					panic(err)
				}
				return resp
			}
		}
	}
	for i, req := range reqs.Requests {
		if !isController {
			b.logger.Debug("attempt to create topic on non-controller", log.String("topic", req.Topic), log.Bool("controller", b.controller))
			resp.TopicErrorCodes[i] = &protocol.TopicErrorCode{
				Topic:     req.Topic,
				ErrorCode: protocol.ErrNotController.Code(),
			}
			continue
		}
		if req.ReplicationFactor > int16(len(b.clusterMembers())) {
			resp.TopicErrorCodes[i] = &protocol.TopicErrorCode{
				Topic:     req.Topic,
				ErrorCode: protocol.ErrInvalidReplicationFactor.Code(),
			}
			continue
		}
		if b.loner {
			partitions := b.partitionsToCreate(req.Topic, req.NumPartitions, req.ReplicationFactor)
			err := protocol.ErrNone
			for _, p := range partitions {
				if err = b.startReplica(p); err != protocol.ErrNone {
					break
				}
			}
			resp.TopicErrorCodes[i] = &protocol.TopicErrorCode{
				Topic:     req.Topic,
				ErrorCode: err.Code(),
			}
			continue
		}
		b.logger.Debug("creating topic", log.Bool("controller", b.controller), log.String("topic", req.Topic), log.Int32("partitions", req.NumPartitions), log.Int16("replication factor", req.ReplicationFactor))
		err := b.createTopic(req.Topic, req.NumPartitions, req.ReplicationFactor)
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
	isController := b.isController()
	for i, topic := range reqs.Topics {
		if !isController {
			resp.TopicErrorCodes[i] = &protocol.TopicErrorCode{
				Topic:     topic,
				ErrorCode: protocol.ErrNotController.Code(),
			}
			continue
		}
		if err := b.deleteTopic(topic); err != protocol.ErrNone {
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
		partition, err := b.partition(p.Topic, p.Partition)
		if err != protocol.ErrUnknownTopicOrPartition && err != protocol.ErrNone {
			setErr(i, p, err)
			continue
		}
		if partition == nil {
			partition = &Partition{
				Topic:                   p.Topic,
				ID:                      p.Partition,
				Replicas:                p.Replicas,
				ISR:                     p.ISR,
				Leader:                  p.Leader,
				PreferredLeader:         p.Leader,
				LeaderAndISRVersionInZK: p.ZKVersion,
			}
			if err := b.startReplica(partition); err != protocol.ErrNone {
				setErr(i, p, err)
				continue
			}
		}
		if p.Leader == b.id && !partition.IsLeader(b.id) {
			// is command asking this broker to be the new leader for p and this broker is not already the leader for
			if err := b.becomeLeader(partition.Topic, partition.ID, p); err != protocol.ErrNone {
				setErr(i, p, err)
				continue
			}
		} else if contains(p.Replicas, b.id) && !partition.IsFollowing(p.Leader) {
			// is command asking this broker to follow leader who it isn't a leader of already
			if err := b.becomeFollower(partition.Topic, partition.ID, p); err != protocol.ErrNone {
				setErr(i, p, err)
				continue
			}
		}
		resp.Partitions[i] = &protocol.LeaderAndISRPartition{Partition: p.Partition, Topic: p.Topic, ErrorCode: protocol.ErrNone.Code()}
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
			partition, err := b.partition(t.Topic, p.Partition)
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
			presp := &protocol.ProducePartitionResponse{}
			partition, err := b.partition(td.Topic, p.Partition)
			if err != protocol.ErrNone {
				b.logger.Error("produce to partition failed", log.Error("error", err))
				presp.Partition = p.Partition
				presp.ErrorCode = err.Code()
				presps[j] = presp
				continue
			}
			offset, appendErr := partition.Append(p.RecordSet)
			if appendErr != nil {
				b.logger.Error("commitlog/append failed", log.Error("error", err))
				presp.ErrorCode = protocol.ErrUnknown.Code()
				presps[j] = presp
				continue
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
	brokers := make([]*protocol.Broker, 0, len(b.clusterMembers()))
	for _, b := range b.clusterMembers() {
		brokers = append(brokers, &protocol.Broker{
			NodeID: b.ID,
			Host:   b.IP,
			Port:   int32(b.Port),
		})
	}
	var topicMetadata []*protocol.TopicMetadata
	topicMetadataFn := func(topic string, partitions []*Partition, err protocol.Error) *protocol.TopicMetadata {
		if err != protocol.ErrNone {
			return &protocol.TopicMetadata{
				TopicErrorCode: err.Code(),
				Topic:          topic,
			}
		}
		partitionMetadata := make([]*protocol.PartitionMetadata, len(partitions))
		for i, p := range partitions {
			partitionMetadata[i] = &protocol.PartitionMetadata{
				ParititionID:       p.ID,
				PartitionErrorCode: protocol.ErrNone.Code(),
				Leader:             p.Leader,
				Replicas:           p.Replicas,
				ISR:                p.ISR,
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
		topics := b.topics()
		topicMetadata = make([]*protocol.TopicMetadata, len(topics))
		idx := 0
		for topic, partitions := range topics {
			topicMetadata[idx] = topicMetadataFn(topic, partitions, protocol.ErrNone)
			idx++
		}
	} else {
		topicMetadata = make([]*protocol.TopicMetadata, len(req.Topics))
		for i, topic := range req.Topics {
			partitions, err := b.topicPartitions(topic)
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
			partition, err := b.partition(topic.Topic, p.Partition)
			if err != protocol.ErrNone {
				fr.PartitionResponses[j] = &protocol.FetchPartitionResponse{
					Partition: p.Partition,
					ErrorCode: err.Code(),
				}
				continue
			}
			if !partition.IsLeader(b.id) {
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

// clusterMembers is used to get a list of members in the cluster.
func (b *Broker) clusterMembers() []*jocko.ClusterMember {
	return b.serf.Cluster()
}

// isController returns true if this is the cluster controller.
func (b *Broker) isController() bool {
	return b.raft.IsLeader()
}

// topicPartitions is used to get the partitions for the given topic.
func (b *Broker) topicPartitions(topic string) (found []*Partition, err protocol.Error) {
	b.RLock()
	defer b.RUnlock()
	if p, ok := b.topicMap[topic]; ok {
		return p, protocol.ErrNone
	} else {
		return nil, protocol.ErrUnknownTopicOrPartition
	}
}

func (b *Broker) topics() map[string][]*Partition {
	b.RLock()
	defer b.RUnlock()
	return b.topicMap
}

func (b *Broker) partition(topic string, partition int32) (*Partition, protocol.Error) {
	found, err := b.topicPartitions(topic)
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

// createPartition is used to add a partition across the cluster.
func (b *Broker) createPartition(partition *Partition) error {
	return b.raftApply(createPartition, partition)
}

// clusterMember is used to get a specific member in the cluster.
func (b *Broker) clusterMember(id int32) *jocko.ClusterMember {
	return b.serf.Member(id)
}

// startReplica is used to start a replica on this, including creating its commit log.
func (b *Broker) startReplica(partition *Partition) protocol.Error {
	b.Lock()
	defer b.Unlock()
	b.logger.Debug("start replica", log.Bool("controller", b.controller), log.Int32("partition id", partition.ID))
	if v, ok := b.topicMap[partition.Topic]; ok {
		b.topicMap[partition.Topic] = append(v, partition)
	} else {
		b.topicMap[partition.Topic] = []*Partition{partition}
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

// createTopic is used to create the topic across the cluster.
func (b *Broker) createTopic(topic string, partitions int32, replicationFactor int16) protocol.Error {
	for t, _ := range b.topics() {
		if t == topic {
			return protocol.ErrTopicAlreadyExists
		}
	}
	for _, partition := range b.partitionsToCreate(topic, partitions, replicationFactor) {
		if err := b.createPartition(partition); err != nil {
			return protocol.ErrUnknown.WithErr(err)
		}
	}
	return protocol.ErrNone
}

func (b *Broker) partitionsToCreate(topic string, partitionsCount int32, replicationFactor int16) []*Partition {
	mems := b.clusterMembers()
	memCount := int32(len(mems))
	var partitions []*Partition

	for i := int32(0); i < partitionsCount; i++ {
		leader := mems[i%memCount].ID
		replicas := []int32{leader}
		for replica := rand.Int31n(memCount); len(replicas) < int(replicationFactor); replica++ {
			if replica != leader {
				replicas = append(replicas, replica)
			}
			if replica+1 == memCount {
				replica = -1
			}
		}
		partition := &Partition{
			Topic:           topic,
			ID:              i,
			Leader:          leader,
			PreferredLeader: leader,
			Replicas:        replicas,
			ISR:             replicas,
		}
		partitions = append(partitions, partition)
	}
	return partitions
}

// deleteTopic is used to delete the topic across the cluster.
func (b *Broker) deleteTopic(topic string) protocol.Error {
	if err := b.raftApply(deleteTopic, &Partition{Topic: topic}); err != nil {
		return protocol.ErrUnknown.WithErr(err)
	}
	return protocol.ErrNone
}

// deletePartitions is used to delete the topic from this.
func (b *Broker) deletePartitions(tp *Partition) error {
	partitions, err := b.topicPartitions(tp.Topic)
	if err != protocol.ErrNone {
		return err
	}
	for _, p := range partitions {
		if err := p.Delete(); err != nil {
			return err
		}
	}
	b.Lock()
	delete(b.topicMap, tp.Topic)
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
	defer close(b.shutdownCh)

	if b.serf != nil {
		if err := b.serf.Shutdown(); err != nil {
			b.logger.Error("failed to shut down serf", log.Error("error", err))
			return err
		}
	}

	if b.raft != nil {
		if err := b.raft.Shutdown(); err != nil {
			b.logger.Error("failed to shut down raft", log.Error("error", err))
			return err
		}
	}

	return nil
}

// Replication.

func (b *Broker) becomeFollower(topic string, partitionID int32, partitionState *protocol.PartitionState) protocol.Error {
	p, err := b.partition(topic, partitionID)
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
	if err := p.Truncate(hw); err != nil {
		return protocol.ErrUnknown.WithErr(err)
	}
	p.Leader = partitionState.Leader
	p.Conn = b.clusterMember(p.LeaderID())
	r := NewReplicator(b.logger, p, b.id,
		ReplicatorLeader(server.NewClient(p.Conn)))
	b.replicators[p] = r
	if !b.loner {
		r.Replicate()
	}
	return protocol.ErrNone
}

func (b *Broker) becomeLeader(topic string, partitionID int32, partitionState *protocol.PartitionState) protocol.Error {
	p, err := b.partition(topic, partitionID)
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
	p.Leader = b.id
	p.Conn = b.clusterMember(p.LeaderID())
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

func (b *Broker) handleRaftEvents(eventCh chan jocko.RaftEvent) {
	for {
		select {
		case <-b.shutdownCh:
			return
		case e := <-eventCh:
			switch e.Op {
			case "acquired-leadership":
				b.Lock()
				b.controller = true
				b.Unlock()
			case "lost-leadership":
				b.Lock()
				b.controller = false
				b.Unlock()
			}
		}
	}
}
