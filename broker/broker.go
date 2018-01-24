package broker

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"math/rand"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb"
	"github.com/hashicorp/serf/serf"
	"github.com/pkg/errors"
	"github.com/travisjeffery/jocko"
	"github.com/travisjeffery/jocko/broker/config"
	"github.com/travisjeffery/jocko/broker/fsm"
	"github.com/travisjeffery/jocko/broker/metadata"
	"github.com/travisjeffery/jocko/commitlog"
	"github.com/travisjeffery/jocko/log"
	"github.com/travisjeffery/jocko/protocol"
	"github.com/travisjeffery/jocko/server"
)

const (
	serfLANSnapshot   = "serf/local.snapshot"
	raftState         = "raft/"
	raftLogCacheSize  = 512
	snapshotsRetained = 2
)

var (
	ErrTopicExists     = errors.New("topic exists already")
	ErrInvalidArgument = errors.New("no logger set")
)

// Broker represents a broker in a Jocko cluster, like a broker in a Kafka cluster.
type Broker struct {
	sync.RWMutex
	logger log.Logger
	config *config.Config

	topicMap    map[string][]*jocko.Partition
	replicators map[*jocko.Partition]*Replicator

	// readyForConsistentReads is used to track when the leader server is
	// ready to serve consistent reads, after it has applied its initial
	// barrier. This is updated atomically.
	readyForConsistentReads int32
	// serverLookup tracks servers in the local datacenter.
	serverLookup *serverLookup
	// The raft instance is used among Jocko brokers within the DC to protect operations that require strong consistency.
	raft          *raft.Raft
	raftStore     *raftboltdb.BoltStore
	raftTransport *raft.NetworkTransport
	raftInmem     *raft.InmemStore
	// raftNotifyCh ensures we get reliable leader transition notifications from the raft layer.
	raftNotifyCh <-chan bool
	// reconcileCh is used to pass events from the serf handler to the raft leader to update its state.
	reconcileCh chan serf.Member
	serf        *serf.Serf
	fsm         *fsm.FSM
	eventChLAN  chan serf.Event

	shutdownCh   chan struct{}
	shutdown     bool
	shutdownLock sync.Mutex
}

// New is used to instantiate a new broker.
func New(config *config.Config, logger log.Logger) (*Broker, error) {
	b := &Broker{
		config:       config,
		logger:       logger.With(log.Int32("id", config.ID), log.String("raft addr", config.RaftAddr)),
		topicMap:     make(map[string][]*jocko.Partition),
		replicators:  make(map[*jocko.Partition]*Replicator),
		shutdownCh:   make(chan struct{}),
		eventChLAN:   make(chan serf.Event, 256),
		serverLookup: NewServerLookup(),
		reconcileCh:  make(chan serf.Member, 32),
	}

	if b.logger == nil {
		return nil, ErrInvalidArgument
	}

	b.logger.Info("hello")

	if err := b.setupRaft(); err != nil {
		b.Shutdown()
		return nil, fmt.Errorf("failed to start raft: %v", err)
	}

	var err error
	b.serf, err = b.setupSerf(config.SerfLANConfig, b.eventChLAN, serfLANSnapshot)
	if err != nil {
		return nil, err
	}

	go b.lanEventHandler()

	go b.monitorLeadership()

	return b, nil
}

// jocko.Broker API.

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
func (b *Broker) JoinLAN(addrs ...string) protocol.Error {
	if _, err := b.serf.Join(addrs, true); err != nil {
		return protocol.ErrUnknown.WithErr(err)
	}
	return protocol.ErrNone
}

// Request handling.

var (
	APIVersions = &protocol.APIVersionsResponse{
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
)

func (b *Broker) handleAPIVersions(header *protocol.RequestHeader, req *protocol.APIVersionsRequest) *protocol.APIVersionsResponse {
	return APIVersions
}

func (b *Broker) handleCreateTopic(header *protocol.RequestHeader, reqs *protocol.CreateTopicRequests) *protocol.CreateTopicsResponse {
	resp := new(protocol.CreateTopicsResponse)
	resp.TopicErrorCodes = make([]*protocol.TopicErrorCode, len(reqs.Requests))
	isController := b.isController()
	for i, req := range reqs.Requests {
		if !isController {
			resp.TopicErrorCodes[i] = &protocol.TopicErrorCode{
				Topic:     req.Topic,
				ErrorCode: protocol.ErrNotController.Code(),
			}
			continue
		}
		if req.ReplicationFactor > int16(len(b.LANMembers())) {
			resp.TopicErrorCodes[i] = &protocol.TopicErrorCode{
				Topic:     req.Topic,
				ErrorCode: protocol.ErrInvalidReplicationFactor.Code(),
			}
			continue
		}
		if b.config.DevMode {
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
			partition = &jocko.Partition{
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
		if p.Leader == b.config.ID && !partition.IsLeader(b.config.ID) {
			// is command asking this broker to be the new leader for p and this broker is not already the leader for
			if err := b.becomeLeader(partition.Topic, partition.ID, p); err != protocol.ErrNone {
				setErr(i, p, err)
				continue
			}
		} else if contains(p.Replicas, b.config.ID) && !partition.IsFollowing(p.Leader) {
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
	brokers := make([]*protocol.Broker, 0, len(b.LANMembers()))
	for _, mem := range b.LANMembers() {
		m, ok := metadata.IsBroker(mem)
		if !ok {
			continue
		}
		// TODO: replace this -- just use the addr
		host, portStr, err := net.SplitHostPort(m.BrokerAddr)
		if err != nil {
			panic(err)
		}
		port, err := strconv.Atoi(portStr)
		if err != nil {
			panic(err)
		}
		brokers = append(brokers, &protocol.Broker{
			NodeID: b.config.ID,
			Host:   host,
			Port:   int32(port),
		})
	}
	var topicMetadata []*protocol.TopicMetadata
	topicMetadataFn := func(topic string, partitions []*jocko.Partition, err protocol.Error) *protocol.TopicMetadata {
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
			if !partition.IsLeader(b.config.ID) {
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

// isController returns true if this is the cluster controller.
func (b *Broker) isController() bool {
	return b.isLeader()
}

func (b *Broker) isLeader() bool {
	return b.raft.State() == raft.Leader
}

// topicPartitions is used to get the partitions for the given topic.
func (b *Broker) topicPartitions(topic string) (found []*jocko.Partition, err protocol.Error) {
	b.RLock()
	defer b.RUnlock()
	if p, ok := b.topicMap[topic]; ok {
		return p, protocol.ErrNone
	} else {
		return nil, protocol.ErrUnknownTopicOrPartition
	}
}

func (b *Broker) topics() map[string][]*jocko.Partition {
	b.RLock()
	defer b.RUnlock()
	return b.topicMap
}

func (b *Broker) partition(topic string, partition int32) (*jocko.Partition, protocol.Error) {
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
func (b *Broker) createPartition(partition *jocko.Partition) error {
	return nil
	// return b.raftApply(createPartition, partition)
}

// startReplica is used to start a replica on this, including creating its commit log.
func (b *Broker) startReplica(partition *jocko.Partition) protocol.Error {
	b.Lock()
	defer b.Unlock()
	if v, ok := b.topicMap[partition.Topic]; ok {
		b.topicMap[partition.Topic] = append(v, partition)
	} else {
		b.topicMap[partition.Topic] = []*jocko.Partition{partition}
	}
	isLeader := partition.Leader == b.config.ID
	isFollower := false
	for _, r := range partition.Replicas {
		if r == b.config.ID {
			isFollower = true
		}
	}
	if isLeader || isFollower {
		commitLog, err := commitlog.New(commitlog.Options{
			Path:            filepath.Join(b.config.DataDir, "data", partition.String()),
			MaxSegmentBytes: 1024,
			MaxLogBytes:     -1,
		})
		if err != nil {
			return protocol.ErrUnknown.WithErr(err)
		}
		partition.CommitLog = commitLog
		// partition.Conn = b.serf.Member(partition.LeaderID())
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

func (b *Broker) partitionsToCreate(topic string, partitionsCount int32, replicationFactor int16) []*jocko.Partition {
	mems := b.serverLookup.Servers()
	memCount := int32(len(mems))
	var partitions []*jocko.Partition

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
		partition := &jocko.Partition{
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
	return protocol.ErrNone

	// if err := b.raftApply(deleteTopic, &jocko.Partition{Topic: topic}); err != nil {
	// 	return protocol.ErrUnknown.WithErr(err)
	// }
	// return protocol.ErrNone
}

// deletePartitions is used to delete the topic from this.
func (b *Broker) deletePartitions(tp *jocko.Partition) error {
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

// Leave is used to prepare for a graceful shutdown.
func (s *Broker) Leave() error {
	s.logger.Info("broker: starting leave")

	numPeers, err := s.numPeers()
	if err != nil {
		s.logger.Error("jocko: failed to check raft peers", log.Error("error", err))
		return err
	}

	isLeader := s.isLeader()
	if isLeader && numPeers > 1 {
		future := s.raft.RemoveServer(raft.ServerID(s.config.RaftAddr), 0, 0)
		if err := future.Error(); err != nil {
			s.logger.Error("failed to remove ourself as raft peer", log.Error("error", err))
		}
	}

	if s.serf != nil {
		if err := s.serf.Leave(); err != nil {
			s.logger.Error("failed to leave LAN serf cluster", log.Error("error", err))
		}
	}

	if !isLeader {
		left := false
		limit := time.Now().Add(5 * time.Second)
		for !left && time.Now().Before(limit) {
			// Sleep a while before we check.
			time.Sleep(50 * time.Millisecond)

			// Get the latest configuration.
			future := s.raft.GetConfiguration()
			if err := future.Error(); err != nil {
				s.logger.Error("failed to get raft configuration", log.Error("error", err))
				break
			}

			// See if we are no longer included.
			left = true
			for _, server := range future.Configuration().Servers {
				if server.Address == raft.ServerAddress(s.config.RaftAddr) {
					left = false
					break
				}
			}
		}
	}

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
		b.serf.Shutdown()
	}

	if b.raft != nil {
		b.raftTransport.Close()
		future := b.raft.Shutdown()
		if err := future.Error(); err != nil {
			b.logger.Error("failed to shutdown", log.Error("error", err))
		}
		if b.raftStore != nil {
			b.raftStore.Close()
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
	p.Conn = b.serverLookup.ServerByID(raft.ServerID(p.LeaderID()))
	r := NewReplicator(ReplicatorConfig{}, p, b.config.ID, server.NewClient(p.Conn), b.logger)
	b.replicators[p] = r
	if !b.config.DevMode {
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
	p.Leader = b.config.ID
	p.Conn = b.serverLookup.ServerByID(raft.ServerID(p.LeaderID()))
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

// ensurePath is used to make sure a path exists
func ensurePath(path string, dir bool) error {
	if !dir {
		path = filepath.Dir(path)
	}
	return os.MkdirAll(path, 0755)
}

// Atomically sets a readiness state flag when leadership is obtained, to indicate that server is past its barrier write
func (s *Broker) setConsistentReadReady() {
	atomic.StoreInt32(&s.readyForConsistentReads, 1)
}

// Atomically reset readiness state flag on leadership revoke
func (s *Broker) resetConsistentReadReady() {
	atomic.StoreInt32(&s.readyForConsistentReads, 0)
}

// Returns true if this server is ready to serve consistent reads
func (s *Broker) isReadyForConsistentReads() bool {
	return atomic.LoadInt32(&s.readyForConsistentReads) == 1
}

func (s *Broker) numPeers() (int, error) {
	future := s.raft.GetConfiguration()
	if err := future.Error(); err != nil {
		return 0, err
	}
	raftConfig := future.Configuration()
	var numPeers int
	for _, server := range raftConfig.Servers {
		if server.Suffrage == raft.Voter {
			numPeers++
		}
	}
	return numPeers, nil
}

func (s *Broker) LANMembers() []serf.Member {
	return s.serf.Members()
}
