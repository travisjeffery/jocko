package jocko

import (
	"bytes"
	"container/ring"
	"context"
	"fmt"
	"io"
	"math/rand"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/davecgh/go-spew/spew"
	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb"
	"github.com/hashicorp/serf/serf"
	opentracing "github.com/opentracing/opentracing-go"
	"github.com/pkg/errors"
	uuid "github.com/satori/go.uuid"
	"github.com/travisjeffery/jocko/commitlog"
	"github.com/travisjeffery/jocko/jocko/config"
	"github.com/travisjeffery/jocko/jocko/fsm"
	"github.com/travisjeffery/jocko/jocko/metadata"
	"github.com/travisjeffery/jocko/jocko/structs"
	"github.com/travisjeffery/jocko/jocko/util"
	"github.com/travisjeffery/jocko/log"
	"github.com/travisjeffery/jocko/protocol"
)

var (
	brokerVerboseLogs bool

	ErrTopicExists            = errors.New("topic exists already")
	ErrInvalidArgument        = errors.New("no logger set")
	OffsetsTopicName          = "__consumer_offsets"
	OffsetsTopicNumPartitions = 50
)

const (
	serfLANSnapshot   = "serf/local.snapshot"
	raftState         = "raft/"
	raftLogCacheSize  = 512
	snapshotsRetained = 2
)

func init() {
	spew.Config.Indent = ""

	e := os.Getenv("JOCKODEBUG")
	if strings.Contains(e, "broker=1") {
		brokerVerboseLogs = true
	}
}

// Broker represents a broker in a Jocko cluster, like a broker in a Kafka cluster.
type Broker struct {
	sync.RWMutex
	logger log.Logger
	config *config.BrokerConfig

	// readyForConsistentReads is used to track when the leader server is
	// ready to serve consistent reads, after it has applied its initial
	// barrier. This is updated atomically.
	readyForConsistentReads int32
	// brokerLookup tracks servers in the local datacenter.
	brokerLookup  *brokerLookup
	replicaLookup *replicaLookup
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

	tracer opentracing.Tracer

	shutdownCh   chan struct{}
	shutdown     bool
	shutdownLock sync.Mutex
}

// New is used to instantiate a new broker.
func NewBroker(config *config.BrokerConfig, tracer opentracing.Tracer, logger log.Logger) (*Broker, error) {
	b := &Broker{
		config:        config,
		logger:        logger.With(log.Int32("id", config.ID), log.String("raft addr", config.RaftAddr)),
		shutdownCh:    make(chan struct{}),
		eventChLAN:    make(chan serf.Event, 256),
		brokerLookup:  NewBrokerLookup(),
		replicaLookup: NewReplicaLookup(),
		reconcileCh:   make(chan serf.Member, 32),
		tracer:        tracer,
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

// Broker API.

// Run starts a loop to handle requests send back responses.
func (b *Broker) Run(ctx context.Context, requestc <-chan Request, responsec chan<- Response) {
	var conn io.ReadWriter
	var header *protocol.RequestHeader
	var resp protocol.ResponseBody
	var reqCtx context.Context

	for {
		select {
		case request := <-requestc:
			conn = request.Conn
			header = request.Header
			reqCtx = request.Ctx

			queueSpan, ok := reqCtx.Value(requestQueueSpanKey).(opentracing.Span)
			if ok {
				queueSpan.Finish()
			}

			switch req := request.Request.(type) {
			case *protocol.ProduceRequest:
				resp = b.handleProduce(reqCtx, header, req)
			case *protocol.FetchRequest:
				resp = b.handleFetch(reqCtx, header, req)
			case *protocol.OffsetsRequest:
				resp = b.handleOffsets(reqCtx, header, req)
			case *protocol.MetadataRequest:
				resp = b.handleMetadata(reqCtx, header, req)
			case *protocol.LeaderAndISRRequest:
				resp = b.handleLeaderAndISR(reqCtx, header, req)
			case *protocol.StopReplicaRequest:
				resp = b.handleStopReplica(reqCtx, header, req)
			case *protocol.UpdateMetadataRequest:
				resp = b.handleUpdateMetadata(reqCtx, header, req)
			case *protocol.ControlledShutdownRequest:
				resp = b.handleControlledShutdown(reqCtx, header, req)
			case *protocol.OffsetCommitRequest:
				resp = b.handleOffsetCommit(reqCtx, header, req)
			case *protocol.OffsetFetchRequest:
				resp = b.handleOffsetFetch(reqCtx, header, req)
			case *protocol.FindCoordinatorRequest:
				resp = b.handleFindCoordinator(reqCtx, header, req)
			case *protocol.JoinGroupRequest:
				resp = b.handleJoinGroup(reqCtx, header, req)
			case *protocol.HeartbeatRequest:
				resp = b.handleHeartbeat(reqCtx, header, req)
			case *protocol.LeaveGroupRequest:
				resp = b.handleLeaveGroup(reqCtx, header, req)
			case *protocol.SyncGroupRequest:
				resp = b.handleSyncGroup(reqCtx, header, req)
			case *protocol.DescribeGroupsRequest:
				resp = b.handleDescribeGroups(reqCtx, header, req)
			case *protocol.ListGroupsRequest:
				resp = b.handleListGroups(reqCtx, header, req)
			case *protocol.SaslHandshakeRequest:
				resp = b.handleSaslHandshake(reqCtx, header, req)
			case *protocol.APIVersionsRequest:
				resp = b.handleAPIVersions(reqCtx, header, req)
			case *protocol.CreateTopicRequests:
				resp = b.handleCreateTopic(reqCtx, header, req)
			case *protocol.DeleteTopicsRequest:
				resp = b.handleDeleteTopics(reqCtx, header, req)
			}

		case <-ctx.Done():
			return
		}

		parentSpan := opentracing.SpanFromContext(reqCtx)
		queueSpan := b.tracer.StartSpan("broker: queue response", opentracing.ChildOf(parentSpan.Context()))
		responseCtx := context.WithValue(reqCtx, responseQueueSpanKey, queueSpan)

		responsec <- Response{Ctx: responseCtx, Conn: conn, Header: header, Response: &protocol.Response{
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

func span(ctx context.Context, tracer opentracing.Tracer, op string) opentracing.Span {
	if ctx == nil {
		// only done for unit tests
		return tracer.StartSpan("broker: " + op)
	}
	parentSpan := opentracing.SpanFromContext(ctx)
	if parentSpan == nil {
		// only done for unit tests
		return tracer.StartSpan("broker: " + op)
	}
	return tracer.StartSpan("broker: "+op, opentracing.ChildOf(parentSpan.Context()))
}

var apiVersions = &protocol.APIVersionsResponse{APIVersions: protocol.APIVersions}

func (b *Broker) handleAPIVersions(ctx context.Context, header *protocol.RequestHeader, req *protocol.APIVersionsRequest) *protocol.APIVersionsResponse {
	sp := span(ctx, b.tracer, "api versions")
	defer sp.Finish()
	return apiVersions
}

func (b *Broker) handleCreateTopic(ctx context.Context, header *protocol.RequestHeader, reqs *protocol.CreateTopicRequests) *protocol.CreateTopicsResponse {
	sp := span(ctx, b.tracer, "create topic")
	defer sp.Finish()
	resp := new(protocol.CreateTopicsResponse)
	resp.APIVersion = reqs.Version()
	resp.TopicErrorCodes = make([]*protocol.TopicErrorCode, len(reqs.Requests))
	isController := b.isController()
	sp.LogKV("is controller", isController)
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
		err := b.createTopic(ctx, req.Topic, req.NumPartitions, req.ReplicationFactor)
		resp.TopicErrorCodes[i] = &protocol.TopicErrorCode{
			Topic:     req.Topic,
			ErrorCode: err.Code(),
		}
	}
	return resp
}

func (b *Broker) handleDeleteTopics(ctx context.Context, header *protocol.RequestHeader, reqs *protocol.DeleteTopicsRequest) *protocol.DeleteTopicsResponse {
	sp := span(ctx, b.tracer, "delete topics")
	defer sp.Finish()
	resp := new(protocol.DeleteTopicsResponse)
	resp.APIVersion = reqs.Version()
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
		// TODO: this will delete from fsm -- need to delete associated partitions, etc.
		_, err := b.raftApply(structs.DeregisterTopicRequestType, structs.DeregisterTopicRequest{
			structs.Topic{
				Topic: topic,
			},
		})
		if err != nil {
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

func (b *Broker) handleLeaderAndISR(ctx context.Context, header *protocol.RequestHeader, req *protocol.LeaderAndISRRequest) *protocol.LeaderAndISRResponse {
	sp := span(ctx, b.tracer, "leader and isr")
	defer sp.Finish()
	resp := &protocol.LeaderAndISRResponse{
		Partitions: make([]*protocol.LeaderAndISRPartition, len(req.PartitionStates)),
	}
	resp.APIVersion = req.Version()
	setErr := func(i int, p *protocol.PartitionState, err protocol.Error) {
		resp.Partitions[i] = &protocol.LeaderAndISRPartition{
			ErrorCode: err.Code(),
			Partition: p.Partition,
			Topic:     p.Topic,
		}
	}
	for i, p := range req.PartitionStates {
		_, err := b.replicaLookup.Replica(p.Topic, p.Partition)
		isNew := err != nil

		// TODO: need to replace the replica regardless
		replica := &Replica{
			BrokerID: b.config.ID,
			Partition: structs.Partition{
				ID:              p.Partition,
				Partition:       p.Partition,
				Topic:           p.Topic,
				ISR:             p.ISR,
				AR:              p.Replicas,
				ControllerEpoch: p.ZKVersion,
				LeaderEpoch:     p.LeaderEpoch,
				Leader:          p.Leader,
			},
			IsLocal: true,
		}
		b.replicaLookup.AddReplica(replica)

		if err := b.startReplica(replica); err != protocol.ErrNone {
			setErr(i, p, err)
			continue
		}
		if p.Leader == b.config.ID && (replica.Partition.Leader != b.config.ID || isNew) {
			// is command asking this broker to be the new leader for p and this broker is not already the leader for
			if err := b.becomeLeader(replica, p); err != protocol.ErrNone {
				setErr(i, p, err)
				continue
			}
		} else if contains(p.Replicas, b.config.ID) && (!contains(replica.Partition.AR, p.Leader) || isNew) {
			// is command asking this broker to follow leader who it isn't a leader of already
			if err := b.becomeFollower(replica, p); err != protocol.ErrNone {
				setErr(i, p, err)
				continue
			}
		}
		resp.Partitions[i] = &protocol.LeaderAndISRPartition{Partition: p.Partition, Topic: p.Topic, ErrorCode: protocol.ErrNone.Code()}
	}
	return resp
}

func (b *Broker) handleOffsets(ctx context.Context, header *protocol.RequestHeader, req *protocol.OffsetsRequest) *protocol.OffsetsResponse {
	sp := span(ctx, b.tracer, "offsets")
	defer sp.Finish()
	oResp := new(protocol.OffsetsResponse)
	oResp.APIVersion = req.Version()
	oResp.Responses = make([]*protocol.OffsetResponse, len(req.Topics))
	for i, t := range req.Topics {
		oResp.Responses[i] = new(protocol.OffsetResponse)
		oResp.Responses[i].Topic = t.Topic
		oResp.Responses[i].PartitionResponses = make([]*protocol.PartitionResponse, 0, len(t.Partitions))
		for _, p := range t.Partitions {
			pResp := new(protocol.PartitionResponse)
			pResp.Partition = p.Partition
			replica, err := b.replicaLookup.Replica(t.Topic, p.Partition)
			if err != nil {
				// TODO: have replica lookup return an error with a code
				pResp.ErrorCode = protocol.ErrUnknown.Code()
				continue
			}
			var offset int64
			if p.Timestamp == -2 {
				offset = replica.Log.OldestOffset()
			} else {
				// TODO: this is nil because i'm not sending the leader and isr requests telling the new leader to start the replica and instantiate the log...
				offset = replica.Log.NewestOffset()
			}
			pResp.Offsets = []int64{offset}
			oResp.Responses[i].PartitionResponses = append(oResp.Responses[i].PartitionResponses, pResp)
		}
	}
	return oResp
}

func (b *Broker) handleProduce(ctx context.Context, header *protocol.RequestHeader, req *protocol.ProduceRequest) *protocol.ProduceResponses {
	sp := span(ctx, b.tracer, "produce")
	defer sp.Finish()
	resp := new(protocol.ProduceResponses)
	resp.APIVersion = req.Version()
	resp.Responses = make([]*protocol.ProduceResponse, len(req.TopicData))
	for i, td := range req.TopicData {
		presps := make([]*protocol.ProducePartitionResponse, len(td.Data))
		for j, p := range td.Data {
			presp := &protocol.ProducePartitionResponse{}
			state := b.fsm.State()
			_, t, err := state.GetTopic(td.Topic)
			if err != nil {
				presp.ErrorCode = protocol.ErrUnknown.WithErr(err).Code()
				presps[j] = presp
				continue
			}
			if t == nil {
				presp.ErrorCode = protocol.ErrUnknownTopicOrPartition.Code()
				presps[j] = presp
				continue
			}
			replica, err := b.replicaLookup.Replica(td.Topic, p.Partition)
			if err != nil || replica == nil || replica.Log == nil {
				b.logger.Error("produce to partition failed", log.Error("error", err))
				presp.Partition = p.Partition
				presp.ErrorCode = protocol.ErrReplicaNotAvailable.Code()
				presps[j] = presp
				continue
			}
			offset, appendErr := replica.Log.Append(p.RecordSet)
			if appendErr != nil {
				b.logger.Error("commitlog/append failed", log.Error("error", err))
				presp.ErrorCode = protocol.ErrUnknown.Code()
				presps[j] = presp
				continue
			}
			presp.Partition = p.Partition
			presp.BaseOffset = offset
			presp.LogAppendTime = time.Now()
			presps[j] = presp
		}
		resp.Responses[i] = &protocol.ProduceResponse{
			Topic:              td.Topic,
			PartitionResponses: presps,
		}
	}
	return resp
}

func (b *Broker) handleMetadata(ctx context.Context, header *protocol.RequestHeader, req *protocol.MetadataRequest) *protocol.MetadataResponse {
	sp := span(ctx, b.tracer, "metadata")
	defer sp.Finish()
	state := b.fsm.State()
	brokers := make([]*protocol.Broker, 0, len(b.LANMembers()))

	_, nodes, err := state.GetNodes()
	if err != nil {
		panic(err)
	}

	// TODO: add an index to the table on the check status
	var passing []*structs.Node
	for _, n := range nodes {
		if n.Check.Status == structs.HealthPassing {
			passing = append(passing, n)
		}
	}

	for _, mem := range b.LANMembers() {
		// TODO: should filter elsewhere
		if mem.Status != serf.StatusAlive {
			continue
		}

		m, ok := metadata.IsBroker(mem)
		if !ok {
			continue
		}
		brokers = append(brokers, &protocol.Broker{
			NodeID: m.ID.Int32(),
			Host:   m.Host(),
			Port:   m.Port(),
		})
	}
	var topicMetadata []*protocol.TopicMetadata
	topicMetadataFn := func(topic *structs.Topic, err protocol.Error) *protocol.TopicMetadata {
		if err != protocol.ErrNone {
			return &protocol.TopicMetadata{
				TopicErrorCode: err.Code(),
				Topic:          topic.Topic,
			}
		}
		partitionMetadata := make([]*protocol.PartitionMetadata, 0, len(topic.Partitions))
		for id := range topic.Partitions {
			_, p, err := state.GetPartition(topic.Topic, id)
			if err != nil {
				partitionMetadata = append(partitionMetadata, &protocol.PartitionMetadata{
					ParititionID:       id,
					PartitionErrorCode: protocol.ErrUnknown.Code(),
				})
				continue
			}
			if p == nil {
				partitionMetadata = append(partitionMetadata, &protocol.PartitionMetadata{
					ParititionID:       id,
					PartitionErrorCode: protocol.ErrUnknownTopicOrPartition.Code(),
				})
				continue
			}
			partitionMetadata = append(partitionMetadata, &protocol.PartitionMetadata{
				ParititionID:       p.ID,
				PartitionErrorCode: protocol.ErrNone.Code(),
				Leader:             p.Leader,
				Replicas:           p.AR,
				ISR:                p.ISR,
			})
		}
		return &protocol.TopicMetadata{
			TopicErrorCode:    protocol.ErrNone.Code(),
			Topic:             topic.Topic,
			PartitionMetadata: partitionMetadata,
		}
	}
	if len(req.Topics) == 0 {
		// Respond with metadata for all topics
		// how to handle err here?
		_, topics, _ := state.GetTopics()
		topicMetadata = make([]*protocol.TopicMetadata, 0, len(topics))
		for _, topic := range topics {
			topicMetadata = append(topicMetadata, topicMetadataFn(topic, protocol.ErrNone))
		}
	} else {
		topicMetadata = make([]*protocol.TopicMetadata, 0, len(req.Topics))
		for _, topicName := range req.Topics {
			_, topic, err := state.GetTopic(topicName)
			if topic == nil {
				topicMetadata = append(topicMetadata, topicMetadataFn(&structs.Topic{Topic: topicName}, protocol.ErrUnknownTopicOrPartition))
			} else if err != nil {
				topicMetadata = append(topicMetadata, topicMetadataFn(&structs.Topic{Topic: topicName}, protocol.ErrUnknown.WithErr(err)))
			} else {
				topicMetadata = append(topicMetadata, topicMetadataFn(topic, protocol.ErrNone))
			}
		}
	}
	resp := &protocol.MetadataResponse{
		Brokers:       brokers,
		TopicMetadata: topicMetadata,
	}
	resp.APIVersion = req.Version()
	return resp
}

func (b *Broker) handleFindCoordinator(ctx context.Context, header *protocol.RequestHeader, req *protocol.FindCoordinatorRequest) *protocol.FindCoordinatorResponse {
	sp := span(ctx, b.tracer, "find coordinator")
	defer sp.Finish()

	resp := &protocol.FindCoordinatorResponse{}
	resp.APIVersion = req.Version()

	// TODO: distribute this.
	state := b.fsm.State()

	var broker *metadata.Broker
	var p *structs.Partition
	var i int32

	topic, err := b.offsetsTopic(ctx)
	if err != nil {
		goto ERROR
	}
	i = int32(util.Hash(req.CoordinatorKey) % uint64(len(topic.Partitions)))
	_, p, err = state.GetPartition(OffsetsTopicName, i)
	if err != nil {
		goto ERROR
	}
	broker = b.brokerLookup.BrokerByID(raft.ServerID(p.Leader))

	resp.Coordinator.NodeID = broker.ID.Int32()
	resp.Coordinator.Host = broker.Host()
	resp.Coordinator.Port = broker.Port()

	return resp

ERROR:
	// todo: which err code to use?
	resp.ErrorCode = protocol.ErrUnknown.Code()
	b.logger.Error("find coordinator failed", log.Error("error", err), log.Any("coordinator key", req.CoordinatorKey), log.Any("broker", broker))

	return resp
}

func (b *Broker) handleJoinGroup(ctx context.Context, header *protocol.RequestHeader, r *protocol.JoinGroupRequest) *protocol.JoinGroupResponse {
	sp := span(ctx, b.tracer, "join group")
	defer sp.Finish()

	resp := &protocol.JoinGroupResponse{}
	resp.APIVersion = r.Version()

	// // TODO: distribute this.
	state := b.fsm.State()

	_, group, err := state.GetGroup(r.GroupID)
	if err != nil {
		b.logger.Error("failed getting group", log.Error("error", err))
		resp.ErrorCode = protocol.ErrUnknown.Code()
		return resp
	}
	if group == nil {
		// group doesn't exist so let's create it
		group = &structs.Group{
			Group:       r.GroupID,
			Coordinator: b.config.ID,
		}
	}
	if r.MemberID == "" {
		// for group member IDs -- can replace with something else
		r.MemberID = uuid.NewV1().String()
		group.Members[r.MemberID] = structs.Member{ID: r.MemberID}
	}
	if group.LeaderID == "" {
		group.LeaderID = r.MemberID
	}
	_, err = b.raftApply(structs.RegisterGroupRequestType, structs.RegisterGroupRequest{
		Group: *group,
	})
	if err != nil {
		b.logger.Error("failed to register group", log.Error("error", err))
		resp.ErrorCode = protocol.ErrUnknown.Code()
		return resp
	}

	resp.GenerationID = 0
	resp.LeaderID = group.LeaderID
	resp.MemberID = r.MemberID
	for _, m := range group.Members {
		resp.Members = append(resp.Members, protocol.Member{MemberID: m.ID, MemberMetadata: m.Metadata})
	}

	return resp
}

func (b *Broker) handleLeaveGroup(ctx context.Context, header *protocol.RequestHeader, r *protocol.LeaveGroupRequest) *protocol.LeaveGroupResponse {
	sp := span(ctx, b.tracer, "leave group")
	defer sp.Finish()

	resp := &protocol.LeaveGroupResponse{}
	resp.APIVersion = r.Version()

	// // TODO: distribute this.
	state := b.fsm.State()

	_, group, err := state.GetGroup(r.GroupID)
	if err != nil {
		resp.ErrorCode = protocol.ErrUnknown.Code()
		return resp
	}
	if group == nil {
		resp.ErrorCode = protocol.ErrInvalidGroupId.Code()
		return resp
	}
	if _, ok := group.Members[r.MemberID]; !ok {
		resp.ErrorCode = protocol.ErrUnknownMemberId.Code()
		return resp
	}

	delete(group.Members, r.MemberID)

	_, err = b.raftApply(structs.RegisterGroupRequestType, structs.RegisterGroupRequest{
		Group: *group,
	})
	if err != nil {
		resp.ErrorCode = protocol.ErrUnknown.Code()
		return resp
	}

	return resp
}

func (b *Broker) handleSyncGroup(ctx context.Context, header *protocol.RequestHeader, r *protocol.SyncGroupRequest) *protocol.SyncGroupResponse {
	sp := span(ctx, b.tracer, "sync group")
	defer sp.Finish()

	state := b.fsm.State()
	resp := &protocol.SyncGroupResponse{}
	resp.APIVersion = r.Version()

	_, group, err := state.GetGroup(r.GroupID)
	if err != nil {
		resp.ErrorCode = protocol.ErrUnknown.Code()
		return resp
	}
	if group == nil {
		resp.ErrorCode = protocol.ErrInvalidGroupId.Code()
		return resp
	}
	if group.LeaderID == r.MemberID {
		// take the assignments from the leader and save them
		for _, ga := range r.GroupAssignments {
			if m, ok := group.Members[ga.MemberID]; ok {
				m.Assignment = ga.MemberAssignment
			} else {
				panic("sync group: unknown member")
			}
		}
		_, err = b.raftApply(structs.RegisterGroupRequestType, structs.RegisterGroupRequest{
			Group: *group,
		})
		if err != nil {
			resp.ErrorCode = protocol.ErrUnknown.Code()
			return resp
		}
	} else {
		if m, ok := group.Members[r.MemberID]; ok {
			resp.MemberAssignment = m.Assignment
		} else {
			panic("sync group: unknown member")
		}
	}

	return resp
}

func (b *Broker) handleHeartbeat(ctx context.Context, header *protocol.RequestHeader, r *protocol.HeartbeatRequest) *protocol.HeartbeatResponse {
	sp := span(ctx, b.tracer, "heartbeat")
	defer sp.Finish()

	resp := &protocol.HeartbeatResponse{}
	resp.APIVersion = r.Version()

	state := b.fsm.State()
	_, group, err := state.GetGroup(r.GroupID)
	if err != nil {
		resp.ErrorCode = protocol.ErrUnknown.Code()
		return resp
	}
	if group == nil {
		resp.ErrorCode = protocol.ErrInvalidGroupId.Code()
		return resp
	}
	// TODO: need to handle case when rebalance is in process

	resp.ErrorCode = protocol.ErrNone.Code()

	return resp
}

func (b *Broker) handleFetch(ctx context.Context, header *protocol.RequestHeader, r *protocol.FetchRequest) *protocol.FetchResponses {
	sp := span(ctx, b.tracer, "fetch")
	defer sp.Finish()
	fresp := &protocol.FetchResponses{
		Responses: make([]*protocol.FetchResponse, len(r.Topics)),
	}
	fresp.APIVersion = r.Version()
	received := time.Now()
	for i, topic := range r.Topics {
		fr := &protocol.FetchResponse{
			Topic:              topic.Topic,
			PartitionResponses: make([]*protocol.FetchPartitionResponse, len(topic.Partitions)),
		}
		for j, p := range topic.Partitions {
			replica, err := b.replicaLookup.Replica(topic.Topic, p.Partition)
			if err != nil {
				fr.PartitionResponses[j] = &protocol.FetchPartitionResponse{
					Partition: p.Partition,
					ErrorCode: protocol.ErrReplicaNotAvailable.Code(),
				}
				continue
			}
			if replica.Partition.Leader != b.config.ID {
				fr.PartitionResponses[j] = &protocol.FetchPartitionResponse{
					Partition: p.Partition,
					ErrorCode: protocol.ErrNotLeaderForPartition.Code(),
				}
				continue
			}
			if replica.Log == nil {
				fr.PartitionResponses[j] = &protocol.FetchPartitionResponse{
					Partition: p.Partition,
					ErrorCode: protocol.ErrReplicaNotAvailable.Code(),
				}
				continue
			}
			rdr, rdrErr := replica.Log.NewReader(p.FetchOffset, p.MaxBytes)
			if rdrErr != nil {
				fr.PartitionResponses[j] = &protocol.FetchPartitionResponse{
					Partition: p.Partition,
					ErrorCode: protocol.ErrUnknown.Code(),
				}
				continue
			}
			buf := new(bytes.Buffer)
			var n int32
			for n < r.MinBytes {
				if r.MaxWaitTime != 0 && int32(time.Since(received).Nanoseconds()/1e6) > r.MaxWaitTime {
					break
				}
				// TODO: copy these bytes to outer bytes
				nn, err := io.Copy(buf, rdr)
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
				HighWatermark: replica.Log.NewestOffset(),
				RecordSet:     buf.Bytes(),
			}
		}
		fresp.Responses[i] = fr
	}
	return fresp
}

func (b *Broker) handleSaslHandshake(ctx context.Context, header *protocol.RequestHeader, req *protocol.SaslHandshakeRequest) *protocol.SaslHandshakeResponse {
	panic("not implemented: sasl handshake")
	return nil
}

func (b *Broker) handleListGroups(ctx context.Context, header *protocol.RequestHeader, req *protocol.ListGroupsRequest) *protocol.ListGroupsResponse {
	sp := span(ctx, b.tracer, "create topic")
	defer sp.Finish()
	resp := new(protocol.ListGroupsResponse)
	resp.APIVersion = req.Version()
	state := b.fsm.State()

	fmt.Println("list")
	fmt.Println("list")
	fmt.Println("list")

	_, groups, err := state.GetGroups()
	if err != nil {
		resp.ErrorCode = protocol.ErrUnknown.Code()
		return resp
	}
	for _, group := range groups {
		resp.Groups = append(resp.Groups, protocol.ListGroup{
			GroupID: group.Group,
			// TODO: add protocol type
			ProtocolType: "consumer",
		})
	}
	return resp
}

func (b *Broker) handleDescribeGroups(ctx context.Context, header *protocol.RequestHeader, req *protocol.DescribeGroupsRequest) *protocol.DescribeGroupsResponse {
	sp := span(ctx, b.tracer, "create topic")
	defer sp.Finish()
	resp := new(protocol.DescribeGroupsResponse)
	resp.APIVersion = req.Version()
	state := b.fsm.State()

	fmt.Println("describe")
	fmt.Println("describe")
	fmt.Println("describe")

	for _, id := range req.GroupIDs {
		group := protocol.Group{}
		_, g, err := state.GetGroup(id)
		if err != nil {
			group.ErrorCode = protocol.ErrUnknown.Code()
			group.GroupID = id
			resp.Groups = append(resp.Groups, group)
			return resp
		}
		group.GroupID = id
		group.State = "Stable"
		group.ProtocolType = "consumer"
		group.Protocol = "consumer"
		for id, member := range g.Members {
			group.GroupMembers[id] = &protocol.GroupMember{
				ClientID: member.ID,
				// TODO: ???
				ClientHost:            "",
				GroupMemberMetadata:   member.Metadata,
				GroupMemberAssignment: member.Assignment,
			}
		}
		resp.Groups = append(resp.Groups)

	}

	return resp
}

func (b *Broker) handleStopReplica(ctx context.Context, header *protocol.RequestHeader, req *protocol.StopReplicaRequest) *protocol.StopReplicaResponse {
	panic("not implemented: stop replica")
	return nil
}

func (b *Broker) handleUpdateMetadata(ctx context.Context, header *protocol.RequestHeader, req *protocol.UpdateMetadataRequest) *protocol.UpdateMetadataResponse {
	panic("not implemented: update metadata")
	return nil
}

func (b *Broker) handleControlledShutdown(ctx context.Context, header *protocol.RequestHeader, req *protocol.ControlledShutdownRequest) *protocol.ControlledShutdownResponse {
	panic("not implemented: controlled shutdown")
	return nil
}

func (b *Broker) handleOffsetCommit(ctx context.Context, header *protocol.RequestHeader, req *protocol.OffsetCommitRequest) *protocol.OffsetCommitResponse {
	panic("not implemented: offset commit")
	return nil
}

func (b *Broker) handleOffsetFetch(ctx context.Context, header *protocol.RequestHeader, req *protocol.OffsetFetchRequest) *protocol.OffsetFetchResponse {
	sp := span(ctx, b.tracer, "create topic")
	defer sp.Finish()

	resp := new(protocol.OffsetFetchResponse)
	resp.APIVersion = req.Version()
	resp.Responses = make([]protocol.OffsetFetchTopicResponse, len(req.Topics))

	// state := b.fsm.State()

	// _, g, err := state.GetGroup(req.GroupID)

	// // If group doesn't exist then create it?
	// if err != nil {
	// 	// TODO: handle err
	// 	panic(err)
	// }

	return resp

}

// isController returns true if this is the cluster controller.
func (b *Broker) isController() bool {
	return b.isLeader()
}

func (b *Broker) isLeader() bool {
	return b.raft.State() == raft.Leader
}

// createPartition is used to add a partition across the cluster.
func (b *Broker) createPartition(partition structs.Partition) error {
	_, err := b.raftApply(structs.RegisterPartitionRequestType, structs.RegisterPartitionRequest{
		partition,
	})
	return err
}

// startReplica is used to start a replica on this, including creating its commit log.
func (b *Broker) startReplica(replica *Replica) protocol.Error {
	b.Lock()
	defer b.Unlock()

	if replica.Log == nil {
		log, err := commitlog.New(commitlog.Options{
			Path:            filepath.Join(b.config.DataDir, "data", fmt.Sprintf("%d", replica.Partition.ID)),
			MaxSegmentBytes: 1024,
			MaxLogBytes:     -1,
		})
		if err != nil {
			return protocol.ErrUnknown.WithErr(err)
		}
		replica.Log = log
		// TODO: register leader-change listener on r.replica.Partition.id
	}

	return protocol.ErrNone
}

// createTopic is used to create the topic across the cluster.
func (b *Broker) createTopic(ctx context.Context, topic string, partitions int32, replicationFactor int16) protocol.Error {
	state := b.fsm.State()
	_, t, _ := state.GetTopic(topic)
	if t != nil {
		return protocol.ErrTopicAlreadyExists
	}
	ps := b.buildPartitions(topic, partitions, replicationFactor)
	tt := structs.Topic{
		Topic:      topic,
		Partitions: make(map[int32][]int32),
	}
	for _, partition := range ps {
		tt.Partitions[partition.ID] = partition.AR
	}
	if _, err := b.raftApply(structs.RegisterTopicRequestType, structs.RegisterTopicRequest{Topic: tt}); err != nil {
		return protocol.ErrUnknown.WithErr(err)
	}
	for _, partition := range ps {
		if err := b.createPartition(partition); err != nil {
			return protocol.ErrUnknown.WithErr(err)
		}
	}
	// could move this up maybe and do the iteration once
	req := &protocol.LeaderAndISRRequest{
		ControllerID: b.config.ID,
		// TODO ControllerEpoch
		PartitionStates: make([]*protocol.PartitionState, 0, len(ps)),
	}
	for _, partition := range ps {
		req.PartitionStates = append(req.PartitionStates, &protocol.PartitionState{
			Topic:     partition.Topic,
			Partition: partition.ID,
			// TODO: ControllerEpoch, LeaderEpoch, ZKVersion
			Leader:   partition.Leader,
			ISR:      partition.ISR,
			Replicas: partition.AR,
		})
	}
	// TODO: can optimize this
	for _, broker := range b.brokerLookup.Brokers() {
		if broker.ID.Int32() == b.config.ID {
			errCode := b.handleLeaderAndISR(ctx, nil, req).ErrorCode
			if protocol.ErrNone.Code() != errCode {
				panic(fmt.Sprintf("failed handling leader and isr: %d", errCode))
			}
		} else {
			conn, err := Dial("tcp", broker.BrokerAddr)
			if err != nil {
				return protocol.ErrUnknown.WithErr(err)
			}
			_, err = conn.LeaderAndISR(req)
			if err != nil {
				// handle err and responses
				return protocol.ErrUnknown.WithErr(err)
			}
		}
	}
	return protocol.ErrNone
}

func (b *Broker) buildPartitions(topic string, partitionsCount int32, replicationFactor int16) []structs.Partition {
	brokers := b.brokerLookup.Brokers()
	count := len(brokers)

	// container/ring is dope af
	r := ring.New(count)
	for i := 0; i < r.Len(); i++ {
		r.Value = brokers[i]
		r = r.Next()
	}

	var partitions []structs.Partition

	for i := int32(0); i < partitionsCount; i++ {
		// TODO: maybe just go next here too
		r = r.Move(rand.Intn(count))
		leader := r.Value.(*metadata.Broker)
		replicas := []int32{leader.ID.Int32()}
		for i := int16(0); i < replicationFactor-1; i++ {
			r = r.Next()
			replicas = append(replicas, r.Value.(*metadata.Broker).ID.Int32())
		}
		partition := structs.Partition{
			Topic:     topic,
			ID:        i,
			Partition: i,
			Leader:    leader.ID.Int32(),
			AR:        replicas,
			ISR:       replicas,
		}
		partitions = append(partitions, partition)
	}

	return partitions
}

// Leave is used to prepare for a graceful shutdown.
func (b *Broker) Leave() error {
	b.logger.Info("broker: starting leave")

	numPeers, err := b.numPeers()
	if err != nil {
		b.logger.Error("jocko: failed to check raft peers", log.Error("error", err))
		return err
	}

	isLeader := b.isLeader()
	if isLeader && numPeers > 1 {
		future := b.raft.RemoveServer(raft.ServerID(b.config.ID), 0, 0)
		if err := future.Error(); err != nil {
			b.logger.Error("failed to remove ourself as raft peer", log.Error("error", err))
		}
	}

	if b.serf != nil {
		if err := b.serf.Leave(); err != nil {
			b.logger.Error("failed to leave LAN serf cluster", log.Error("error", err))
		}
	}

	time.Sleep(b.config.LeaveDrainTime)

	if !isLeader {
		left := false
		limit := time.Now().Add(5 * time.Second)
		for !left && time.Now().Before(limit) {
			// Sleep a while before we check.
			time.Sleep(50 * time.Millisecond)

			// Get the latest configuration.
			future := b.raft.GetConfiguration()
			if err := future.Error(); err != nil {
				b.logger.Error("failed to get raft configuration", log.Error("error", err))
				break
			}

			// See if we are no longer included.
			left = true
			for _, server := range future.Configuration().Servers {
				if server.Address == raft.ServerAddress(b.config.RaftAddr) {
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

func (b *Broker) becomeFollower(replica *Replica, cmd *protocol.PartitionState) protocol.Error {
	// stop replicator to current leader
	b.Lock()
	defer b.Unlock()
	if replica.Replicator != nil {
		if err := replica.Replicator.Close(); err != nil {
			return protocol.ErrUnknown.WithErr(err)
		}
	}
	hw := replica.Log.NewestOffset()
	if err := replica.Log.Truncate(hw); err != nil {
		return protocol.ErrUnknown.WithErr(err)
	}
	broker := b.brokerLookup.BrokerByID(raft.ServerID(cmd.Leader))
	if broker == nil {
		return protocol.ErrBrokerNotAvailable
	}
	conn, err := NewDialer(fmt.Sprintf("jocko-replicator-%d", b.config.ID)).Dial("tcp", broker.BrokerAddr)
	if err != nil {
		return protocol.ErrUnknown.WithErr(err)
	}
	r := NewReplicator(ReplicatorConfig{}, replica, conn, b.logger)
	replica.Replicator = r
	if !b.config.DevMode {
		r.Replicate()
	}
	return protocol.ErrNone
}

func (b *Broker) becomeLeader(replica *Replica, cmd *protocol.PartitionState) protocol.Error {
	b.Lock()
	defer b.Unlock()
	if replica.Replicator != nil {
		if err := replica.Replicator.Close(); err != nil {
			return protocol.ErrUnknown.WithErr(err)
		}
		replica.Replicator = nil
	}
	replica.Partition.Leader = cmd.Leader
	replica.Partition.AR = cmd.Replicas
	replica.Partition.ISR = cmd.ISR
	replica.Partition.LeaderEpoch = cmd.ZKVersion
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
func (b *Broker) setConsistentReadReady() {
	atomic.StoreInt32(&b.readyForConsistentReads, 1)
}

// Atomically reset readiness state flag on leadership revoke
func (b *Broker) resetConsistentReadReady() {
	atomic.StoreInt32(&b.readyForConsistentReads, 0)
}

// Returns true if this server is ready to serve consistent reads
func (b *Broker) isReadyForConsistentReads() bool {
	return atomic.LoadInt32(&b.readyForConsistentReads) == 1
}

func (b *Broker) numPeers() (int, error) {
	future := b.raft.GetConfiguration()
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

func (b *Broker) LANMembers() []serf.Member {
	return b.serf.Members()
}

// Replica
type Replica struct {
	BrokerID   int32
	Partition  structs.Partition
	IsLocal    bool
	Log        CommitLog
	Hw         int64
	Leo        int64
	Replicator *Replicator
	sync.Mutex
}

func (b *Broker) offsetsTopic(ctx context.Context) (topic *structs.Topic, err error) {
	state := b.fsm.State()
	name := "__consumer_offsets"

	// check if the topic exists already
	_, topic, err = state.GetTopic(name)
	if err != nil {
		return
	}
	if topic != nil {
		return
	}

	// doesn't exist so let's create it
	partitions := b.buildPartitions(name, 50, 3)
	topic = &structs.Topic{
		Topic:      "__consumer_offsets",
		Partitions: make(map[int32][]int32),
	}
	for _, p := range partitions {
		topic.Partitions[p.Partition] = p.AR
	}
	_, err = b.raftApply(structs.RegisterTopicRequestType, structs.RegisterTopicRequest{
		Topic: *topic,
	})
	return
}
