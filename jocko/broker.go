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
	config *config.Config

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
	reconcileCh      chan serf.Member
	serf             *serf.Serf
	fsm              *fsm.FSM
	eventChLAN       chan serf.Event
	logStateInterval time.Duration

	tracer opentracing.Tracer

	shutdownCh   chan struct{}
	shutdown     bool
	shutdownLock sync.Mutex
}

// New is used to instantiate a new broker.
func NewBroker(config *config.Config, tracer opentracing.Tracer) (*Broker, error) {
	b := &Broker{
		config:           config,
		shutdownCh:       make(chan struct{}),
		eventChLAN:       make(chan serf.Event, 256),
		brokerLookup:     NewBrokerLookup(),
		replicaLookup:    NewReplicaLookup(),
		reconcileCh:      make(chan serf.Member, 32),
		tracer:           tracer,
		logStateInterval: time.Millisecond * 250,
	}

	if err := b.setupRaft(); err != nil {
		b.Shutdown()
		return nil, fmt.Errorf("start raft: %v", err)
	}

	var err error
	b.serf, err = b.setupSerf(config.SerfLANConfig, b.eventChLAN, serfLANSnapshot)
	if err != nil {
		return nil, err
	}

	go b.lanEventHandler()

	go b.monitorLeadership()

	go b.logState()

	return b, nil
}

// Broker API.

// Run starts a loop to handle requests send back responses.
func (b *Broker) Run(ctx context.Context, requests <-chan *Context, responses chan<- *Context) {
	for {
		select {
		case reqCtx := <-requests:
			log.Debug.Printf("broker/%d: request: %v", b.config.ID, reqCtx)

			if reqCtx == nil {
				goto DONE
			}

			queueSpan, ok := reqCtx.Value(requestQueueSpanKey).(opentracing.Span)
			if ok {
				queueSpan.Finish()
			}

			var res protocol.ResponseBody

			switch req := reqCtx.req.(type) {
			case *protocol.ProduceRequest:
				res = b.handleProduce(reqCtx, req)
			case *protocol.FetchRequest:
				res = b.handleFetch(reqCtx, req)
			case *protocol.OffsetsRequest:
				res = b.handleOffsets(reqCtx, req)
			case *protocol.MetadataRequest:
				res = b.handleMetadata(reqCtx, req)
			case *protocol.LeaderAndISRRequest:
				res = b.handleLeaderAndISR(reqCtx, req)
			case *protocol.StopReplicaRequest:
				res = b.handleStopReplica(reqCtx, req)
			case *protocol.UpdateMetadataRequest:
				res = b.handleUpdateMetadata(reqCtx, req)
			case *protocol.ControlledShutdownRequest:
				res = b.handleControlledShutdown(reqCtx, req)
			case *protocol.OffsetCommitRequest:
				res = b.handleOffsetCommit(reqCtx, req)
			case *protocol.OffsetFetchRequest:
				res = b.handleOffsetFetch(reqCtx, req)
			case *protocol.FindCoordinatorRequest:
				res = b.handleFindCoordinator(reqCtx, req)
			case *protocol.JoinGroupRequest:
				res = b.handleJoinGroup(reqCtx, req)
			case *protocol.HeartbeatRequest:
				res = b.handleHeartbeat(reqCtx, req)
			case *protocol.LeaveGroupRequest:
				res = b.handleLeaveGroup(reqCtx, req)
			case *protocol.SyncGroupRequest:
				res = b.handleSyncGroup(reqCtx, req)
			case *protocol.DescribeGroupsRequest:
				res = b.handleDescribeGroups(reqCtx, req)
			case *protocol.ListGroupsRequest:
				res = b.handleListGroups(reqCtx, req)
			case *protocol.SaslHandshakeRequest:
				res = b.handleSaslHandshake(reqCtx, req)
			case *protocol.APIVersionsRequest:
				res = b.handleAPIVersions(reqCtx, req)
			case *protocol.CreateTopicRequests:
				res = b.handleCreateTopic(reqCtx, req)
			case *protocol.DeleteTopicsRequest:
				res = b.handleDeleteTopics(reqCtx, req)
			}

			parentSpan := opentracing.SpanFromContext(reqCtx)
			queueSpan = b.tracer.StartSpan("broker: queue response", opentracing.ChildOf(parentSpan.Context()))
			responseCtx := context.WithValue(reqCtx, responseQueueSpanKey, queueSpan)

			responses <- &Context{
				parent: responseCtx,
				conn:   reqCtx.conn,
				header: reqCtx.header,
				res: &protocol.Response{
					CorrelationID: reqCtx.header.CorrelationID,
					Body:          res,
				},
			}
		case <-ctx.Done():
			goto DONE
		}
	}
DONE:
	log.Debug.Printf("broker/%d: run done", b.config.ID)
	return
}

// Join is used to have the broker join the gossip ring.
// The given address should be another broker listening on the Serf address.
func (b *Broker) JoinLAN(addrs ...string) protocol.Error {
	if _, err := b.serf.Join(addrs, true); err != nil {
		return protocol.ErrUnknown.WithErr(err)
	}
	return protocol.ErrNone
}

// req handling.

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

func (b *Broker) handleAPIVersions(ctx *Context, req *protocol.APIVersionsRequest) *protocol.APIVersionsResponse {
	sp := span(ctx, b.tracer, "api versions")
	defer sp.Finish()
	return apiVersions
}

func (b *Broker) handleCreateTopic(ctx *Context, reqs *protocol.CreateTopicRequests) *protocol.CreateTopicsResponse {
	sp := span(ctx, b.tracer, "create topic")
	defer sp.Finish()
	res := new(protocol.CreateTopicsResponse)
	res.APIVersion = reqs.Version()
	res.TopicErrorCodes = make([]*protocol.TopicErrorCode, len(reqs.Requests))
	isController := b.isController()
	sp.LogKV("is controller", isController)
	for i, req := range reqs.Requests {
		if !isController {
			res.TopicErrorCodes[i] = &protocol.TopicErrorCode{
				Topic:     req.Topic,
				ErrorCode: protocol.ErrNotController.Code(),
			}
			continue
		}
		if req.ReplicationFactor > int16(len(b.LANMembers())) {
			res.TopicErrorCodes[i] = &protocol.TopicErrorCode{
				Topic:     req.Topic,
				ErrorCode: protocol.ErrInvalidReplicationFactor.Code(),
			}
			continue
		}
		err := b.withTimeout(reqs.Timeout, func() protocol.Error {
			return b.createTopic(ctx, req)
		})
		res.TopicErrorCodes[i] = &protocol.TopicErrorCode{
			Topic:     req.Topic,
			ErrorCode: err.Code(),
		}

	}
	return res
}

func (b *Broker) handleDeleteTopics(ctx *Context, reqs *protocol.DeleteTopicsRequest) *protocol.DeleteTopicsResponse {
	sp := span(ctx, b.tracer, "delete topics")
	defer sp.Finish()
	res := new(protocol.DeleteTopicsResponse)
	res.APIVersion = reqs.Version()
	res.TopicErrorCodes = make([]*protocol.TopicErrorCode, len(reqs.Topics))
	isController := b.isController()
	for i, topic := range reqs.Topics {
		if !isController {
			res.TopicErrorCodes[i] = &protocol.TopicErrorCode{
				Topic:     topic,
				ErrorCode: protocol.ErrNotController.Code(),
			}
			continue
		}
		err := b.withTimeout(reqs.Timeout, func() protocol.Error {
			// TODO: this will delete from fsm -- need to delete associated partitions, etc.
			_, err := b.raftApply(structs.DeregisterTopicRequestType, structs.DeregisterTopicRequest{
				structs.Topic{
					Topic: topic,
				},
			})
			if err != nil {
				return protocol.ErrUnknown.WithErr(err)
			}
			return protocol.ErrNone
		})
		res.TopicErrorCodes[i] = &protocol.TopicErrorCode{
			Topic:     topic,
			ErrorCode: err.Code(),
		}
	}
	return res
}

func (b *Broker) handleLeaderAndISR(ctx *Context, req *protocol.LeaderAndISRRequest) *protocol.LeaderAndISRResponse {
	sp := span(ctx, b.tracer, "leader and isr")
	defer sp.Finish()
	res := &protocol.LeaderAndISRResponse{
		Partitions: make([]*protocol.LeaderAndISRPartition, len(req.PartitionStates)),
	}
	res.APIVersion = req.Version()
	setErr := func(i int, p *protocol.PartitionState, err protocol.Error) {
		res.Partitions[i] = &protocol.LeaderAndISRPartition{
			ErrorCode: err.Code(),
			Partition: p.Partition,
			Topic:     p.Topic,
		}
	}
	for i, p := range req.PartitionStates {
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

		if p.Leader == b.config.ID && (replica.Partition.Leader == b.config.ID) {
			// is command asking this broker to be the new leader for p and this broker is not already the leader for

			if err := b.startReplica(replica); err != protocol.ErrNone {
				setErr(i, p, err)
				continue
			}

			if err := b.becomeLeader(replica, p); err != protocol.ErrNone {
				setErr(i, p, err)
				continue
			}
		} else if contains(p.Replicas, b.config.ID) && (p.Leader != b.config.ID) {
			// is command asking this broker to follow leader who it isn't a leader of already
			if err := b.startReplica(replica); err != protocol.ErrNone {
				setErr(i, p, err)
				continue
			}

			if err := b.becomeFollower(replica, p); err != protocol.ErrNone {
				setErr(i, p, err)
				continue
			}
		}
		res.Partitions[i] = &protocol.LeaderAndISRPartition{Partition: p.Partition, Topic: p.Topic, ErrorCode: protocol.ErrNone.Code()}
	}
	return res
}

func (b *Broker) handleOffsets(ctx *Context, req *protocol.OffsetsRequest) *protocol.OffsetsResponse {
	sp := span(ctx, b.tracer, "offsets")
	defer sp.Finish()
	res := new(protocol.OffsetsResponse)
	res.APIVersion = req.Version()
	res.Responses = make([]*protocol.OffsetResponse, len(req.Topics))
	for i, t := range req.Topics {
		res.Responses[i] = new(protocol.OffsetResponse)
		res.Responses[i].Topic = t.Topic
		res.Responses[i].PartitionResponses = make([]*protocol.PartitionResponse, 0, len(t.Partitions))
		for _, p := range t.Partitions {
			pres := new(protocol.PartitionResponse)
			pres.Partition = p.Partition
			replica, err := b.replicaLookup.Replica(t.Topic, p.Partition)
			if err != nil {
				// TODO: have replica lookup return an error with a code
				pres.ErrorCode = protocol.ErrUnknown.Code()
				continue
			}
			var offset int64
			if p.Timestamp == -2 {
				offset = replica.Log.OldestOffset()
			} else {
				// TODO: this is nil because i'm not sending the leader and isr requests telling the new leader to start the replica and instantiate the log...
				offset = replica.Log.NewestOffset()
			}
			pres.Offsets = []int64{offset}
			res.Responses[i].PartitionResponses = append(res.Responses[i].PartitionResponses, pres)
		}
	}
	return res
}

func (b *Broker) handleProduce(ctx *Context, req *protocol.ProduceRequest) *protocol.ProduceResponse {
	sp := span(ctx, b.tracer, "produce")
	defer sp.Finish()
	res := new(protocol.ProduceResponse)
	res.APIVersion = req.Version()
	res.Responses = make([]*protocol.ProduceTopicResponse, len(req.TopicData))
	log.Debug.Printf("broker/%d: produce: %#v", b.config.ID, req)
	for i, td := range req.TopicData {
		log.Debug.Printf("broker/%d: produce to partition: %d: %v", b.config.ID, i, td)
		tres := make([]*protocol.ProducePartitionResponse, len(td.Data))
		for j, p := range td.Data {
			pres := &protocol.ProducePartitionResponse{}
			pres.Partition = p.Partition
			err := b.withTimeout(req.Timeout, func() protocol.Error {
				state := b.fsm.State()
				_, t, err := state.GetTopic(td.Topic)
				if err != nil {
					log.Error.Printf("broker/%d: produce to partition error: get topic: %s", b.config.ID, err)
					return protocol.ErrUnknown.WithErr(err)
				}
				if t == nil {
					log.Error.Printf("broker/%d: produce to partition error: unknown topic", b.config.ID)
					return protocol.ErrUnknownTopicOrPartition
				}
				replica, err := b.replicaLookup.Replica(td.Topic, p.Partition)
				if err != nil || replica == nil || replica.Log == nil {
					log.Error.Printf("broker/%d: produce to partition error: %s", b.config.ID, err)
					pres.Partition = p.Partition
					return protocol.ErrReplicaNotAvailable
				}
				offset, appendErr := replica.Log.Append(p.RecordSet)
				if appendErr != nil {
					log.Error.Printf("broker/%d: log append error: %s", b.config.ID, err)
					return protocol.ErrUnknown
				}
				pres.BaseOffset = offset
				pres.LogAppendTime = time.Now()
				return protocol.ErrNone
			})
			pres.ErrorCode = err.Code()
			tres[j] = pres
		}
		res.Responses[i] = &protocol.ProduceTopicResponse{
			Topic:              td.Topic,
			PartitionResponses: tres,
		}
	}
	return res
}

func (b *Broker) handleMetadata(ctx *Context, req *protocol.MetadataRequest) *protocol.MetadataResponse {
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
					PartitionID:        id,
					PartitionErrorCode: protocol.ErrUnknown.Code(),
				})
				continue
			}
			if p == nil {
				partitionMetadata = append(partitionMetadata, &protocol.PartitionMetadata{
					PartitionID:        id,
					PartitionErrorCode: protocol.ErrUnknownTopicOrPartition.Code(),
				})
				continue
			}
			partitionMetadata = append(partitionMetadata, &protocol.PartitionMetadata{
				PartitionID:        p.ID,
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
	res := &protocol.MetadataResponse{
		Brokers:       brokers,
		TopicMetadata: topicMetadata,
	}
	res.APIVersion = req.Version()
	return res
}

func (b *Broker) handleFindCoordinator(ctx *Context, req *protocol.FindCoordinatorRequest) *protocol.FindCoordinatorResponse {
	sp := span(ctx, b.tracer, "find coordinator")
	defer sp.Finish()

	res := &protocol.FindCoordinatorResponse{}
	res.APIVersion = req.Version()

	var broker *metadata.Broker
	var p *structs.Partition
	var i int32

	state := b.fsm.State()

	topic, err := b.offsetsTopic(ctx)
	if err != nil {
		goto ERROR
	}
	i = int32(util.Hash(req.CoordinatorKey) % uint64(len(topic.Partitions)))
	_, p, err = state.GetPartition(OffsetsTopicName, i)
	if err != nil {
		goto ERROR
	}
	if p == nil {
		res.ErrorCode = protocol.ErrUnknownTopicOrPartition.Code()
		goto ERROR
	}
	broker = b.brokerLookup.BrokerByID(raft.ServerID(fmt.Sprintf("%d", p.Leader)))

	res.Coordinator.NodeID = broker.ID.Int32()
	res.Coordinator.Host = broker.Host()
	res.Coordinator.Port = broker.Port()

	return res

ERROR:
	// todo: which err code to use?
	if res.ErrorCode == 0 {
		res.ErrorCode = protocol.ErrUnknown.Code()
	}
	log.Error.Printf("broker/%d: broker: %v: coordinator error: %s", b.config.ID, broker, err)

	return res
}

func (b *Broker) handleJoinGroup(ctx *Context, r *protocol.JoinGroupRequest) *protocol.JoinGroupResponse {
	sp := span(ctx, b.tracer, "join group")
	defer sp.Finish()

	res := &protocol.JoinGroupResponse{}
	res.APIVersion = r.Version()

	// // TODO: distribute this.
	state := b.fsm.State()

	_, group, err := state.GetGroup(r.GroupID)
	if err != nil {
		log.Error.Printf("broker/%d: get group error: %s", b.config.ID, err)
		res.ErrorCode = protocol.ErrUnknown.Code()
		return res
	}
	// TODO: only try to create the group if the group is not unknown AND
	// the member id is UNKNOWN, if member is specified but group does not
	// exist we should reject the request
	if group == nil {
		// group doesn't exist so let's create it
		group = &structs.Group{
			Group:       r.GroupID,
			Coordinator: b.config.ID,
			Members:     make(map[string]structs.Member),
		}
	}
	if r.MemberID == "" {
		// for group member IDs -- can replace with something else
		r.MemberID = ctx.Header().ClientID + "-" + uuid.NewV1().String()
		group.Members[r.MemberID] = structs.Member{ID: r.MemberID}
	}
	if group.LeaderID == "" {
		group.LeaderID = r.MemberID
	}
	_, err = b.raftApply(structs.RegisterGroupRequestType, structs.RegisterGroupRequest{
		Group: *group,
	})
	if err != nil {
		log.Error.Printf("broker/%d: register group error: %s", b.config.ID, err)
		res.ErrorCode = protocol.ErrUnknown.Code()
		return res
	}

	res.GenerationID = 0
	res.LeaderID = group.LeaderID
	res.MemberID = r.MemberID

	if res.LeaderID == res.MemberID {
		// fill in members on response, we only do this for the leader to reduce overhead
		for _, m := range group.Members {
			res.Members = append(res.Members, protocol.Member{MemberID: m.ID, MemberMetadata: m.Metadata})
		}

	}

	return res
}

func (b *Broker) handleLeaveGroup(ctx *Context, r *protocol.LeaveGroupRequest) *protocol.LeaveGroupResponse {
	sp := span(ctx, b.tracer, "leave group")
	defer sp.Finish()

	res := &protocol.LeaveGroupResponse{}
	res.APIVersion = r.Version()

	// // TODO: distribute this.
	state := b.fsm.State()

	_, group, err := state.GetGroup(r.GroupID)
	if err != nil {
		res.ErrorCode = protocol.ErrUnknown.Code()
		return res
	}
	if group == nil {
		res.ErrorCode = protocol.ErrInvalidGroupId.Code()
		return res
	}
	if _, ok := group.Members[r.MemberID]; !ok {
		res.ErrorCode = protocol.ErrUnknownMemberId.Code()
		return res
	}

	delete(group.Members, r.MemberID)

	_, err = b.raftApply(structs.RegisterGroupRequestType, structs.RegisterGroupRequest{
		Group: *group,
	})
	if err != nil {
		res.ErrorCode = protocol.ErrUnknown.Code()
		return res
	}

	return res
}

func (b *Broker) handleSyncGroup(ctx *Context, r *protocol.SyncGroupRequest) *protocol.SyncGroupResponse {
	sp := span(ctx, b.tracer, "sync group")
	defer sp.Finish()

	state := b.fsm.State()
	res := &protocol.SyncGroupResponse{}
	res.APIVersion = r.Version()

	_, group, err := state.GetGroup(r.GroupID)
	if err != nil {
		res.ErrorCode = protocol.ErrUnknown.Code()
		return res
	}
	if group == nil {
		res.ErrorCode = protocol.ErrInvalidGroupId.Code()
		return res
	}
	if _, ok := group.Members[r.MemberID]; !ok {
		res.ErrorCode = protocol.ErrUnknownMemberId.Code()
		return res
	}
	if r.GenerationID != group.GenerationID {
		res.ErrorCode = protocol.ErrIllegalGeneration.Code()
		return res
	}
	switch group.State {
	case structs.GroupStateEmpty, structs.GroupStateDead:
		res.ErrorCode = protocol.ErrUnknownMemberId.Code()
		return res
	case structs.GroupStatePreparingRebalance:
		res.ErrorCode = protocol.ErrRebalanceInProgress.Code()
		return res
	case structs.GroupStateCompletingRebalance:
		// TODO: wait to get member in group

		if group.LeaderID == r.MemberID {
			// if is leader, attempt to persist state and transition to stable
			var assignment []protocol.GroupAssignment
			for _, ga := range r.GroupAssignments {
				if _, ok := group.Members[ga.MemberID]; !ok {
					// if member isn't set fill in with empty assignment
					assignment = append(assignment, protocol.GroupAssignment{
						MemberID:         ga.MemberID,
						MemberAssignment: nil,
					})
				} else {
					assignment = append(assignment, ga)
				}

				// save group
			}
		}
	case structs.GroupStateStable:
		// in stable, return current assignment

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
			res.ErrorCode = protocol.ErrUnknown.Code()
			return res
		}
	} else {
		// TODO: need to wait until leader sets assignments
		if m, ok := group.Members[r.MemberID]; ok {
			res.MemberAssignment = m.Assignment
		} else {
			panic(fmt.Errorf("sync group: unknown member: %s", r.MemberID))
		}
	}

	return res
}

func (b *Broker) handleHeartbeat(ctx *Context, r *protocol.HeartbeatRequest) *protocol.HeartbeatResponse {
	sp := span(ctx, b.tracer, "heartbeat")
	defer sp.Finish()

	res := &protocol.HeartbeatResponse{}
	res.APIVersion = r.Version()

	state := b.fsm.State()
	_, group, err := state.GetGroup(r.GroupID)
	if err != nil {
		res.ErrorCode = protocol.ErrUnknown.Code()
		return res
	}
	if group == nil {
		res.ErrorCode = protocol.ErrInvalidGroupId.Code()
		return res
	}
	// TODO: need to handle case when rebalance is in process

	res.ErrorCode = protocol.ErrNone.Code()

	return res
}

func (b *Broker) handleFetch(ctx *Context, r *protocol.FetchRequest) *protocol.FetchResponse {
	sp := span(ctx, b.tracer, "fetch")
	defer sp.Finish()
	fres := &protocol.FetchResponse{
		Responses: make(protocol.FetchTopicResponses, len(r.Topics)),
	}
	fres.APIVersion = r.Version()
	for i, topic := range r.Topics {
		fr := &protocol.FetchTopicResponse{
			Topic:              topic.Topic,
			PartitionResponses: make([]*protocol.FetchPartitionResponse, len(topic.Partitions)),
		}
		for j, p := range topic.Partitions {
			fpres := &protocol.FetchPartitionResponse{}
			fpres.Partition = p.Partition
			err := b.withTimeout(r.MaxWaitTime, func() protocol.Error {
				replica, err := b.replicaLookup.Replica(topic.Topic, p.Partition)
				if err != nil {
					return protocol.ErrReplicaNotAvailable
				}
				if replica.Partition.Leader != b.config.ID {
					return protocol.ErrNotLeaderForPartition
				}
				if replica.Log == nil {
					return protocol.ErrReplicaNotAvailable
				}
				rdr, rdrErr := replica.Log.NewReader(p.FetchOffset, p.MaxBytes)
				if rdrErr != nil {
					log.Error.Printf("broker/%d: replica log read error: %s", b.config.ID, rdrErr)
					return protocol.ErrUnknown.WithErr(rdrErr)
				}
				buf := new(bytes.Buffer)
				var n int32
				for n < r.MinBytes {
					// TODO: copy these bytes to outer bytes
					nn, err := io.Copy(buf, rdr)
					if err != nil && err != io.EOF {
						log.Error.Printf("broker/%d: reader copy error", b.config.ID, err)
						return protocol.ErrUnknown.WithErr(rdrErr)
					}
					n += int32(nn)
					if err == io.EOF {
						// TODO: should use a different error here?
						break
					}
				}
				fpres.HighWatermark = replica.Log.NewestOffset() - 1
				fpres.RecordSet = buf.Bytes()
				return protocol.ErrNone
			})
			fpres.ErrorCode = err.Code()
			fr.PartitionResponses[j] = fpres
		}
		fres.Responses[i] = fr
	}
	return fres
}

func (b *Broker) handleSaslHandshake(ctx *Context, req *protocol.SaslHandshakeRequest) *protocol.SaslHandshakeResponse {
	panic("not implemented: sasl handshake")
	return nil
}

func (b *Broker) handleListGroups(ctx *Context, req *protocol.ListGroupsRequest) *protocol.ListGroupsResponse {
	sp := span(ctx, b.tracer, "create topic")
	defer sp.Finish()
	res := new(protocol.ListGroupsResponse)
	res.APIVersion = req.Version()
	state := b.fsm.State()

	fmt.Println("list")
	fmt.Println("list")
	fmt.Println("list")

	_, groups, err := state.GetGroups()
	if err != nil {
		res.ErrorCode = protocol.ErrUnknown.Code()
		return res
	}
	for _, group := range groups {
		res.Groups = append(res.Groups, protocol.ListGroup{
			GroupID: group.Group,
			// TODO: add protocol type
			ProtocolType: "consumer",
		})
	}
	return res
}

func (b *Broker) handleDescribeGroups(ctx *Context, req *protocol.DescribeGroupsRequest) *protocol.DescribeGroupsResponse {
	sp := span(ctx, b.tracer, "create topic")
	defer sp.Finish()
	res := new(protocol.DescribeGroupsResponse)
	res.APIVersion = req.Version()
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
			res.Groups = append(res.Groups, group)
			return res
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
		res.Groups = append(res.Groups)

	}

	return res
}

func (b *Broker) handleStopReplica(ctx *Context, req *protocol.StopReplicaRequest) *protocol.StopReplicaResponse {
	panic("not implemented: stop replica")
	return nil
}

func (b *Broker) handleUpdateMetadata(ctx *Context, req *protocol.UpdateMetadataRequest) *protocol.UpdateMetadataResponse {
	panic("not implemented: update metadata")
	return nil
}

func (b *Broker) handleControlledShutdown(ctx *Context, req *protocol.ControlledShutdownRequest) *protocol.ControlledShutdownResponse {
	panic("not implemented: controlled shutdown")
	return nil
}

func (b *Broker) handleOffsetCommit(ctx *Context, req *protocol.OffsetCommitRequest) *protocol.OffsetCommitResponse {
	panic("not implemented: offset commit")
	return nil
}

func (b *Broker) handleOffsetFetch(ctx *Context, req *protocol.OffsetFetchRequest) *protocol.OffsetFetchResponse {
	sp := span(ctx, b.tracer, "create topic")
	defer sp.Finish()

	res := new(protocol.OffsetFetchResponse)
	res.APIVersion = req.Version()
	res.Responses = make([]protocol.OffsetFetchTopicResponse, len(req.Topics))

	// state := b.fsm.State()

	// _, g, err := state.GetGroup(req.GroupID)

	// // If group doesn't exist then create it?
	// if err != nil {
	// 	// TODO: handle err
	// 	panic(err)
	// }

	return res

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

	state := b.fsm.State()
	_, topic, _ := state.GetTopic(replica.Partition.Topic)

	// TODO: think i need to just ensure/add the topic if it's not here yet

	if topic == nil {
		log.Info.Printf("broker/%d: start replica called on unknown topic: %s", b.config.ID, replica.Partition.Topic)
		return protocol.ErrUnknownTopicOrPartition
	}

	if replica.Log == nil {
		log, err := commitlog.New(commitlog.Options{
			Path:            filepath.Join(b.config.DataDir, "data", fmt.Sprintf("%d", replica.Partition.ID)),
			MaxSegmentBytes: 1024,
			MaxLogBytes:     -1,
			CleanupPolicy:   commitlog.CleanupPolicy(topic.Config.GetValue("cleanup.policy").(string)),
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
func (b *Broker) createTopic(ctx *Context, topic *protocol.CreateTopicRequest) protocol.Error {
	state := b.fsm.State()
	_, t, _ := state.GetTopic(topic.Topic)
	if t != nil {
		return protocol.ErrTopicAlreadyExists
	}
	ps, err := b.buildPartitions(topic.Topic, topic.NumPartitions, topic.ReplicationFactor)
	if err != protocol.ErrNone {
		return err
	}
	tt := structs.Topic{
		Topic:      topic.Topic,
		Partitions: make(map[int32][]int32),
	}
	for _, partition := range ps {
		tt.Partitions[partition.ID] = partition.AR
	}
	// TODO: create/set topic config here
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
			errCode := b.handleLeaderAndISR(ctx, req).ErrorCode
			if protocol.ErrNone.Code() != errCode {
				panic(fmt.Sprintf("broker/%d: handling leader and isr error: %d", b.config.ID, errCode))
			}
		} else {
			conn, err := Dial("tcp", broker.BrokerAddr)
			if err != nil {
				return protocol.ErrUnknown.WithErr(err)
			}
			res, err := conn.LeaderAndISR(req)
			if err != nil {
				// handle err and responses
				return protocol.ErrUnknown.WithErr(err)
			}
			spew.Dump("leader and isr res", res)
		}
	}
	return protocol.ErrNone
}

func (b *Broker) buildPartitions(topic string, partitionsCount int32, replicationFactor int16) ([]structs.Partition, protocol.Error) {
	brokers := b.brokerLookup.Brokers()
	count := len(brokers)

	if int(replicationFactor) > count {
		return nil, protocol.ErrInvalidReplicationFactor
	}

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

	return partitions, protocol.ErrNone
}

// Leave is used to prepare for a graceful shutdown.
func (b *Broker) Leave() error {
	log.Info.Printf("broker/%d: starting leave", b.config.ID)

	numPeers, err := b.numPeers()
	if err != nil {
		log.Error.Printf("broker/%d: check raft peers error: %s", b.config.ID, err)
		return err
	}

	isLeader := b.isLeader()
	if isLeader && numPeers > 1 {
		future := b.raft.RemoveServer(raft.ServerID(fmt.Sprintf("%d", b.config.ID)), 0, 0)
		if err := future.Error(); err != nil {
			log.Error.Printf("broker/%d: remove ourself as raft peer error: %s", b.config.ID, err)
		}
	}

	if b.serf != nil {
		if err := b.serf.Leave(); err != nil {
			log.Error.Printf("broker/%d: leave LAN serf cluster error: %s", b.config.ID, err)
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
				log.Error.Printf("broker/%d: get raft configuration error: %s", b.config.ID, err)
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
	log.Info.Printf("broker/%d: shutting down broker", b.config.ID)
	b.shutdownLock.Lock()
	defer b.shutdownLock.Unlock()

	if b.shutdown {
		return nil
	}
	b.shutdown = true
	close(b.shutdownCh)

	if b.serf != nil {
		b.serf.Shutdown()
	}

	if b.raft != nil {
		b.raftTransport.Close()
		future := b.raft.Shutdown()
		if err := future.Error(); err != nil {
			log.Error.Printf("broker/%d: shutdown error: %s", b.config.ID, err)
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
	broker := b.brokerLookup.BrokerByID(raft.ServerID(fmt.Sprintf("%d", cmd.Leader)))
	if broker == nil {
		return protocol.ErrBrokerNotAvailable
	}
	conn, err := NewDialer(fmt.Sprintf("jocko-replicator-%d", b.config.ID)).Dial("tcp", broker.BrokerAddr)
	if err != nil {
		return protocol.ErrUnknown.WithErr(err)
	}
	r := NewReplicator(ReplicatorConfig{}, replica, conn)
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

func (r Replica) String() string {
	return fmt.Sprintf("replica: %d {broker: %d, leader: %d, hw: %d, leo: %d}", r.Partition.ID, r.BrokerID, r.Partition.Leader, r.Hw, r.Leo)
}

func (b *Broker) offsetsTopic(ctx *Context) (topic *structs.Topic, err error) {
	state := b.fsm.State()

	// check if the topic exists already
	_, topic, err = state.GetTopic(OffsetsTopicName)
	if err != nil {
		return
	}
	if topic != nil {
		return
	}

	// doesn't exist so let's create it
	partitions, err := b.buildPartitions(OffsetsTopicName, 50, b.config.OffsetsTopicReplicationFactor)
	if err != protocol.ErrNone {
		return nil, err
	}
	topic = &structs.Topic{
		Topic:      OffsetsTopicName,
		Internal:   true,
		Partitions: make(map[int32][]int32),
	}
	for _, p := range partitions {
		topic.Partitions[p.Partition] = p.AR
	}
	_, err = b.raftApply(structs.RegisterTopicRequestType, structs.RegisterTopicRequest{
		Topic: *topic,
	})
	for _, partition := range partitions {
		if err := b.createPartition(partition); err != nil {
			return nil, err
		}
	}
	return
}

// debugSnapshot takes a snapshot of this broker's state. Used to debug errors.
func (b *Broker) debugSnapshot() {

}

func (b *Broker) withTimeout(timeout time.Duration, fn func() protocol.Error) protocol.Error {
	if timeout <= 0 {
		go fn()
		return protocol.ErrNone
	}

	c := make(chan protocol.Error, 1)
	defer close(c)

	timer := time.NewTimer(timeout)
	defer timer.Stop()

	go func() {
		c <- fn()
	}()

	select {
	case err := <-c:
		return err
	case <-timer.C:
		return protocol.ErrRequestTimedOut
	}
}

func (b *Broker) logState() {
	t := time.NewTicker(b.logStateInterval)
	for {
		select {
		case <-b.shutdownCh:
			return
		case <-t.C:
			var buf bytes.Buffer
			buf.WriteString("\tmembers:\n")
			members := b.LANMembers()
			for i, member := range members {
				buf.WriteString(fmt.Sprintf("\t\t- %d:\n\t\t\tname: %s\n\t\t\taddr: %s\n\t\t\tstatus: %s\n", i, member.Name, member.Addr, member.Status))
			}
			buf.WriteString("\tnodes:\n")
			state := b.fsm.State()
			_, nodes, err := state.GetNodes()
			if err != nil {
				panic(err)
			}
			for i, node := range nodes {
				buf.WriteString(fmt.Sprintf("\t\t- %d:\n\t\t\tid: %d\n\t\t\tstatus: %s\n", i, node.Node, node.Check.Status))
			}
			_, topics, err := state.GetTopics()
			if err != nil {
				panic(err)
			}
			buf.WriteString("\ttopics:\n")
			for i, topic := range topics {
				buf.WriteString(fmt.Sprintf("\t\t- %d:\n\t\t\tid: %s\n\t\t\tpartitions: %v\n", i, topic.Topic, topic.Partitions))
			}
			log.Info.Printf("broker/%d: state:\n%s", b.config.ID, buf.String())
		}
	}
}
