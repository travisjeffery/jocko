package broker

import (
	"net"
	"path"
	"strconv"
	"sync"
	"time"

	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb"
	"github.com/hashicorp/serf/serf"
	"github.com/pkg/errors"
	"github.com/travisjeffery/jocko/commitlog"
	"github.com/travisjeffery/jocko/jocko"
	"github.com/travisjeffery/simplelog"
)

const (
	timeout   = 10 * time.Second
	waitDelay = 100 * time.Millisecond
)

var (
	ErrTopicExists = errors.New("topic exists already")
)

type Broker struct {
	*replicationManager
	mu     sync.RWMutex
	logger *simplelog.Logger

	id     int32
	host   string
	port   int
	topics map[string][]*jocko.Partition

	peers    map[int32]*jocko.BrokerConn
	peerLock sync.Mutex

	dataDir             string
	brokerAddr          string
	logDir              string
	devDisableBootstrap bool

	raft          *raft.Raft
	raftAddr      string
	raftPeers     raft.PeerStore
	raftTransport *raft.NetworkTransport
	raftStore     *raftboltdb.BoltStore
	raftLeaderCh  chan bool
	raftConfig    *raft.Config

	serf                  *serf.Serf
	serfAddr              string
	serfReconcileCh       chan serf.Member
	serfReconcileInterval time.Duration
	serfEventCh           chan serf.Event
	serfMembers           []string

	left         bool
	shutdownCh   chan struct{}
	shutdown     bool
	shutdownLock sync.Mutex
}

const (
	raftState    = "raft/"
	serfSnapshot = "serf/snapshot"
)

func New(id int32, opts ...BrokerFn) (*Broker, error) {
	var err error
	b := &Broker{
		raftConfig:            raft.DefaultConfig(),
		replicationManager:    newReplicationManager(),
		peers:                 make(map[int32]*jocko.BrokerConn),
		id:                    id,
		topics:                make(map[string][]*jocko.Partition),
		serfReconcileCh:       make(chan serf.Member, 32),
		serfEventCh:           make(chan serf.Event, 256),
		serfReconcileInterval: time.Second * 5,
		shutdownCh:            make(chan struct{}),
	}

	for _, o := range opts {
		o(b)
	}

	host, strPort, err := net.SplitHostPort(b.brokerAddr)
	if err != nil {
		return nil, err
	}

	port, err := strconv.Atoi(strPort)
	if err != nil {
		return nil, err
	}

	b.host = host
	b.port = port

	serfConfig := serf.DefaultConfig()
	b.serf, err = b.setupSerf(serfConfig, b.serfEventCh, serfSnapshot)
	if err != nil {
		// b.Shutdown()
		b.logger.Info("failed to start serf: %s", err)
		return nil, err
	}

	// bootstrap serf members.
	if len(b.serfMembers) != 0 {
		if _, err := b.serf.Join(b.serfMembers, true); err != nil {
			return nil, err
		}
	}

	if err = b.setupRaft(); err != nil {
		return nil, err
	}

	// monitor leadership changes
	go b.monitorLeadership()

	// ingest events for serf
	go b.serfEventHandler()

	return b, nil
}

// ID is used to get the broker's ID
func (b *Broker) ID() int32 {
	return b.id
}

// Host is used to get Broker's host
func (b *Broker) Host() string {
	return b.host
}

func (b *Broker) Port() int {
	return b.port
}

func (b *Broker) Cluster() []*jocko.BrokerConn {
	b.peerLock.Lock()
	defer b.peerLock.Unlock()

	cluster := make([]*jocko.BrokerConn, 0, len(b.peers))
	for _, v := range b.peers {
		cluster = append(cluster, v)
	}
	return cluster
}

// IsController checks if this broker is the cluster controller
func (b *Broker) IsController() bool {
	return b.raft.State() == raft.Leader
}

func (b *Broker) ControllerID() string {
	return b.raft.Leader()
}

func (b *Broker) TopicPartitions(topic string) (found []*jocko.Partition, err error) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.topics[topic], nil
}

func (b *Broker) Partition(topic string, partition int32) (*jocko.Partition, error) {
	found, err := b.TopicPartitions(topic)
	if err != nil {
		return nil, err
	}
	for _, f := range found {
		if f.ID == partition {
			return f, nil
		}
	}
	return nil, errors.New("partition not found")
}

func (b *Broker) AddPartition(partition *jocko.Partition) error {
	return b.raftApply(addPartition, partition)
}

func (b *Broker) AddBroker(broker jocko.BrokerConn) error {
	return b.raftApply(addBroker, broker)
}

func (b *Broker) BrokerConn(id int32) *jocko.BrokerConn {
	for _, b := range b.Cluster() {
		if b.ID == id {
			return b
		}
	}
	return nil
}

func (b *Broker) StartReplica(partition *jocko.Partition) error {
	b.mu.Lock()
	if v, ok := b.topics[partition.Topic]; ok {
		b.topics[partition.Topic] = append(v, partition)
	} else {
		b.topics[partition.Topic] = []*jocko.Partition{partition}
	}
	b.mu.Unlock()
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
			return err
		}
		if err = commitLog.Init(); err != nil {
			return err
		}
		if err = commitLog.Open(); err != nil {
			return err
		}
		partition.CommitLog = commitLog

		b.peerLock.Lock()
		partition.Conn = b.peers[partition.LeaderID()]
		b.peerLock.Unlock()
	}
	return nil
}

func (b *Broker) addBroker(broker *jocko.BrokerConn) {
	// TODO: remove this
	b.peerLock.Lock()
	b.peers[broker.ID] = broker
	b.peerLock.Unlock()
}

func (b *Broker) IsLeaderOfPartition(topic string, pid int32, lid int32) bool {
	b.mu.RLock()
	defer b.mu.RUnlock()
	result := false
	for _, p := range b.topics[topic] {
		if p.ID == pid {
			result = lid == p.LeaderID()
			break
		}
	}
	return result
}

func (b *Broker) Topics() []string {
	topics := []string{}
	for k := range b.topics {
		topics = append(topics, k)
	}
	return topics
}

// Join is used to have the broker join the gossip ring
// The target address should be another broker listening on the Serf address
func (b *Broker) Join(addrs ...string) (int, error) {
	return b.serf.Join(addrs, true)
}

// CreateTopic creates topic with partitions count.
func (b *Broker) CreateTopic(topic string, partitions int32) error {
	for _, t := range b.Topics() {
		if t == topic {
			return ErrTopicExists
		}
	}

	c := b.Cluster()
	cLen := int32(len(c))

	for i := int32(0); i < partitions; i++ {
		// TODO: need to know replica assignment here
		partition := &jocko.Partition{
			Topic:           topic,
			ID:              i,
			Leader:          c[i%cLen].ID,
			PreferredLeader: c[i%cLen].ID,
			Replicas:        []int32{},
		}
		if err := b.AddPartition(partition); err != nil {
			return err
		}
	}
	return nil
}

// DeleteTopics creates topic with partitions count.
func (b *Broker) DeleteTopics(topics ...string) error {
	for _, topic := range topics {
		if err := b.DeleteTopic(topic); err != nil {
			return err
		}
	}
	return nil
}

func (b *Broker) DeleteTopic(topic string) error {
	return b.raftApply(deleteTopic, &jocko.Partition{Topic: topic})
}

func (b *Broker) deleteTopic(tp *jocko.Partition) error {
	partitions, err := b.TopicPartitions(tp.Topic)
	if err != nil {
		return err
	}
	for _, p := range partitions {
		if err := p.Delete(); err != nil {
			return err
		}
	}
	b.mu.Lock()
	delete(b.topics, tp.Topic)
	b.mu.Unlock()
	return nil
}

// Leave is used to prepare for a graceful shutdown of the server
func (b *Broker) Leave() error {
	b.logger.Info("broker starting to leave")
	b.left = true

	// TODO: handle case if we're the controller/leader

	// leave the gossip pool
	if b.serf != nil {
		if err := b.serf.Leave(); err != nil {
			b.logger.Info("failed to leave serf cluster: %v", err)
		}
	}

	return nil
}

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
		b.serf.Shutdown()
	}

	if b.raft != nil {
		b.raftTransport.Close()
		future := b.raft.Shutdown()
		if err := future.Error(); err != nil {
			b.logger.Info("failed to shutdown raft: %s", err)
		}
		if b.raftStore != nil {
			b.raftStore.Close()
		}
	}

	return nil
}

func (b *Broker) IsShutdown() bool {
	select {
	case <-b.shutdownCh:
		return true
	default:
		return false
	}
}
