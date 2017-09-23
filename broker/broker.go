package broker

import (
	"math/rand"
	"path"
	"sync"

	"github.com/pkg/errors"
	"github.com/travisjeffery/jocko"
	"github.com/travisjeffery/jocko/commitlog"
	"github.com/travisjeffery/jocko/protocol"
	"github.com/travisjeffery/simplelog"
)

var (
	ErrTopicExists = errors.New("topic exists already")
)

// Broker represents a broker in a Jocko cluster, like a broker in a Kafka cluster.
type Broker struct {
	*replicationManager
	mu     sync.RWMutex
	logger *simplelog.Logger

	id         int32
	topics     map[string][]*jocko.Partition
	brokerAddr string
	logDir     string

	raft jocko.Raft
	serf jocko.Serf

	shutdownCh   chan struct{}
	shutdown     bool
	shutdownLock sync.Mutex
}

// New is used to instantiate a new broker.
func New(id int32, opts ...BrokerFn) (*Broker, error) {
	b := &Broker{
		replicationManager: newReplicationManager(),
		id:                 id,
		topics:             make(map[string][]*jocko.Partition),
		shutdownCh:         make(chan struct{}),
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

// ID is used to get the broker's ID
func (b *Broker) ID() int32 {
	return b.id
}

// Cluster is used to get a list of members in the cluster.
func (b *Broker) Cluster() []*jocko.ClusterMember {
	return b.serf.Cluster()
}

// IsController returns true if this broker is the cluster controller.
func (b *Broker) IsController() bool {
	return b.raft.IsLeader()
}

// TopicPartitions is used to get the partitions for the given topic.
func (b *Broker) TopicPartitions(topic string) (found []*jocko.Partition, err protocol.Error) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	if p, ok := b.topics[topic]; !ok {
		return nil, protocol.ErrUnknownTopicOrPartition
	} else {
		return p, protocol.ErrNone
	}
}

func (b *Broker) Topics() map[string][]*jocko.Partition {
	b.mu.RLock()
	defer b.mu.RUnlock()
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

// StartReplica is used to start a replica on this broker, including creating its commit log.
func (b *Broker) StartReplica(partition *jocko.Partition) error {
	b.mu.Lock()
	defer b.mu.Unlock()
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
			return err
		}
		partition.CommitLog = commitLog
		partition.Conn = b.serf.Member(partition.LeaderID())
	}
	return nil
}

// IsLeaderPartitions returns true if the given leader is the leader of the given partition.
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

// Join is used to have the broker join the gossip ring.
// The given address should be another broker listening on the Serf address.
func (b *Broker) Join(addrs ...string) (int, error) {
	return b.serf.Join(addrs...)
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
			return protocol.ErrUnknown
		}
	}
	return protocol.ErrNone
}

// DeleteTopic is used to delete the topic across the cluster.
func (b *Broker) DeleteTopic(topic string) error {
	return b.raftApply(deleteTopic, &jocko.Partition{Topic: topic})
}

// deleteTopic is used to delete the topic from this broker.
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
	b.mu.Lock()
	delete(b.topics, tp.Topic)
	b.mu.Unlock()
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
