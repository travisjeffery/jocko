package broker

import (
	"path"
	"sync"
	"time"

	"github.com/pkg/errors"
	"github.com/travisjeffery/jocko"
	"github.com/travisjeffery/jocko/commitlog"
	"github.com/travisjeffery/simplelog"
)

var (
	ErrTopicExists = errors.New("topic exists already")
)

const (
	addPartition jocko.RaftCmdType = iota
	deleteTopic
	// others
)

type Broker struct {
	*replicationManager
	mu     sync.RWMutex
	logger *simplelog.Logger

	id         int32
	topics     map[string][]*jocko.Partition
	brokerAddr string
	logDir     string

	raft     jocko.Raft
	leaderCh chan bool

	serf              jocko.Serf
	reconcileCh       chan *jocko.ClusterMember
	reconcileInterval time.Duration

	shutdownCh   chan struct{}
	shutdown     bool
	shutdownLock sync.Mutex
}

func New(id int32, opts ...BrokerFn) (*Broker, error) {
	b := &Broker{
		replicationManager: newReplicationManager(),
		id:                 id,
		topics:             make(map[string][]*jocko.Partition),
		reconcileCh:        make(chan *jocko.ClusterMember, 32),
		reconcileInterval:  time.Second * 5,
		shutdownCh:         make(chan struct{}),
		leaderCh:           make(chan bool, 1),
	}

	for _, o := range opts {
		o(b)
	}

	port, err := getPortFromAddr(b.brokerAddr)
	if err != nil {
		return nil, err
	}
	raftPort, err := getPortFromAddr(b.raft.Addr())
	if err != nil {
		return nil, err
	}

	conn := &jocko.ClusterMember{
		ID:       b.id,
		Port:     port,
		RaftPort: raftPort,
	}

	if err := b.serf.Bootstrap(conn, b.reconcileCh); err != nil {
		b.logger.Info("failed to start serf: %s", err)
		return nil, err
	}

	if err := b.raft.Bootstrap(b.serf.Cluster(), b, b.leaderCh); err != nil {
		return nil, err
	}

	// monitor leadership changes
	go b.monitorLeadership()

	return b, nil
}

// ID is used to get the broker's ID
func (b *Broker) ID() int32 {
	return b.id
}

func (b *Broker) Cluster() []*jocko.ClusterMember {
	return b.serf.Cluster()
}

// IsController checks if this broker is the cluster controller
func (b *Broker) IsController() bool {
	return b.raft.IsLeader()
}

func (b *Broker) ControllerID() string {
	return b.raft.LeaderID()
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

func (b *Broker) ClusterMember(id int32) *jocko.ClusterMember {
	return b.serf.Member(id)
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
		partition.Conn = b.serf.Member(partition.LeaderID())
	}
	return nil
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
	return b.serf.Join(addrs...)
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
		leader := c[i%cLen].ID
		partition := &jocko.Partition{
			Topic:           topic,
			ID:              i,
			Leader:          leader,
			PreferredLeader: leader,
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

// leave is used to prepare for a graceful shutdown of the server
func (b *Broker) leave() error {
	b.logger.Info("broker starting to leave")

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
	if err := b.leave(); err != nil {
		return err
	}
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
