package broker

import (
	"encoding/json"
	"fmt"
	"io"
	"net"
	"os"
	"path"
	"path/filepath"
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

type CmdType int

const (
	addPartition CmdType = iota
	addBroker
	removeBroker
	deleteTopic
)

var (
	ErrTopicExists = errors.New("topic exists already")
)

type command struct {
	Cmd  CmdType          `json:"type"`
	Data *json.RawMessage `json:"data"`
}

func newCommand(cmd CmdType, data interface{}) (c command, err error) {
	var b []byte
	b, err = json.Marshal(data)
	if err != nil {
		return c, err
	}
	r := json.RawMessage(b)
	return command{
		Cmd:  cmd,
		Data: &r,
	}, nil
}

type Broker struct {
	*replicationManager
	mu     sync.RWMutex
	logger *simplelog.Logger

	id      int32
	host    string
	port    string
	topics  map[string][]*jocko.Partition
	brokers []*jocko.BrokerConn

	dataDir  string
	raftAddr string
	tcpAddr  string
	logDir   string

	raft          *raft.Raft
	raftPeers     raft.PeerStore
	raftTransport *raft.NetworkTransport
	raftStore     *raftboltdb.BoltStore
	raftLeaderCh  chan bool

	serf            *serf.Serf
	serfReconcileCh chan serf.Member
	serfEventCh     chan serf.Event

	reconcileInterval time.Duration
	reconcileCh       chan serf.Member

	left         bool
	shutdownCh   chan struct{}
	shutdown     bool
	shutdownLock sync.Mutex
}

const (
	raftState    = "raft/"
	serfSnapshot = "serf/snapshot"
)

func New(id int32, opts ...Option) (*Broker, error) {
	var err error
	b := &Broker{
		replicationManager: newReplicationManager(),
		id:                 id,
		topics:             make(map[string][]*jocko.Partition),
		serfReconcileCh:    make(chan serf.Member, 32),
		serfEventCh:        make(chan serf.Event, 256),
		reconcileInterval:  time.Second * 60,
	}
	for _, o := range opts {
		o.modifyBroker(b)
	}

	serfConfig := serf.DefaultConfig()
	b.serf, err = b.setupSerf(serfConfig, b.serfEventCh, serfSnapshot)
	if err != nil {
		return nil, err
	}

	host, port, err := net.SplitHostPort(b.tcpAddr)
	if err != nil {
		return nil, err
	}

	b.host = host
	b.port = port

	b.brokers = append(b.brokers, &jocko.BrokerConn{
		Host:     host,
		Port:     port,
		RaftAddr: b.raftAddr,
		ID:       b.id,
	})

	if err = b.setupRaft(); err != nil {
		return nil, err
	}

	return b, nil
}

func (b *Broker) setupRaft() error {
	conf := raft.DefaultConfig()

	addr, err := net.ResolveTCPAddr("tcp", b.raftAddr)
	if err != nil {
		return errors.Wrap(err, "resolve bind addr failed")
	}

	if b.raftTransport == nil {
		b.raftTransport, err = raft.NewTCPTransport(b.raftAddr, addr, 3, timeout, os.Stderr)
		if err != nil {
			return errors.Wrap(err, "tcp transport failed")
		}
	}

	path := filepath.Join(b.dataDir, raftState)
	if err = os.MkdirAll(path, 0755); err != nil {
		return errors.Wrap(err, "data directory mkdir failed")
	}

	b.raftPeers = raft.NewJSONPeers(path, b.raftTransport)

	if len(b.brokers) == 1 {
		conf.EnableSingleNode = true
	} else {
		var peers []string
		for _, b := range b.brokers {
			peers = append(peers, b.RaftAddr)
		}
		err = b.raftPeers.SetPeers(peers)
		if err != nil {
			return errors.Wrap(err, "set peers failed")
		}
	}

	snapshots, err := raft.NewFileSnapshotStore(path, 2, os.Stderr)
	if err != nil {
		return err
	}

	boltStore, err := raftboltdb.NewBoltStore(filepath.Join(path, "raft.db"))
	if err != nil {
		return errors.Wrap(err, "bolt store failed")
	}
	b.raftStore = boltStore

	leaderCh := make(chan bool, 1)
	b.raftLeaderCh = leaderCh
	conf.NotifyCh = leaderCh

	raft, err := raft.NewRaft(conf, b, boltStore, boltStore, snapshots, b.raftPeers, b.raftTransport)
	if err != nil {
		if b.raftStore != nil {
			b.raftStore.Close()
		}
		b.raftTransport.Close()
		return errors.Wrap(err, "raft failed")
	}
	b.raft = raft

	return nil
}

func (b *Broker) setupSerf(conf *serf.Config, eventCh chan serf.Event, serfSnapshot string) (*serf.Serf, error) {
	conf.Init()
	conf.NodeName = fmt.Sprintf("%d", b.id)
	conf.EventCh = eventCh
	conf.EnableNameConflictResolution = false
	return serf.Create(conf)
}

func (b *Broker) ID() int32 {
	return b.id
}

func (b *Broker) Host() string {
	return b.host
}

func (b *Broker) Port() string {
	return b.port
}

func (b *Broker) Cluster() []*jocko.BrokerConn {
	return b.brokers
}

func (s *Broker) Close() error {
	return s.raft.Shutdown().Error()
}

// IsController checks if this broker is the cluster controller
func (s *Broker) IsController() bool {
	return s.raft.State() == raft.Leader
}

func (s *Broker) ControllerID() string {
	return s.raft.Leader()
}

func (s *Broker) TopicPartitions(topic string) (found []*jocko.Partition, err error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.topics[topic], nil
}

func (s *Broker) Partition(topic string, partition int32) (*jocko.Partition, error) {
	found, err := s.TopicPartitions(topic)
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

func (s *Broker) AddPartition(partition *jocko.Partition) error {
	return s.apply(addPartition, partition)
}

func (s *Broker) AddBroker(broker jocko.BrokerConn) error {
	return s.apply(addBroker, broker)
}

func (s *Broker) apply(cmdType CmdType, data interface{}) error {
	c, err := newCommand(cmdType, data)
	if err != nil {
		return err
	}
	b, err := json.Marshal(c)
	if err != nil {
		return err
	}
	f := s.raft.Apply(b, timeout)
	return f.Error()
}

func (s *Broker) BrokerConn(id int32) *jocko.BrokerConn {
	for _, b := range s.brokers {
		if b.ID == id {
			return b
		}
	}
	return nil
}

func (s *Broker) addPartition(partition *jocko.Partition) {
	s.mu.RLock()
	if v, ok := s.topics[partition.Topic]; ok {
		s.topics[partition.Topic] = append(v, partition)
	} else {
		s.topics[partition.Topic] = []*jocko.Partition{partition}
	}
	s.mu.RUnlock()
	if s.IsLeaderOfPartition(partition.Topic, partition.ID, partition.LeaderID()) {
		commitLog, err := commitlog.New(commitlog.Options{
			Path:            path.Join(s.logDir, partition.String()),
			MaxSegmentBytes: 1024,
			MaxLogBytes:     -1,
		})
		if err != nil {
			panic(err)
		}
		if err = commitLog.Init(); err != nil {
			panic(err)
		}
		if err = commitLog.Open(); err != nil {
			panic(err)
		}
		partition.CommitLog = commitLog
	}
}

func (s *Broker) addBroker(broker *jocko.BrokerConn) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.brokers = append(s.brokers, broker)
}

func (s *Broker) IsLeaderOfPartition(topic string, pid int32, lid int32) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	for _, p := range s.topics[topic] {
		if p.ID == pid {
			if lid == s.id {
				return true
			}
			break
		}
	}
	return false
}

func (s *Broker) Topics() []string {
	topics := []string{}
	for k := range s.topics {
		topics = append(topics, k)
	}
	return topics
}

// Join is used to have the broker join the gossip ring
// The target address should be another broker listening on the Serf address
func (s *Broker) Join(id int32, addrs ...string) (int, error) {
	return s.serf.Join(addrs, true)
}

func (s *Broker) Apply(l *raft.Log) interface{} {
	var c command
	if err := json.Unmarshal(l.Data, &c); err != nil {
		panic(errors.Wrap(err, "json unmarshal failed"))
	}
	s.logger.Debug("broker/apply cmd [%d]", c.Cmd)
	switch c.Cmd {
	case addBroker:
		broker := new(jocko.BrokerConn)
		b, err := c.Data.MarshalJSON()
		if err != nil {
			panic(errors.Wrap(err, "json unmarshal failed"))
		}
		if err := json.Unmarshal(b, broker); err != nil {
			panic(errors.Wrap(err, "json unmarshal failed"))
		}
		s.addBroker(broker)
	case addPartition:
		p := new(jocko.Partition)
		b, err := c.Data.MarshalJSON()
		if err != nil {
			panic(errors.Wrap(err, "json unmarshal failed"))
		}
		if err := json.Unmarshal(b, p); err != nil {
			panic(errors.Wrap(err, "json unmarshal failed"))
		}
		s.addPartition(p)
	case deleteTopic:
		p := new(jocko.Partition)
		b, err := c.Data.MarshalJSON()
		if err != nil {
			panic(errors.Wrap(err, "json unmarshal failed"))
		}
		if err := json.Unmarshal(b, p); err != nil {
			panic(errors.Wrap(err, "json unmarshal failed"))
		}
		if err := s.deleteTopic(p); err != nil {
			panic(errors.Wrap(err, "topic delete failed"))
		}
	}
	return nil
}

// CreateTopic creates topic with partitions count.
func (s *Broker) CreateTopic(topic string, partitions int32) error {
	for _, t := range s.Topics() {
		if t == topic {
			return ErrTopicExists
		}
	}
	brokers := s.brokers
	for i := 0; i < int(partitions); i++ {
		broker := brokers[i%len(brokers)]
		partition := &jocko.Partition{
			Topic:           topic,
			ID:              int32(i),
			Leader:          broker,
			PreferredLeader: broker,
			Replicas:        []*jocko.BrokerConn{broker},
		}
		if err := s.AddPartition(partition); err != nil {
			return err
		}
	}
	return nil
}

// DeleteTopics creates topic with partitions count.
func (s *Broker) DeleteTopics(topics ...string) error {
	for _, topic := range topics {
		if err := s.DeleteTopic(topic); err != nil {
			return err
		}
	}

	return nil
}

func (s *Broker) DeleteTopic(topic string) error {
	return s.apply(deleteTopic, &jocko.Partition{Topic: topic})
}

func (s *Broker) deleteTopic(tp *jocko.Partition) error {
	partitions, err := s.TopicPartitions(tp.Topic)
	if err != nil {
		return err
	}
	for _, p := range partitions {
		if err := p.Delete(); err != nil {
			return err
		}
	}
	s.mu.Lock()
	delete(s.topics, tp.Topic)
	s.mu.Unlock()
	return nil
}

func (s *Broker) Restore(rc io.ReadCloser) error {
	return nil
}

type FSMSnapshot struct {
}

func (f *FSMSnapshot) Persist(sink raft.SnapshotSink) error {
	return nil
}

func (f *FSMSnapshot) Release() {}

func (s *Broker) Snapshot() (raft.FSMSnapshot, error) {
	return &FSMSnapshot{}, nil
}

func (s *Broker) WaitForLeader(timeout time.Duration) (string, error) {
	tick := time.NewTicker(waitDelay)
	defer tick.Stop()

	timer := time.NewTimer(timeout)
	defer timer.Stop()

	for {
		select {
		case <-tick.C:
			l := s.raft.Leader()
			if l != "" {
				return l, nil
			}
		case <-timer.C:
		}
	}
}

func (s *Broker) WaitForAppliedIndex(idx uint64, timeout time.Duration) error {
	tick := time.NewTicker(waitDelay)
	defer tick.Stop()
	timer := time.NewTimer(timeout)
	defer timer.Stop()

	for {
		select {
		case <-tick.C:
			if s.raft.AppliedIndex() >= idx {
				return nil
			}
		case <-timer.C:
		}
	}
}
