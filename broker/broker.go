package broker

import (
	"encoding/json"
	"fmt"
	"net"
	"os"
	"path"
	"path/filepath"
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

	id     int32
	host   string
	port   int
	topics map[string][]*jocko.Partition

	peers    map[int32]*jocko.BrokerConn
	peerLock sync.Mutex

	dataDir             string
	bindAddr            string
	logDir              string
	devDisableBootstrap bool

	raft          *raft.Raft
	raftPort      int
	raftPeers     raft.PeerStore
	raftTransport *raft.NetworkTransport
	raftStore     *raftboltdb.BoltStore
	raftLeaderCh  chan bool
	raftConfig    *raft.Config

	serf                  *serf.Serf
	serfPort              int
	serfAddr              string
	serfReconcileCh       chan serf.Member
	serfReconcileInterval time.Duration
	serfEventCh           chan serf.Event

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
		serfPort:              7946,
		serfAddr:              "0.0.0.0",
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
		o.modifyBroker(b)
	}

	serfConfig := serf.DefaultConfig()
	b.serf, err = b.setupSerf(serfConfig, b.serfEventCh, serfSnapshot)
	if err != nil {
		// b.Shutdown()
		b.logger.Info("failed to start serf: %s", err)
		return nil, err
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

func (b *Broker) setupRaft() (err error) {
	addr := &net.TCPAddr{IP: net.ParseIP(b.bindAddr), Port: b.raftPort}

	if b.raftTransport == nil {
		b.raftTransport, err = raft.NewTCPTransport(addr.String(), nil, 3, timeout, os.Stderr)
		if err != nil {
			return errors.Wrap(err, "tcp transport failed")
		}
	}

	path := filepath.Join(b.dataDir, raftState)
	if err = os.MkdirAll(path, 0755); err != nil {
		return errors.Wrap(err, "data directory mkdir failed")
	}

	b.raftPeers = raft.NewJSONPeers(path, b.raftTransport)

	var peers []string
	for _, p := range b.peers {
		addr := &net.TCPAddr{IP: net.ParseIP(p.IP), Port: p.RaftPort}
		peers = append(peers, addr.String())
	}
	if err = b.raftPeers.SetPeers(peers); err != nil {
		return err
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
	b.raftConfig.NotifyCh = leaderCh
	b.raftConfig.StartAsLeader = !b.devDisableBootstrap

	raft, err := raft.NewRaft(b.raftConfig, b, boltStore, boltStore, snapshots, b.raftPeers, b.raftTransport)
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
	id := fmt.Sprintf("jocko-%03d", b.id)
	conf.MemberlistConfig.BindAddr = b.serfAddr
	conf.MemberlistConfig.BindPort = b.serfPort
	conf.NodeName = id
	conf.Tags["id"] = strconv.Itoa(int(b.id))
	conf.Tags["port"] = strconv.Itoa(b.port)
	conf.Tags["raft_port"] = strconv.Itoa(b.raftPort)
	conf.EventCh = eventCh
	conf.EnableNameConflictResolution = false
	s, err := serf.Create(conf)
	if err != nil {
		return nil, err
	}
	if len(b.peers) > 0 {
		var addrs []string
		for _, p := range b.peers {
			addr := &net.TCPAddr{IP: net.ParseIP(p.IP), Port: p.SerfPort}
			addrs = append(addrs, addr.String())
		}
		s.Join(addrs, true)
	}
	return s, nil
}

func (b *Broker) ID() int32 {
	return b.id
}

func (b *Broker) Host() string {
	return b.host
}

func (b *Broker) Port() int {
	return b.port
}

func (b *Broker) Cluster() []*jocko.BrokerConn {
	cluster := make([]*jocko.BrokerConn, len(b.peers))
	for i, v := range b.peers {
		cluster[i] = v
	}
	return cluster
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
	for _, b := range s.Cluster() {
		if b.ID == id {
			return b
		}
	}
	return nil
}

func (s *Broker) addPartition(partition *jocko.Partition) {
	s.mu.Lock()
	if v, ok := s.topics[partition.Topic]; ok {
		s.topics[partition.Topic] = append(v, partition)
	} else {
		s.topics[partition.Topic] = []*jocko.Partition{partition}
	}
	s.mu.Unlock()
	isLeader := partition.Leader == s.id
	isFollower := false
	for _, r := range partition.Replicas {
		if r == s.id {
			isFollower = true
		}
	}
	if isLeader || isFollower {
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

		partition.Conn = s.peers[partition.LeaderID()]
	}
}

func (s *Broker) addBroker(broker *jocko.BrokerConn) {
	// TODO: remove this
	s.peerLock.Lock()
	s.peers[broker.ID] = broker
	s.peerLock.Unlock()
}

func (s *Broker) IsLeaderOfPartition(topic string, pid int32, lid int32) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	result := false
	for _, p := range s.topics[topic] {
		if p.ID == pid {
			result = lid == p.LeaderID()
			break
		}
	}
	return result
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
func (s *Broker) Join(addrs ...string) (int, error) {
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
	for i := int32(0); i < partitions; i++ {
		partition := &jocko.Partition{
			Topic:           topic,
			ID:              i,
			Leader:          i,
			PreferredLeader: i,
			Replicas:        []int32{i},
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
