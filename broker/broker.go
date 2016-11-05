package broker

import (
	"encoding/json"
	"io"
	"net"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb"
	"github.com/pkg/errors"
	"github.com/travisjeffery/jocko/cluster"
)

const (
	timeout   = 10 * time.Second
	waitDelay = 100 * time.Millisecond
)

const (
	addPartition CmdType = iota
)

type CmdType int

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

type Options struct {
	DataDir  string
	BindAddr string
	LogDir   string

	numPartitions int
	transport     raft.Transport
}

type Broker struct {
	Options

	mu sync.Mutex

	partitions []*cluster.TopicPartition
	topics     map[string][]*cluster.TopicPartition

	peerStore raft.PeerStore
	transport raft.Transport

	raft  *raft.Raft
	store *raftboltdb.BoltStore
}

func New(options Options) *Broker {
	return &Broker{
		topics:  make(map[string][]*cluster.TopicPartition),
		Options: options,
	}
}

func (s *Broker) Open() error {
	conf := raft.DefaultConfig()

	conf.EnableSingleNode = true

	addr, err := net.ResolveTCPAddr("tcp", s.BindAddr)
	if err != nil {
		return errors.Wrap(err, "resolve bind addr failed")
	}

	if s.transport == nil {
		s.transport, err = raft.NewTCPTransport(s.BindAddr, addr, 3, timeout, os.Stderr)
		if err != nil {
			return errors.Wrap(err, "tcp transport failede")
		}
	}

	s.peerStore = raft.NewJSONPeers(s.DataDir, s.transport)

	snapshots, err := raft.NewFileSnapshotStore(s.DataDir, 2, os.Stderr)
	if err != nil {
	}

	boltStore, err := raftboltdb.NewBoltStore(filepath.Join(s.DataDir, "store.db"))
	if err != nil {
		return errors.Wrap(err, "bolt store failed")
	}
	s.store = boltStore

	raft, err := raft.NewRaft(conf, s, boltStore, boltStore, snapshots, s.peerStore, s.transport)
	if err != nil {
		return errors.Wrap(err, "raft failed")
	}
	s.raft = raft

	return nil
}

func (s *Broker) Close() error {
	return s.raft.Shutdown().Error()
}

func (s *Broker) IsController() bool {
	return s.raft.State() == raft.Leader
}

func (s *Broker) ControllerID() string {
	return s.raft.Leader()
}

func (s *Broker) BrokerID() string {
	return s.transport.LocalAddr()
}

func (s *Broker) Brokers() ([]string, error) {
	return s.peerStore.Peers()
}

func (s *Broker) Partitions() ([]*cluster.TopicPartition, error) {
	return s.partitions, nil
}

func (s *Broker) PartitionsForTopic(topic string) (found []*cluster.TopicPartition, err error) {
	return s.topics[topic], nil
}

func (s *Broker) Partition(topic string, partition int32) (*cluster.TopicPartition, error) {
	found, err := s.PartitionsForTopic(topic)
	if err != nil {
		return nil, err
	}
	for _, f := range found {
		if f.Partition == partition {
			return f, nil
		}
	}
	return nil, errors.New("partition not found")
}

func (s *Broker) NumPartitions() (int, error) {
	// TODO: need to get to get from store
	if s.numPartitions == 0 {
		return 4, nil
	} else {
		return s.numPartitions, nil
	}

}

func (s *Broker) AddPartition(partition cluster.TopicPartition) error {
	return s.apply(addPartition, partition)
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

func (s *Broker) addPartition(partition *cluster.TopicPartition) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.partitions = append(s.partitions, partition)
	if v, ok := s.topics[partition.Topic]; ok {
		s.topics[partition.Topic] = append(v, partition)
	} else {
		s.topics[partition.Topic] = []*cluster.TopicPartition{partition}
	}
	if s.IsLeaderOfPartition(partition) {
		if err := partition.OpenCommitLog(s.LogDir); err != nil {
			panic(err)
		}
	}
}

func (s *Broker) IsLeaderOfPartition(partition *cluster.TopicPartition) bool {
	// TODO: switch this to a map for perf
	for _, p := range s.topics[partition.Topic] {
		if p.Partition == partition.Partition {
			if partition.Leader == s.BrokerID() {
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

func (s *Broker) Join(addr string) error {
	f := s.raft.AddPeer(addr)
	return f.Error()
}

func (s *Broker) Apply(l *raft.Log) interface{} {
	var c command
	if err := json.Unmarshal(l.Data, &c); err != nil {
		panic(errors.Wrap(err, "json unmarshal failed"))
	}
	switch c.Cmd {
	case addPartition:
		p := new(cluster.TopicPartition)
		b, err := c.Data.MarshalJSON()
		if err != nil {
			panic(errors.Wrap(err, "json unmarshal failed"))
		}
		if err := json.Unmarshal(b, p); err != nil {
			panic(errors.Wrap(err, "json unmarshal failed"))
		}
		s.addPartition(p)
	}

	return nil
}

// CreateTopic creates topic with partitions count.
func (s *Broker) CreateTopic(topic string, partitions int) error {
	for _, t := range s.Topics() {
		if t == topic {
			return errors.New("topic exists already")
		}
	}
	numPartitions, err := s.NumPartitions()
	if err != nil {
		return err
	}
	if partitions != 0 {
		numPartitions = partitions
	}
	brokers, err := s.Brokers()
	if err != nil {
		return err
	}
	for i := 0; i < numPartitions; i++ {
		broker := brokers[i%len(brokers)]
		partition := cluster.TopicPartition{
			Partition:       int32(i),
			Topic:           topic,
			Leader:          broker,
			PreferredLeader: broker,
			Replicas:        []string{broker},
		}
		if err := s.AddPartition(partition); err != nil {
			return err
		}
	}
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
