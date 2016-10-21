package store

import (
	"encoding/json"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb"
	"github.com/pkg/errors"
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

type TopicPartition struct {
	Topic     string `json:"topic"`
	Partition int    `json:"partition"`

	// broker ids
	Replicas        []string `json:"replicas"`
	Leader          string   `json:"leader"`
	PreferredLeader string   `json:"preferred_leader"`
}

type Options struct {
	dataDir       string
	bindAddr      string
	numPartitions int
	transport     raft.Transport
}

type Store struct {
	Options

	mu sync.Mutex

	partitions []*TopicPartition
	topics     []string

	peerStore raft.PeerStore
	transport raft.Transport
	raft      *raft.Raft
	store     *raftboltdb.BoltStore
}

func New(options Options) *Store {
	return &Store{
		Options: options,
	}
}

func (s *Store) Open() error {
	conf := raft.DefaultConfig()

	conf.EnableSingleNode = true

	addr, err := net.ResolveTCPAddr("tcp", s.bindAddr)
	if err != nil {
		return errors.Wrap(err, "resolve bind addr failed")
	}

	if s.transport == nil {
		s.transport, err = raft.NewTCPTransport(s.bindAddr, addr, 3, timeout, os.Stderr)
		if err != nil {
			return errors.Wrap(err, "tcp transport failede")
		}
	}

	s.peerStore = raft.NewJSONPeers(s.dataDir, s.transport)

	snapshots, err := raft.NewFileSnapshotStore(s.dataDir, 2, os.Stderr)
	if err != nil {
		return fmt.Errorf("file snapshot store: %s", err)
	}

	boltStore, err := raftboltdb.NewBoltStore(filepath.Join(s.dataDir, "store.db"))
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

func (s *Store) Close() error {
	return s.raft.Shutdown().Error()
}

func (s *Store) IsController() bool {
	return s.raft.State() == raft.Leader
}

func (s *Store) ControllerID() string {
	return s.raft.Leader()
}

func (s *Store) BrokerID() string {
	return s.transport.LocalAddr()
}

func (s *Store) Brokers() ([]string, error) {
	return s.peerStore.Peers()
}

func (s *Store) Partitions() ([]*TopicPartition, error) {
	return s.partitions, nil
}

func (s *Store) NumPartitions() (int, error) {
	// TODO: need to get to get from store
	return s.numPartitions, nil
}

func (s *Store) AddPartition(partition TopicPartition) error {
	return s.apply(addPartition, partition)
}

func (s *Store) apply(cmdType CmdType, data interface{}) error {
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

func (s *Store) addPartition(partition TopicPartition) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.partitions = append(s.partitions, &partition)
}

func (s *Store) IsLeaderOfPartition(partition TopicPartition) bool {
	// TODO: switch this to a map for perf
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, p := range s.partitions {
		if p.Topic == partition.Topic && p.Partition == partition.Partition {
			if partition.Leader == s.BrokerID() {
				return true
			}
			break
		}
	}
	return false
}

func (s *Store) Topics() []string {
	return s.topics
}

func (s *Store) Join(addr string) error {
	f := s.raft.AddPeer(addr)
	return f.Error()
}

func (s *Store) Apply(l *raft.Log) interface{} {
	var c command
	if err := json.Unmarshal(l.Data, &c); err != nil {
		panic(errors.Wrap(err, "json unmarshal failed"))
	}

	switch c.Cmd {
	case addPartition:
		var p TopicPartition
		b, err := c.Data.MarshalJSON()
		if err != nil {
			panic(errors.Wrap(err, "json unmarshal failed"))
		}
		if err := json.Unmarshal(b, &p); err != nil {
			panic(errors.Wrap(err, "json unmarshal failed"))
		}
		s.addPartition(p)
	}

	return nil
}

func (s *Store) Restore(rc io.ReadCloser) error {
	return nil
}

type FSMSnapshot struct {
}

func (f *FSMSnapshot) Persist(sink raft.SnapshotSink) error {
	return nil
}

func (f *FSMSnapshot) Release() {}

func (s *Store) Snapshot() (raft.FSMSnapshot, error) {
	return &FSMSnapshot{}, nil
}

func (s *Store) WaitForLeader(timeout time.Duration) (string, error) {
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
			return "", fmt.Errorf("timeout expired")
		}
	}
}

func (s *Store) WaitForAppliedIndex(idx uint64, timeout time.Duration) error {
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
			return fmt.Errorf("timeout expired")
		}
	}
}
