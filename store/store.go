package store

import (
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"sync"
	"time"

	msgpack "gopkg.in/vmihailenco/msgpack.v2"

	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb"
	"github.com/pkg/errors"
)

const (
	timeout = 10 * time.Second
)

type command struct {
	Op    []byte `msgpack:"op"`
	Key   []byte `msgpack:"key"`
	Value []byte `msgpack:"value"`
}

type Store struct {
	dataDir  string
	bindAddr string

	mu sync.Mutex

	peerStore raft.PeerStore
	raft      *raft.Raft
	store     *raftboltdb.BoltStore
}

func New(dataDir, bindAddr string) *Store {
	return &Store{
		dataDir:  dataDir,
		bindAddr: bindAddr,
	}
}

func (s *Store) Open() error {
	conf := raft.DefaultConfig()

	conf.EnableSingleNode = true

	addr, err := net.ResolveTCPAddr("tcp", s.bindAddr)
	if err != nil {
		return errors.Wrap(err, "resolve bind addr failed")
	}

	transport, err := raft.NewTCPTransport(s.bindAddr, addr, 3, timeout, os.Stderr)
	if err != nil {
		return errors.Wrap(err, "tcp transport failede")
	}

	s.peerStore = raft.NewJSONPeers(s.dataDir, transport)

	snapshots, err := raft.NewFileSnapshotStore(s.dataDir, 2, os.Stderr)
	if err != nil {
		return fmt.Errorf("file snapshot store: %s", err)
	}

	boltStore, err := raftboltdb.NewBoltStore(filepath.Join(s.dataDir, "store.db"))
	if err != nil {
		return errors.Wrap(err, "bolt store failed")
	}
	s.store = boltStore

	raft, err := raft.NewRaft(conf, s, boltStore, boltStore, snapshots, s.peerStore, transport)
	if err != nil {
		return errors.Wrap(err, "raft failed")
	}

	s.raft = raft

	return nil
}

func (s *Store) Get(key []byte) ([]byte, error) {
	// add cache
	return s.store.Get(key)
}

func (s *Store) Set(key, value []byte) error {
	c := &command{
		Op:    []byte("set"),
		Key:   key,
		Value: value,
	}
	b, err := msgpack.Marshal(c)
	if err != nil {
		return errors.Wrap(err, "msgpack failed")
	}
	f := s.raft.Apply(b, timeout)
	return f.Error()
}

func (s *Store) Delete(key []byte) error {
	return nil
}

func (s *Store) Join(addr []byte) error {
	f := s.raft.AddPeer(string(addr))
	return f.Error()
}

func (s *Store) Apply(l *raft.Log) interface{} {
	return nil
}

func (s *Store) applySet(k, v []byte) interface{} {
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
