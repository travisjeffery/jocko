package raft

import (
	"encoding/json"
	"net"
	"os"
	"path/filepath"
	"time"

	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb"
	"github.com/pkg/errors"
	"github.com/travisjeffery/jocko"
	"github.com/travisjeffery/jocko/log"
)

const (
	timeout = 30 * time.Second
	state   = "raft/"
)

type Event struct {
	Op string
}

// Raft manages consensus on Jocko cluster using Hashicorp Raft
type Raft struct {
	logger    log.Logger
	raft      *raft.Raft
	transport *raft.NetworkTransport
	store     *raftboltdb.BoltStore
	config    *raft.Config

	dataDir             string
	addr                string
	devDisableBootstrap bool

	serf              jocko.Serf
	reconcileInterval time.Duration
	shutdownCh        chan struct{}
}

// New Raft object
func New(opts ...OptionFn) (*Raft, error) {
	r := &Raft{
		config:            raft.DefaultConfig(),
		reconcileInterval: time.Second * 5,
		shutdownCh:        make(chan struct{}),
	}

	for _, o := range opts {
		o(r)
	}

	r.config.LocalID = raft.ServerID(r.addr)
	r.logger = r.logger.With(jocko.String("ctx", "raft"), jocko.String("addr", r.addr))

	return r, nil
}

// Bootstrap is used to bootstrap the raft instance.
// Commands received by raft are sent on commandCh channel.
func (r *Raft) Bootstrap(serf jocko.Serf, serfEventCh <-chan *jocko.ClusterMember, commandCh chan<- jocko.RaftCommand) (err error) {
	r.serf = serf
	r.eventCh = eventCh

	r.transport, err = raft.NewTCPTransport(r.addr, nil, 3, timeout, os.Stderr)
	if err != nil {
		return errors.Wrap(err, "tcp transport failed")
	}

	r.logger = r.logger.With(jocko.Int32("serf id", serf.ID()))

	path := filepath.Join(r.dataDir, state)
	if err = os.MkdirAll(path, 0755); err != nil {
		return errors.Wrap(err, "data directory mkdir failed")
	}

	var peersAddrs []string
	for _, p := range serf.Cluster() {
		addr := &net.TCPAddr{IP: net.ParseIP(p.IP), Port: p.RaftPort}
		peersAddrs = append(peersAddrs, addr.String())
	}

	snapshots, err := raft.NewFileSnapshotStore(path, 2, os.Stderr)
	if err != nil {
		return err
	}

	boltStore, err := raftboltdb.NewBoltStore(filepath.Join(path, "raft.db"))
	if err != nil {
		return errors.Wrap(err, "bolt store failed")
	}
	r.store = boltStore

	notifyCh := make(chan bool, 1)
	r.config.NotifyCh = notifyCh
	r.config.StartAsLeader = !r.devDisableBootstrap

	fsm := &fsm{
		commandCh: commandCh,
		logger:    r.logger,
	}

	raft, err := raft.NewRaft(r.config, fsm, boltStore, boltStore, snapshots, r.transport)
	if err != nil {
		r.store.Close()
		r.transport.Close()
		return errors.Wrap(err, "raft failed")
	}
	r.raft = raft

	go r.monitorLeadership(notifyCh, serfEventCh)

	r.logger.Info("bootstraped raft")

	return nil
}

// Addr of raft node
func (r *Raft) Addr() string {
	return r.addr
}

// Apply command to all raft nodes
func (r *Raft) Apply(cmd jocko.RaftCommand) error {
	c, err := json.Marshal(cmd)
	if err != nil {
		return err
	}
	r.logger.Debug("applying", jocko.Any("cmd", cmd))
	err = r.raft.Apply(c, timeout).Error()
	if err != nil {
		r.logger.Error("applying", jocko.Any("cmd", c), jocko.Error("error", err))
	}
	return err
}

// IsLeader checks if this broker is the cluster controller
func (r *Raft) IsLeader() bool {
	return r.raft.State() == raft.Leader
}

// LeaderID is ID of the controller node
func (r *Raft) LeaderID() string {
	return string(r.raft.Leader())
}

// waitForBarrier to let fsm finish
func (r *Raft) waitForBarrier() error {
	barrier := r.raft.Barrier(0)
	if err := barrier.Error(); err != nil {
		r.logger.Error("failed to wait for barrier", jocko.Error("error", err))
		return err
	}
	return nil
}

// addVoter of given address to raft
func (r *Raft) addVoter(id int32, addr string) error {
	return r.raft.AddVoter(raft.ServerID(id), raft.ServerAddress(addr), 0, 0).Error()
}

func (r *Raft) addNonVoter(id int32, addr string) error {
	panic("not supported")
}

// removeServer of given address from raft
func (r *Raft) removeServer(id int32, addr string) error {
	return r.raft.RemoveServer(raft.ServerID(id), 0, 0).Error()
}

// leave is used to prepare for a graceful shutdown of the server
func (r *Raft) leave() error {
	r.logger.Info("leaving raft peers")

	// TODO: handle case if we're the controller/leader

	return nil
}

// Shutdown raft agent
func (r *Raft) Shutdown() error {
	close(r.shutdownCh)

	if err := r.leave(); err != nil {
		return err
	}

	if err := r.transport.Close(); err != nil {
		return err
	}
	future := r.raft.Shutdown()
	if err := future.Error(); err != nil {
		r.logger.Error("failed to shutdown raft", jocko.Error("error", err))
		return err
	}
	if err := r.store.Close(); err != nil {
		return err
	}
	return nil
}
