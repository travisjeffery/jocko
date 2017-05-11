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
	"github.com/travisjeffery/simplelog"
)

const (
	timeout = 10 * time.Second
	state   = "raft/"
)

// Raft manages consensus on Jocko cluster using Hashicorp Raft
type Raft struct {
	logger    *simplelog.Logger
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

	return r, nil
}

// Bootstrap the Raft agent using fsm and connect to peers
// Commands received by raft are returned on commandCh channel
func (b *Raft) Bootstrap(serf jocko.Serf, serfEventCh <-chan *jocko.ClusterMember, commandCh chan<- jocko.RaftCommand) (err error) {
	b.serf = serf
	b.transport, err = raft.NewTCPTransport(b.addr, nil, 3, timeout, os.Stderr)
	if err != nil {
		return errors.Wrap(err, "tcp transport failed")
	}

	path := filepath.Join(b.dataDir, state)
	if err = os.MkdirAll(path, 0755); err != nil {
		return errors.Wrap(err, "data directory mkdir failed")
	}

	var peersAddrs []string
	for _, p := range serf.Cluster() {
		addr := &net.TCPAddr{IP: net.ParseIP(p.IP), Port: p.RaftPort}
		peersAddrs = append(peersAddrs, addr.String())
	}
	raftPeers := raft.NewJSONPeers(path, b.transport)
	if err = raftPeers.SetPeers(peersAddrs); err != nil {
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
	b.store = boltStore

	notifyCh := make(chan bool, 1)
	b.config.NotifyCh = notifyCh
	b.config.StartAsLeader = !b.devDisableBootstrap

	fsm := &fsm{
		commandCh: commandCh,
		logger:    b.logger,
	}

	raft, err := raft.NewRaft(b.config, fsm, boltStore, boltStore, snapshots, raftPeers, b.transport)
	if err != nil {
		b.store.Close()
		b.transport.Close()
		return errors.Wrap(err, "raft failed")
	}
	b.raft = raft

	// monitor leadership changes
	go b.monitorLeadership(notifyCh, serfEventCh)

	return nil
}

// Addr of raft node
func (b *Raft) Addr() string {
	return b.addr
}

// Apply command to all raft nodes
func (b *Raft) Apply(cmd jocko.RaftCommand) error {
	c, err := json.Marshal(cmd)
	if err != nil {
		return err
	}
	f := b.raft.Apply(c, timeout)
	return f.Error()
}

// IsLeader checks if this broker is the cluster controller
func (b *Raft) IsLeader() bool {
	return b.raft.State() == raft.Leader
}

// LeaderID is ID of the controller node
func (b *Raft) LeaderID() string {
	return b.raft.Leader()
}

// waitForBarrier to let fsm finish
func (b *Raft) waitForBarrier() error {
	barrier := b.raft.Barrier(0)
	if err := barrier.Error(); err != nil {
		b.logger.Info("failed to wait for barrier: %v", err)
		return err
	}
	return nil
}

// addPeer of given address to raft
func (b *Raft) addPeer(addr string) error {
	future := b.raft.AddPeer(addr)
	if err := future.Error(); err != nil && err != raft.ErrKnownPeer {
		b.logger.Info("failed to add raft peer: %v", err)
		return err
	} else if err == nil {
		b.logger.Info("added raft peer: %v", addr)
	}
	return nil
}

// removePeer of given address from raft
func (b *Raft) removePeer(addr string) error {
	future := b.raft.RemovePeer(addr)
	if err := future.Error(); err != nil && err != raft.ErrUnknownPeer {
		b.logger.Info("failed to remove raft peer: %v", err)
		return err
	} else if err == nil {
		b.logger.Info("removed raft peer: %v", addr)
	}
	return nil
}

// leave is used to prepare for a graceful shutdown of the server
func (b *Raft) leave() error {
	b.logger.Info("preparing to leave raft peers")

	// TODO: handle case if we're the controller/leader

	return nil
}

// Shutdown raft agent
func (b *Raft) Shutdown() error {
	close(b.shutdownCh)

	if err := b.leave(); err != nil {
		return err
	}

	if err := b.transport.Close(); err != nil {
		return err
	}
	future := b.raft.Shutdown()
	if err := future.Error(); err != nil {
		b.logger.Info("failed to shutdown raft: %s", err)
		return err
	}
	if err := b.store.Close(); err != nil {
		return err
	}
	return nil
}
