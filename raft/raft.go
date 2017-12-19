package raft

import (
	"encoding/json"
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
	timeout           = 10 * time.Second
	state             = "raft/"
	raftLogCacheSize  = 512
	snapshotsRetained = 2
)

type Config struct {
	Addr              string
	DataDir           string
	DevMode           bool
	Bootstrap         bool
	BootstrapExpect   int
	ReconcileInterval time.Duration
}

// Raft manages consensus on Jocko cluster using Hashicorp Raft
type Raft struct {
	logger     log.Logger
	config     Config
	raftConfig *raft.Config
	raft       *raft.Raft
	transport  *raft.NetworkTransport
	log        raft.LogStore
	stable     raft.StableStore
	snap       raft.SnapshotStore
	serf       jocko.Serf
	shutdownCh chan struct{}
}

// New Raft object
func New(config Config, serf jocko.Serf, logger log.Logger) (*Raft, error) {
	r := &Raft{
		config:     config,
		serf:       serf,
		raftConfig: raft.DefaultConfig(),
		shutdownCh: make(chan struct{}),
		logger:     logger,
	}
	if err := r.setupRaft(); err != nil {
		return nil, err
	}
	return r, nil
}

func (r *Raft) setupRaft() error {
	var err error

	r.raftConfig.LocalID = raft.ServerID(r.config.Addr)

	r.transport, err = raft.NewTCPTransport(r.config.Addr, nil, 3, timeout, os.Stderr)
	if err != nil {
		return errors.Wrap(err, "tcp transport failed")
	}

	path := filepath.Join(r.config.DataDir, state)
	if err = os.MkdirAll(path, 0755); err != nil {
		return errors.Wrap(err, "data directory mkdir failed")
	}

	// var peersAddrs []string
	// for _, p := range serf.Cluster() {
	// 	addr := &net.TCPAddr{IP: net.ParseIP(p.IP), Port: p.RaftPort}
	// 	peersAddrs = append(peersAddrs, addr.String())
	// }

	r.snap, err = raft.NewFileSnapshotStore(path, 2, os.Stderr)
	if err != nil {
		return err
	}

	if r.config.DevMode {
		store := raft.NewInmemStore()
		r.log = store
		r.stable = store
		r.snap = raft.NewInmemSnapshotStore()
	} else {
		store, err := raftboltdb.NewBoltStore(filepath.Join(path, "raft.db"))
		if err != nil {
			return errors.Wrap(err, "bolt store failed")
		}
		r.stable = store
		// Wrap the store in a LogCache to improve performance.
		cacheStore, err := raft.NewLogCache(raftLogCacheSize, store)
		if err != nil {
			return err
		}
		r.log = cacheStore
		r.snap, err = raft.NewFileSnapshotStore(path, snapshotsRetained, nil)
		if err != nil {
			return err
		}
	}

	notifyCh := make(chan bool, 1)
	r.raftConfig.NotifyCh = notifyCh
	r.raftConfig.StartAsLeader = !r.config.Bootstrap

	fsm := &fsm{
		commandCh: nil, // TODO: set this
		logger:    r.logger,
	}

	if r.config.Bootstrap || r.config.DevMode {
		hasState, err := raft.HasExistingState(r.log, r.stable, r.snap)
		if err != nil {
			return err
		}
		if !hasState {
			configuration := raft.Configuration{
				Servers: []raft.Server{{
					ID:      r.raftConfig.LocalID,
					Address: r.transport.LocalAddr(),
				}},
			}
			if err := raft.BootstrapCluster(r.raftConfig, r.log, r.stable, r.snap, r.transport, configuration); err != nil {
				return err
			}
		}
	}

	raft, err := raft.NewRaft(r.raftConfig, fsm, r.log, r.stable, r.snap, r.transport)
	if err != nil {
		return errors.Wrap(err, "raft failed")
	}
	r.raft = raft

	go r.monitorLeadership(notifyCh, nil) // TODO: need serf event ch

	return nil
}

// Addr of raft node
func (b *Raft) Addr() string {
	return b.config.Addr
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
	return string(b.raft.Leader())
}

// waitForBarrier to let fsm finish
func (b *Raft) waitForBarrier() error {
	barrier := b.raft.Barrier(0)
	if err := barrier.Error(); err != nil {
		b.logger.Error("failed to wait for barrier", log.Error("error", err))
		return err
	}
	return nil
}

// addPeer of given address to raft
func (b *Raft) addPeer(id int32, addr string, voter bool) error {
	if !voter {
		panic("non voter not supported yet")
	}
	return b.raft.AddVoter(raft.ServerID(id), raft.ServerAddress(addr), 0, 0).Error()
}

// removePeer of given address from raft
func (b *Raft) removePeer(id int32, addr string) error {
	return b.raft.RemoveServer(raft.ServerID(id), 0, 0).Error()
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
		b.logger.Error("failed to shutdown raft", log.Error("error", err))
		return err
	}
	// if err := b.stable.Close(); err != nil {
	// 	return err
	// }
	return nil
}
