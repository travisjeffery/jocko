package jocko

import (
	"fmt"
	"path/filepath"
	"sync"
	"time"

	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb"
	"github.com/hashicorp/serf/serf"
	"github.com/travisjeffery/jocko/jocko/fsm"
	"github.com/travisjeffery/jocko/jocko/metadata"
	"github.com/travisjeffery/jocko/jocko/structs"
	"github.com/travisjeffery/jocko/log"
)

// setupRaft is used to setup and initialize Raft.
func (s *Broker) setupRaft() error {
	// If we have an unclean exit then attempt to close the Raft store.
	defer func() {
		if s.raft == nil && s.raftStore != nil {
			if err := s.raftStore.Close(); err != nil {
				s.logger.Error("failed to close raft store", log.Error("error", err))
			}
		}
	}()

	var err error
	s.fsm, err = fsm.New(s.logger, s.tracer, fsm.NodeID(s.config.ID))
	if err != nil {
		return err
	}

	trans, err := raft.NewTCPTransport(s.config.RaftAddr, nil, 3, 10*time.Second, nil)
	if err != nil {
		return err
	}
	s.raftTransport = trans

	s.config.RaftConfig.LocalID = raft.ServerID(s.config.RaftAddr)
	s.config.RaftConfig.StartAsLeader = s.config.StartAsLeader

	// build an in-memory setup for dev mode, disk-based otherwise.
	var logStore raft.LogStore
	var stable raft.StableStore
	var snap raft.SnapshotStore
	if s.config.DevMode {
		store := raft.NewInmemStore()
		s.raftInmem = store
		stable = store
		logStore = store
		snap = raft.NewInmemSnapshotStore()
	} else {
		path := filepath.Join(s.config.DataDir, raftState)
		if err := ensurePath(path, true); err != nil {
			return err
		}

		// create the backend raft store for logs and stable storage.
		store, err := raftboltdb.NewBoltStore(filepath.Join(path, "raft.db"))
		if err != nil {
			return err
		}
		s.raftStore = store
		stable = store

		cacheStore, err := raft.NewLogCache(raftLogCacheSize, store)
		if err != nil {
			return err
		}
		logStore = cacheStore

		snapshots, err := raft.NewFileSnapshotStore(path, snapshotsRetained, nil)
		if err != nil {
			return err
		}
		snap = snapshots
	}

	if s.config.Bootstrap || s.config.DevMode {
		hasState, err := raft.HasExistingState(logStore, stable, snap)
		if err != nil {
			return err
		}
		s.logger.Debug("setup raft: has existing state", log.Any("has state", hasState))
		if !hasState {
			configuration := raft.Configuration{
				Servers: []raft.Server{
					raft.Server{
						ID:      s.config.RaftConfig.LocalID,
						Address: trans.LocalAddr(),
					},
				},
			}
			if err := raft.BootstrapCluster(s.config.RaftConfig, logStore, stable, snap, trans, configuration); err != nil {
				return err
			}
		}
	}

	// setup up a channel for reliable leader notifications.
	raftNotifyCh := make(chan bool, 1)
	s.config.RaftConfig.NotifyCh = raftNotifyCh
	s.raftNotifyCh = raftNotifyCh

	// setup raft store
	s.raft, err = raft.NewRaft(s.config.RaftConfig, s.fsm, logStore, stable, snap, trans)
	return err
}

func (s *Broker) monitorLeadership() {
	raftNotifyCh := s.raftNotifyCh
	var weAreLeaderCh chan struct{}
	var leaderLoop sync.WaitGroup
	for {
		select {
		case isLeader := <-raftNotifyCh:
			switch {
			case isLeader:
				if weAreLeaderCh != nil {
					s.logger.Error("attempted to start the leader loop while running")
					continue
				}
				weAreLeaderCh = make(chan struct{})
				leaderLoop.Add(1)
				go func(ch chan struct{}) {
					defer leaderLoop.Done()
					s.leaderLoop(ch)
				}(weAreLeaderCh)
				s.logger.Info("cluster leadership acquired")

			default:
				if weAreLeaderCh == nil {
					s.logger.Error("attempted to stop the leader loop while not running")
					continue
				}
				s.logger.Debug("shutting down leader loop")
				close(weAreLeaderCh)
				leaderLoop.Wait()
				weAreLeaderCh = nil
				s.logger.Info("cluster leadership lost")
			}
		case <-s.shutdownCh:
			return
		}
	}
}

func (s *Broker) revokeLeadership() error {
	s.resetConsistentReadReady()
	return nil
}

func (s *Broker) establishLeadership() error {
	s.setConsistentReadReady()
	return nil
}

// leaderLoop runs as long as we are the leader to run various maintenance activities.
func (s *Broker) leaderLoop(stopCh chan struct{}) {
	var reconcileCh chan serf.Member
	establishedLeader := false

RECONCILE:
	reconcileCh = nil
	interval := time.After(60 * time.Second)
	// start := time.Now()
	barrier := s.raft.Barrier(2 * time.Minute)
	if err := barrier.Error(); err != nil {
		s.logger.Error("failed to wait for barrier", log.Error("error", err))
		goto WAIT
	}

	if !establishedLeader {
		if err := s.establishLeadership(); err != nil {
			s.logger.Error("failedto establish leader", log.Error("error", err))
			goto WAIT
		}
		establishedLeader = true
		defer func() {
			if err := s.revokeLeadership(); err != nil {
				s.logger.Error("failed to revoke leadership", log.Error("error", err))
			}
		}()
	}

	if err := s.reconcile(); err != nil {
		s.logger.Error("failed to reconcile", log.Error("error", err))
		goto WAIT
	}

	reconcileCh = s.reconcileCh

WAIT:
	for {
		select {
		case <-stopCh:
			return
		case <-s.shutdownCh:
			return
		case <-interval:
			goto RECONCILE
		case member := <-reconcileCh:
			s.reconcileMember(member)
		}
	}
}

func (s *Broker) reconcile() error {
	members := s.LANMembers()
	for _, member := range members {
		if err := s.reconcileMember(member); err != nil {
			return err
		}
	}
	return nil
}

func (s *Broker) reconcileMember(m serf.Member) error {
	var err error
	switch m.Status {
	case serf.StatusAlive:
		err = s.handleAliveMember(m)
	case serf.StatusFailed:
		err = s.handleFailedMember(m)
	case serf.StatusLeft:
		err = s.handleLeftMember(m)
	}
	if err != nil {
		s.logger.Error("failed to reconcile member", log.Any("member", m), log.Error("error", err))
	}
	return nil
}

func (s *Broker) handleAliveMember(m serf.Member) error {
	b, ok := metadata.IsBroker(m)
	if ok {
		if err := s.joinCluster(m, b); err != nil {
			return err
		}
	}
	state := s.fsm.State()
	_, node, err := state.GetNode(b.ID.String())
	if err != nil {
		return err
	}
	if node != nil {
		// TODO: should still register?
		return nil
	}
	s.logger.Info("member joined, marking health alive", log.Any("member", m))
	req := structs.RegisterNodeRequest{
		Node: structs.Node{
			Node:    fmt.Sprintf("%s", b.ID),
			Address: b.BrokerAddr,
			Meta: map[string]string{
				"raft_addr":     b.RaftAddr,
				"serf_lan_addr": b.SerfLANAddr,
				"name":          b.Name,
			},
			Check: &structs.HealthCheck{
				Node:    b.RaftAddr,
				CheckID: structs.SerfCheckID,
				Name:    structs.SerfCheckName,
				Status:  structs.HealthPassing,
				Output:  structs.SerfCheckAliveOutput,
			},
		},
	}
	_, err = s.raftApply(structs.RegisterNodeRequestType, &req)
	return err
}

func (s *Broker) raftApply(t structs.MessageType, msg interface{}) (interface{}, error) {
	buf, err := structs.Encode(t, msg)
	if err != nil {
		return nil, fmt.Errorf("failed to encode request: %v", err)
	}
	future := s.raft.Apply(buf, 30*time.Second)
	if err := future.Error(); err != nil {
		return nil, err
	}
	return future.Response(), nil
}

func (s *Broker) handleLeftMember(m serf.Member) error {
	return s.handleDeregisterMember("left", m)
}

// handleDeregisterMember is used to deregister a mmeber for a given reason.
func (s *Broker) handleDeregisterMember(reason string, member serf.Member) error {
	if member.Name == s.config.RaftAddr {
		s.logger.Debug("deregistering self should be done by follower")
		return nil
	}

	meta, ok := metadata.IsBroker(member)
	if !ok {
		return nil
	}

	if err := s.removeServer(member, meta); err != nil {
		return err
	}

	state := s.fsm.State()
	_, node, err := state.GetNode(meta.RaftAddr)
	if err != nil {
		return err
	}
	if node == nil {
		return nil
	}

	s.logger.Info("member is deregistering", log.String("node", meta.RaftAddr), log.String("reason", reason))
	req := structs.DeregisterNodeRequest{
		Node: structs.Node{Node: meta.RaftAddr},
	}
	_, err = s.raftApply(structs.DeregisterNodeRequestType, &req)
	return err
}

func (s *Broker) joinCluster(m serf.Member, parts *metadata.Broker) error {
	if parts.Bootstrap {
		members := s.LANMembers()
		for _, member := range members {
			p, ok := metadata.IsBroker(member)
			if ok && member.Name != m.Name && p.Bootstrap {
				s.logger.Error("multiple nodes in bootstrap mode. there can only be one.")
				return nil
			}
		}
	}

	configFuture := s.raft.GetConfiguration()
	if err := configFuture.Error(); err != nil {
		s.logger.Error("failed to get raft configuration", log.Error("error", err))
		return err
	}

	if m.Name == s.config.NodeName {
		if l := len(configFuture.Configuration().Servers); l < 3 {
			s.logger.Debug("skipping self join since cluster is too small", log.String("member name", m.Name))
			return nil
		}
	}

	if parts.NonVoter {
		addFuture := s.raft.AddNonvoter(raft.ServerID(parts.RaftAddr), raft.ServerAddress(parts.RaftAddr), 0, 0)
		if err := addFuture.Error(); err != nil {
			s.logger.Error("failed to add raft peer", log.Error("error", err))
			return err
		}
	} else {
		s.logger.Debug("join cluster: add voter", log.Any("member", parts))
		addFuture := s.raft.AddVoter(raft.ServerID(parts.RaftAddr), raft.ServerAddress(parts.RaftAddr), 0, 0)
		if err := addFuture.Error(); err != nil {
			s.logger.Error("failed to add raft peer", log.Error("error", err))
			return err
		}
	}

	return nil
}

func (s *Broker) handleFailedMember(m serf.Member) error {
	req := structs.RegisterNodeRequest{
		Node: structs.Node{
			Node: m.Tags["raft_addr"],
			Check: &structs.HealthCheck{
				Node:    m.Tags["raft_addr"],
				CheckID: structs.SerfCheckID,
				Name:    structs.SerfCheckName,
				Status:  structs.HealthCritical,
				Output:  structs.SerfCheckFailedOutput,
			},
		},
	}
	_, err := s.raftApply(structs.RegisterNodeRequestType, &req)
	return err
}

func (s *Broker) removeServer(m serf.Member, meta *metadata.Broker) error {
	configFuture := s.raft.GetConfiguration()
	if err := configFuture.Error(); err != nil {
		s.logger.Error("failed to get raft configuration", log.Error("error", err))
		return err
	}
	for _, server := range configFuture.Configuration().Servers {
		s.logger.Info("removing server by id", log.Any("id", server.ID))
		future := s.raft.RemoveServer(raft.ServerID(meta.RaftAddr), 0, 0)
		if err := future.Error(); err != nil {
			s.logger.Error("failed to remove server", log.Error("error", err))
			return err
		}
	}
	return nil
}
