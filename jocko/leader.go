package jocko

import (
	"fmt"
	"math/rand"
	"path/filepath"
	"sync"
	"time"

	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb"
	"github.com/hashicorp/serf/serf"
	jockocli "github.com/travisjeffery/jocko/client"
	"github.com/travisjeffery/jocko/jocko/fsm"
	"github.com/travisjeffery/jocko/jocko/metadata"
	"github.com/travisjeffery/jocko/jocko/structs"
	"github.com/travisjeffery/jocko/log"
	"github.com/travisjeffery/jocko/protocol"
)

const (
	barrierWriteTimeout = 2 * time.Minute
)

// setupRaft is used to setup and initialize Raft.
func (b *Broker) setupRaft() error {
	// If we have an unclean exit then attempt to close the Raft store.
	defer func() {
		if b.raft == nil && b.raftStore != nil {
			if err := b.raftStore.Close(); err != nil {
				b.logger.Error("leader: failed to close raft store", log.Error("error", err))
			}
		}
	}()

	var err error
	b.fsm, err = fsm.New(b.logger, b.tracer, fsm.NodeID(b.config.ID))
	if err != nil {
		return err
	}

	trans, err := raft.NewTCPTransport(b.config.RaftAddr, nil, 3, 10*time.Second, nil)
	if err != nil {
		return err
	}
	b.raftTransport = trans

	b.config.RaftConfig.LocalID = raft.ServerID(b.config.ID)
	b.config.RaftConfig.StartAsLeader = b.config.StartAsLeader

	// build an in-memory setup for dev mode, disk-based otherwise.
	var logStore raft.LogStore
	var stable raft.StableStore
	var snap raft.SnapshotStore
	if b.config.DevMode {
		store := raft.NewInmemStore()
		b.raftInmem = store
		stable = store
		logStore = store
		snap = raft.NewInmemSnapshotStore()
	} else {
		path := filepath.Join(b.config.DataDir, raftState)
		if err := ensurePath(path, true); err != nil {
			return err
		}

		// create the backend raft store for logs and stable storage.
		store, err := raftboltdb.NewBoltStore(filepath.Join(path, "raft.db"))
		if err != nil {
			return err
		}
		b.raftStore = store
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

	if b.config.Bootstrap || b.config.DevMode {
		hasState, err := raft.HasExistingState(logStore, stable, snap)
		if err != nil {
			return err
		}
		b.logger.Debug("leader: setup raft: has existing state", log.Any("has state", hasState))
		if !hasState {
			configuration := raft.Configuration{
				Servers: []raft.Server{
					raft.Server{
						ID:      b.config.RaftConfig.LocalID,
						Address: trans.LocalAddr(),
					},
				},
			}
			if err := raft.BootstrapCluster(b.config.RaftConfig, logStore, stable, snap, trans, configuration); err != nil {
				return err
			}
		}
	}

	// setup up a channel for reliable leader notifications.
	raftNotifyCh := make(chan bool, 1)
	b.config.RaftConfig.NotifyCh = raftNotifyCh
	b.raftNotifyCh = raftNotifyCh

	// setup raft store
	b.raft, err = raft.NewRaft(b.config.RaftConfig, b.fsm, logStore, stable, snap, trans)
	return err
}

func (b *Broker) monitorLeadership() {
	raftNotifyCh := b.raftNotifyCh
	var weAreLeaderCh chan struct{}
	var leaderLoop sync.WaitGroup
	for {
		select {
		case isLeader := <-raftNotifyCh:
			switch {
			case isLeader:
				if weAreLeaderCh != nil {
					b.logger.Error("leader: attempted to start the leader loop while running")
					continue
				}
				weAreLeaderCh = make(chan struct{})
				leaderLoop.Add(1)
				go func(ch chan struct{}) {
					defer leaderLoop.Done()
					b.leaderLoop(ch)
				}(weAreLeaderCh)
				b.logger.Info("leader: cluster leadership acquired")

			default:
				if weAreLeaderCh == nil {
					b.logger.Error("leader: attempted to stop the leader loop while not running")
					continue
				}
				b.logger.Debug("leader: shutting down leader loop")
				close(weAreLeaderCh)
				leaderLoop.Wait()
				weAreLeaderCh = nil
				b.logger.Info("leader: cluster leadership lost")
			}
		case <-b.shutdownCh:
			return
		}
	}
}

func (b *Broker) revokeLeadership() error {
	b.resetConsistentReadReady()
	return nil
}

func (b *Broker) establishLeadership() error {
	b.setConsistentReadReady()
	return nil
}

// leaderLoop runs as long as we are the leader to run various maintenance activities.
func (b *Broker) leaderLoop(stopCh chan struct{}) {
	var reconcileCh chan serf.Member
	establishedLeader := false

RECONCILE:
	reconcileCh = nil
	interval := time.After(b.config.ReconcileInterval)
	barrier := b.raft.Barrier(barrierWriteTimeout)
	if err := barrier.Error(); err != nil {
		b.logger.Error("leader: failed to wait for barrier", log.Error("error", err))
		goto WAIT
	}

	if !establishedLeader {
		if err := b.establishLeadership(); err != nil {
			b.logger.Error("leader: failedto establish leader", log.Error("error", err))
			goto WAIT
		}
		establishedLeader = true
		defer func() {
			if err := b.revokeLeadership(); err != nil {
				b.logger.Error("leader: failed to revoke leadership", log.Error("error", err))
			}
		}()
	}

	if err := b.reconcile(); err != nil {
		b.logger.Error("leader: failed to reconcile", log.Error("error", err))
		goto WAIT
	}

	reconcileCh = b.reconcileCh

WAIT:
	for {
		select {
		case <-stopCh:
			return
		case <-b.shutdownCh:
			return
		case <-interval:
			goto RECONCILE
		case member := <-reconcileCh:
			b.reconcileMember(member)
		}
	}
}

// reconcile is used to reconcile the differences between serf membership and what'b reflected in the strongly consistent store.
func (b *Broker) reconcile() error {
	members := b.LANMembers()
	knownMembers := make(map[int32]struct{})
	for _, member := range members {
		if err := b.reconcileMember(member); err != nil {
			return err
		}
		meta, ok := metadata.IsBroker(member)
		if !ok {
			continue
		}
		knownMembers[meta.ID.Int32()] = struct{}{}
	}
	return b.reconcileReaped(knownMembers)
}

func (b *Broker) reconcileReaped(known map[int32]struct{}) error {
	state := b.fsm.State()
	_, nodes, err := state.GetNodes()
	if err != nil {
		return err
	}
	for _, node := range nodes {
		if _, ok := known[node.ID]; ok {
			continue
		}
		member := serf.Member{
			Tags: map[string]string{
				"id":   fmt.Sprintf("%b", node.ID),
				"role": "jocko",
			},
		}
		if err := b.handleReapMember(member); err != nil {
			return err
		}
	}
	return nil
}

func (b *Broker) reconcileMember(m serf.Member) error {
	var err error
	switch m.Status {
	case serf.StatusAlive:
		err = b.handleAliveMember(m)
	case serf.StatusFailed:
		err = b.handleFailedMember(m)
	case serf.StatusLeft:
		err = b.handleLeftMember(m)
	}
	if err != nil {
		b.logger.Error("leader: failed to reconcile member", log.Any("member", m), log.Error("error", err))
	}
	return nil
}

func (b *Broker) handleAliveMember(m serf.Member) error {
	meta, ok := metadata.IsBroker(m)
	if ok {
		if err := b.joinCluster(m, meta); err != nil {
			return err
		}
	}
	state := b.fsm.State()
	_, node, err := state.GetNode(meta.ID.Int32())
	if err != nil {
		return err
	}
	if node != nil {
		// TODO: should still register?
		return nil
	}
	b.logger.Info("leader: member joined, marking health alive", log.Any("member", m))
	req := structs.RegisterNodeRequest{
		Node: structs.Node{
			Node:    meta.ID.Int32(),
			Address: meta.BrokerAddr,
			Meta: map[string]string{
				"raft_addr":     meta.RaftAddr,
				"serf_lan_addr": meta.SerfLANAddr,
				"name":          meta.Name,
			},
			Check: &structs.HealthCheck{
				Node:    meta.ID.String(),
				CheckID: structs.SerfCheckID,
				Name:    structs.SerfCheckName,
				Status:  structs.HealthPassing,
				Output:  structs.SerfCheckAliveOutput,
			},
		},
	}
	_, err = b.raftApply(structs.RegisterNodeRequestType, &req)
	return err
}

func (b *Broker) raftApply(t structs.MessageType, msg interface{}) (interface{}, error) {
	buf, err := structs.Encode(t, msg)
	if err != nil {
		return nil, fmt.Errorf("failed to encode request: %v", err)
	}
	future := b.raft.Apply(buf, 30*time.Second)
	if err := future.Error(); err != nil {
		return nil, err
	}
	return future.Response(), nil
}

func (b *Broker) handleLeftMember(m serf.Member) error {
	return b.handleDeregisterMember("left", m)
}

func (b *Broker) handleReapMember(member serf.Member) error {
	return b.handleDeregisterMember("reaped", member)
}

// handleDeregisterMember is used to deregister a mmeber for a given reason.
func (b *Broker) handleDeregisterMember(reason string, member serf.Member) error {
	meta, ok := metadata.IsBroker(member)
	if !ok {
		return nil
	}

	if meta.ID.Int32() == b.config.ID {
		b.logger.Debug("leader: deregistering self should be done by follower")
		return nil
	}

	if err := b.removeServer(member, meta); err != nil {
		return err
	}

	state := b.fsm.State()
	_, node, err := state.GetNode(meta.ID.Int32())
	if err != nil {
		return err
	}
	if node == nil {
		return nil
	}

	b.logger.Info("leader: member is deregistering", log.String("node", meta.ID.String()), log.String("reason", reason))
	req := structs.DeregisterNodeRequest{
		Node: structs.Node{Node: meta.ID.Int32()},
	}
	_, err = b.raftApply(structs.DeregisterNodeRequestType, &req)
	return err
}

func (b *Broker) joinCluster(m serf.Member, parts *metadata.Broker) error {
	if parts.Bootstrap {
		members := b.LANMembers()
		for _, member := range members {
			p, ok := metadata.IsBroker(member)
			if ok && member.Name != m.Name && p.Bootstrap {
				b.logger.Error("leader: multiple nodes in bootstrap mode. there can only be one.")
				return nil
			}
		}
	}

	configFuture := b.raft.GetConfiguration()
	if err := configFuture.Error(); err != nil {
		b.logger.Error("leader: failed to get raft configuration", log.Error("error", err))
		return err
	}

	if m.Name == b.config.NodeName {
		if l := len(configFuture.Configuration().Servers); l < 3 {
			b.logger.Debug("leader: skipping self join since cluster is too small", log.String("member name", m.Name))
			return nil
		}
	}

	if parts.NonVoter {
		addFuture := b.raft.AddNonvoter(raft.ServerID(parts.ID), raft.ServerAddress(parts.RaftAddr), 0, 0)
		if err := addFuture.Error(); err != nil {
			b.logger.Error("leader: failed to add raft peer", log.Error("error", err))
			return err
		}
	} else {
		b.logger.Debug("leader: join cluster: add voter", log.Any("member", parts))
		addFuture := b.raft.AddVoter(raft.ServerID(parts.ID), raft.ServerAddress(parts.RaftAddr), 0, 0)
		if err := addFuture.Error(); err != nil {
			b.logger.Error("leader: failed to add raft peer", log.Error("error", err))
			return err
		}
	}

	return nil
}

func (b *Broker) handleFailedMember(m serf.Member) error {
	meta, ok := metadata.IsBroker(m)
	if !ok {
		return nil
	}

	req := structs.RegisterNodeRequest{
		Node: structs.Node{
			Node: meta.ID.Int32(),
			Check: &structs.HealthCheck{
				Node:    m.Tags["raft_addr"],
				CheckID: structs.SerfCheckID,
				Name:    structs.SerfCheckName,
				Status:  structs.HealthCritical,
				Output:  structs.SerfCheckFailedOutput,
			},
		},
	}

	if _, err := b.raftApply(structs.RegisterNodeRequestType, &req); err != nil {
		return err
	}

	// TODO should put all the following some where else. maybe onBrokerChange or handleBrokerChange

	state := b.fsm.State()

	_, partitions, err := state.GetPartitions()
	if err != nil {
		panic(err)
	}

	// need to reassign partitions
	_, partitions, err = state.PartitionsByLeader(meta.ID.Int32())
	if err != nil {
		return err
	}
	_, nodes, err := state.GetNodes()
	if err != nil {
		return err
	}

	// TODO: add an index for this. have same code in broker.go:handleMetadata(...)
	var passing []*structs.Node
	for _, n := range nodes {
		if n.Check.Status == structs.HealthPassing && n.ID != meta.ID.Int32() {
			passing = append(passing, n)
		}
	}

	// reassign consumer group coordinators
	_, groups, err := state.GetGroupsByCoordinator(meta.ID.Int32())
	if err != nil {
		return err
	}
	for _, group := range groups {
		i := rand.Intn(len(passing))
		node := passing[i]
		group.Coordinator = node.Node
		req := structs.RegisterGroupRequest{
			Group: *group,
		}
		if _, err = b.raftApply(structs.RegisterGroupRequestType, req); err != nil {
			return err
		}
	}

	leaderAndISRReq := &protocol.LeaderAndISRRequest{
		ControllerID:    b.config.ID,
		PartitionStates: make([]*protocol.PartitionState, 0, len(partitions)),
		// TODO: LiveLeaders, ControllerEpoch
	}
	for _, p := range partitions {
		i := rand.Intn(len(passing))
		// TODO: check that old leader won't be in this list, will have been deregistered removed from fsm
		node := passing[i]

		// TODO: need to check replication factor

		var ar []int32
		for _, r := range p.AR {
			if r != meta.ID.Int32() {
				ar = append(ar, r)
			}
		}
		var isr []int32
		for _, r := range p.ISR {
			if r != meta.ID.Int32() {
				isr = append(isr, r)
			}
		}

		// TODO: need to update epochs

		req := structs.RegisterPartitionRequest{
			Partition: structs.Partition{
				Topic:     p.Topic,
				ID:        p.Partition,
				Partition: p.Partition,
				Leader:    node.Node,
				AR:        ar,
				ISR:       isr,
			},
		}
		if _, err = b.raftApply(structs.RegisterPartitionRequestType, req); err != nil {
			return err
		}
		// TODO: need to send on leader and isr changes now i think
		leaderAndISRReq.PartitionStates = append(leaderAndISRReq.PartitionStates, &protocol.PartitionState{
			Topic:     p.Topic,
			Partition: p.Partition,
			// TODO: ControllerEpoch, LeaderEpoch, ZKVersion - lol
			Leader:   p.Leader,
			ISR:      p.ISR,
			Replicas: p.AR,
		})
	}

	// TODO: optimize this to send requests to only nodes affected
	for _, n := range passing {
		broker := b.brokerLookup.BrokerByID(raft.ServerID(n.Node))
		if broker == nil {
			// TODO: this probably shouldn't happen -- likely a root issue to fix
			b.logger.Error("trying to assign partitions to unknown broker", log.Any("broker", n))
			continue
		}
		conn, err := jockocli.Dial("tcp", broker.BrokerAddr)
		if err != nil {
			return err
		}
		_, err = conn.LeaderAndISR(leaderAndISRReq)
		if err != nil {
			return err
		}
	}

	return nil
}

func (b *Broker) removeServer(m serf.Member, meta *metadata.Broker) error {
	configFuture := b.raft.GetConfiguration()
	if err := configFuture.Error(); err != nil {
		b.logger.Error("leader: failed to get raft configuration", log.Error("error", err))
		return err
	}
	for _, server := range configFuture.Configuration().Servers {
		if server.ID != raft.ServerID(meta.ID) {
			continue
		}
		b.logger.Info("leader: removing server by id", log.Any("server id", server.ID))
		future := b.raft.RemoveServer(raft.ServerID(meta.ID), 0, 0)
		if err := future.Error(); err != nil {
			b.logger.Error("leader: failed to remove server", log.Error("error", err))
			return err
		}
	}
	return nil
}
