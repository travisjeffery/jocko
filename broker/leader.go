package broker

import (
	"fmt"
	"net"
	"time"

	"github.com/hashicorp/raft"
	"github.com/hashicorp/serf/serf"
)

// monitorLeadership is used to monitor if we acquire or lose our role as the
// leader in the Raft cluster.
func (b *Broker) monitorLeadership() {
	var stopCh chan struct{}
	for {
		select {
		case isLeader := <-b.raftLeaderCh:
			if isLeader {
				stopCh = make(chan struct{})
				go b.leaderLoop(stopCh)
				b.logger.Info("cluster leadership acquired")
			} else if stopCh != nil {
				close(stopCh)
				stopCh = nil
				b.logger.Info("cluster leadership lost")
			}
		case <-b.shutdownCh:
			return
		}
	}
}

// revokeLeadership is invoked once we step down as leader.
// This is used to cleanup any state that may be specific to the leader.
func (b *Broker) revokeLeadership() error {
	return nil
}

// leaderLoop runs as long as we are the leader to run maintainence duties
func (b *Broker) leaderLoop(stopCh chan struct{}) {
	defer b.revokeLeadership()
	var reconcileCh chan serf.Member
	establishedLeader := false

RECONCILE:
	reconcileCh = nil
	interval := time.After(b.serfReconcileInterval)

	// start := time.Now()
	barrier := b.raft.Barrier(0)
	if err := barrier.Error(); err != nil {
		b.logger.Info("failed to waat for barrier: %v", err)
		goto WAIT
	}

	if !establishedLeader {
		if err := b.establishLeadership(stopCh); err != nil {
			b.logger.Info("failed to establish leadership: %v", err)
			goto WAIT
		}
		establishedLeader = true
	}

	if err := b.reconcile(); err != nil {
		b.logger.Info("failed to reconcile: %v", err)
		goto WAIT
	}

	reconcileCh = b.serfReconcileCh

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

func (b *Broker) establishLeadership(stopCh chan struct{}) error {
	// start monitoring other brokers
	// b.periodicDispatcher.SetEnabled(true)
	// b.periodicDispatcher.Start()
	return nil
}

func (b *Broker) reconcile() error {
	members := b.serf.Members()
	for _, member := range members {
		if err := b.reconcileMember(member); err != nil {
			return err
		}
	}
	return nil
}

func (b *Broker) reconcileMember(member serf.Member) error {
	// don't reconcile ourself
	if member.Name == fmt.Sprintf("%d", b.id) {
		return nil
	}
	var err error
	switch member.Status {
	case serf.StatusAlive:
		err = b.addRaftPeer(member)
	case serf.StatusLeft, serf.MemberStatus(-1):
		err = b.removeRaftPeer(member)
	}
	if err != nil {
		b.logger.Info("failed to reconcile member: %v: %v", member, err)
		return err
	}
	return nil
}

func (b *Broker) addRaftPeer(member serf.Member) error {
	broker, err := brokerConn(member)
	if err != nil {
		return err
	}
	addr := &net.TCPAddr{IP: net.ParseIP(broker.IP), Port: broker.RaftPort}
	future := b.raft.AddPeer(addr.String())
	if err := future.Error(); err != nil && err != raft.ErrKnownPeer {
		b.logger.Info("failed to add raft peer: %v", err)
		return err
	} else if err == nil {
		b.logger.Info("added raft peer: %v", member)
	}
	return nil
}

func (b *Broker) removeRaftPeer(member serf.Member) error {
	future := b.raft.RemovePeer(member.Addr.String())
	if err := future.Error(); err != nil && err != raft.ErrUnknownPeer {
		b.logger.Info("failed to remove raft peer: %v", err)
		return err
	} else if err == nil {
		b.logger.Info("removed raft peer: %v", member)
	}
	return nil
}
