package raft

import (
	"net"
	"time"

	"github.com/travisjeffery/jocko"
)

// monitorLeadership is used to monitor if we acquire or lose our role as the
// leader in the Raft cluster.
func (b *Raft) monitorLeadership(notifyCh <-chan bool, serfEventCh <-chan *jocko.ClusterMember) {
	var stopCh chan struct{}
	for {
		select {
		case isLeader := <-notifyCh:
			if isLeader {
				stopCh = make(chan struct{})
				go b.leaderLoop(stopCh, serfEventCh)
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
func (b *Raft) revokeLeadership() error {
	return nil
}

// leaderLoop is ran when this raft instance is the leader of the cluster and is used to
// perform cluster leadership duties.
func (b *Raft) leaderLoop(stopCh chan struct{}, serfEventCh <-chan *jocko.ClusterMember) {
	defer b.revokeLeadership()
	var reconcileCh <-chan *jocko.ClusterMember
	establishedLeader := false

RECONCILE:
	reconcileCh = nil
	interval := time.After(b.reconcileInterval)

	if err := b.waitForBarrier(); err != nil {
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

	reconcileCh = serfEventCh

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
			if b.IsLeader() {
				b.reconcileMember(member)
			}
		}
	}
}

func (b *Raft) establishLeadership(stopCh chan struct{}) error {
	// start monitoring other brokers
	// b.periodicDispatcher.SetEnabled(true)
	// b.periodicDispatcher.Start()
	return nil
}

func (b *Raft) reconcile() error {
	members := b.serf.Cluster()
	for _, member := range members {
		if err := b.reconcileMember(member); err != nil {
			return err
		}
	}
	return nil
}

func (b *Raft) reconcileMember(member *jocko.ClusterMember) error {
	// don't reconcile ourself
	if member.ID == b.serf.ID() {
		return nil
	}
	var err error
	switch member.Status {
	case jocko.StatusAlive:
		addr := &net.TCPAddr{IP: net.ParseIP(member.IP), Port: member.RaftPort}
		err = b.addPeer(addr.String())
	case jocko.StatusLeft, jocko.StatusReap:
		err = b.removePeer(member.IP)
	}

	if err != nil {
		b.logger.Info("failed to reconcile member: %v: %v", member, err)
		return err
	}
	return nil
}
