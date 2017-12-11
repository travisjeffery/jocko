package raft

import (
	"net"
	"time"

	"github.com/travisjeffery/jocko"
	"github.com/travisjeffery/jocko/log"
)

// monitorLeadership is used to monitor if we acquire or lose our role as the
// leader in the Raft cluster.
func (r *Raft) monitorLeadership(notifyCh <-chan bool, serfEventCh <-chan *jocko.ClusterMember) {
	var stopCh chan struct{}
	for {
		select {
		case isLeader := <-notifyCh:
			if isLeader {
				stopCh = make(chan struct{})
				go r.leaderLoop(stopCh, serfEventCh)
				r.logger.Info("cluster leadership acquired")
				// r.eventCh <- jocko.RaftEvent{Op: "acquired-leadership"}
			} else if stopCh != nil {
				close(stopCh)
				stopCh = nil
				r.logger.Info("cluster leadership lost")
				// r.eventCh <- jocko.RaftEvent{Op: "lost-leadership"}
			}
		case <-r.shutdownCh:
			return
		}
	}
}

// revokeLeadership is invoked once we step down as leader.
// This is used to cleanup any state that may be specific to the leader.
func (r *Raft) revokeLeadership() error {
	return nil
}

// leaderLoop is ran when this raft instance is the leader of the cluster and is used to
// perform cluster leadership duties.
func (r *Raft) leaderLoop(stopCh chan struct{}, serfEventCh <-chan *jocko.ClusterMember) {
	defer r.revokeLeadership()
	var reconcileCh <-chan *jocko.ClusterMember
	establishedLeader := false

RECONCILE:
	reconcileCh = nil
	interval := time.After(r.reconcileInterval)

	if err := r.waitForBarrier(); err != nil {
		goto WAIT
	}

	if !establishedLeader {
		if err := r.establishLeadership(); err != nil {
			r.logger.Error("failed to establish leadership", log.Error("error", err))
			goto WAIT
		}
		establishedLeader = true
	}

	if err := r.reconcile(); err != nil {
		r.logger.Error("failed to reconcile", log.Error("err", err))
		goto WAIT
	}

	reconcileCh = serfEventCh

WAIT:
	for {
		select {
		case <-stopCh:
			return
		case <-r.shutdownCh:
			return
		case <-interval:
			goto RECONCILE
		case member := <-reconcileCh:
			if r.IsLeader() {
				r.reconcileMember(member)
			}
		}
	}
}

// establishLeadership is invoked once this raft becomes the leader.
// TODO: We need to invoke an initial barrier used to ensure any
// previously inflight transactions have been committed and that our
// state is up-to-date.
func (r *Raft) establishLeadership() error {
	r.logger.Debug("establish leadership")
	// start monitoring other brokers
	return nil
}

// reconcile is used to reconcile the differences between Serf membership and
// what is reflected in our strongly consistent store. Mainly we need to ensure
// all live nodes are registered, all failed nodes are marked as such, and all
// left nodes are de-registered.
func (r *Raft) reconcile() error {
	members := r.serf.Cluster()
	for _, member := range members {
		if err := r.reconcileMember(member); err != nil {
			return err
		}
	}
	return nil
}

// reconcileMember is used to do an async reconcile of a single
// serf member
func (r *Raft) reconcileMember(member *jocko.ClusterMember) error {
	r.logger.Debug("reconcile member", log.Int32("member id", member.ID), log.String("member ip", member.IP))
	// don't reconcile ourself
	if member.ID == r.serf.ID() {
		return nil
	}
	var err error
	switch member.Status {
	case jocko.StatusAlive:
		addr := &net.TCPAddr{IP: net.ParseIP(member.IP), Port: member.RaftPort}
		r.logger.Debug("adding voter", log.Int32("member id", member.ID), log.String("member ip", member.IP))
		err = r.addVoter(member.ID, addr.String())
	case jocko.StatusLeft, jocko.StatusReap:
		r.logger.Debug("removing server", log.Int32("member id", member.ID), log.String("member ip", member.IP))
		err = r.removeServer(member.ID, member.IP)
	}

	if err != nil {
		r.logger.Error("failed to reconcile member", log.Int32("member id", member.ID), log.Error("error", err))
		return err
	}
	return nil
}
