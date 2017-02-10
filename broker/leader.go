package broker

import (
	"net"
	"time"

	"github.com/hashicorp/serf/serf"
	"github.com/travisjeffery/jocko/jocko"
)

// monitorLeadership is used to monitor if we acquire or lose our role as the
// leader in the Raft cluster.
func (b *Broker) monitorLeadership() {
	var stopCh chan struct{}
	for {
		select {
		case isLeader := <-b.leaderCh:
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
	var reconcileCh chan *jocko.BrokerConn
	establishedLeader := false

RECONCILE:
	reconcileCh = nil
	interval := time.After(b.reconcileInterval)

	if err := b.raft.WaitForBarrier(); err != nil {
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
			if b.IsController() {
				b.reconcileMember(member)
			}
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
	members := b.Cluster()
	for _, member := range members {
		if err := b.reconcileMember(member); err != nil {
			return err
		}
	}
	return nil
}

func (b *Broker) reconcileMember(member *jocko.BrokerConn) error {
	// don't reconcile ourself
	if member.ID == b.id {
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

func (b *Broker) addRaftPeer(member *jocko.BrokerConn) error {
	addr := &net.TCPAddr{IP: net.ParseIP(member.IP), Port: member.RaftPort}
	return b.raft.AddPeer(addr.String())
}

func (b *Broker) removeRaftPeer(member *jocko.BrokerConn) error {
	return b.raft.RemovePeer(member.IP)
}
