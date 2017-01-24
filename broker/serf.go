package broker

import (
	"fmt"
	"net"
	"strconv"

	"github.com/hashicorp/serf/serf"
)

const (
	// StatusReap is used to update the status of a node if we
	// are handling a EventMemberReap
	StatusReap = serf.MemberStatus(-1)
)

// setupSerf is used to configure and create the serf instance
func (b *Broker) setupSerf(conf *serf.Config, eventCh chan serf.Event, serfSnapshot string) (*serf.Serf, error) {
	conf.Init()
	id := fmt.Sprintf("jocko-%03d", b.id)
	conf.MemberlistConfig.BindAddr = b.serfAddr
	conf.MemberlistConfig.BindPort = b.serfPort
	conf.NodeName = id
	conf.Tags["id"] = strconv.Itoa(int(b.id))
	conf.Tags["port"] = strconv.Itoa(b.port)
	conf.Tags["raft_port"] = strconv.Itoa(b.raftPort)
	conf.EventCh = eventCh
	conf.EnableNameConflictResolution = false
	s, err := serf.Create(conf)
	if err != nil {
		return nil, err
	}
	if len(b.peers) > 0 {
		var addrs []string
		for _, p := range b.peers {
			addr := &net.TCPAddr{IP: net.ParseIP(p.IP), Port: p.SerfPort}
			addrs = append(addrs, addr.String())
		}
		if _, err := s.Join(addrs, true); err != nil {
			return nil, err
		}
	}
	return s, nil
}

// serfEventHandler is used to handle events from the serf cluster
func (b *Broker) serfEventHandler() {
	for {
		select {
		case e := <-b.serfEventCh:
			switch e.EventType() {
			case serf.EventMemberJoin:
				b.nodeJoin(e.(serf.MemberEvent))
				b.localMemberEvent(e.(serf.MemberEvent))
			case serf.EventMemberLeave, serf.EventMemberFailed:
				b.nodeFailed(e.(serf.MemberEvent))
				b.localMemberEvent(e.(serf.MemberEvent))
			case serf.EventMemberUpdate, serf.EventMemberReap, serf.EventUser, serf.EventQuery:
				// ignore
			default:
				b.logger.Info("unhandled serf event: %#v", e)
			}
		case <-b.shutdownCh:
			return
		}
	}
}

// nodeJoin is used to handle join events on the serf cluster
func (b *Broker) nodeJoin(me serf.MemberEvent) {
	for _, m := range me.Members {
		// TODO: need to change these parts
		peer, err := brokerConn(m)
		if err != nil {
			b.logger.Info("failed to parse peer from serf member: %s", m.Name)
			continue
		}
		b.logger.Info("adding peer: %s", peer)
		b.peerLock.Lock()
		b.peers[peer.ID] = peer
		b.peerLock.Unlock()
	}
}

// localMemberEvent is used to reconcile Serf events with the store if we are the leader.
func (b *Broker) localMemberEvent(me serf.MemberEvent) {
	if !b.IsController() {
		return
	}
	isReap := me.EventType() == serf.EventMemberReap
	for _, m := range me.Members {
		if isReap {
			m.Status = StatusReap
		}
		select {
		case b.serfReconcileCh <- m:
		default:
		}
	}
}

// nodeFailed is used to handle fail events on the serf cluster.
func (b *Broker) nodeFailed(me serf.MemberEvent) {
	for _, m := range me.Members {
		peer, err := brokerConn(m)
		if err != nil {
			continue
		}
		b.logger.Info("removing peer: %s", me)
		b.peerLock.Lock()
		delete(b.peers, peer.ID)
		b.peerLock.Unlock()
	}
}

func (b *Broker) members() []serf.Member {
	return b.serf.Members()
}
