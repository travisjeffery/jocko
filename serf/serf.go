package serf

import (
	"fmt"
	"net"
	"strconv"
	"sync"

	"github.com/hashicorp/serf/serf"
	"github.com/travisjeffery/jocko"
	"github.com/travisjeffery/simplelog"
)

const (
	statusReap = serf.MemberStatus(-1)
)

// Serf manages membership of Jocko cluster using Hashicorp Serf
type Serf struct {
	logger      *simplelog.Logger
	serf        *serf.Serf
	addr        string
	reconcileCh chan<- *jocko.ClusterMember
	eventCh     chan serf.Event
	initMembers []string
	shutdownCh  chan struct{}

	peers    map[int32]*jocko.ClusterMember
	peerLock sync.RWMutex
}

// New Serf object
func New(opts ...OptionFn) (*Serf, error) {
	b := &Serf{
		peers:      make(map[int32]*jocko.ClusterMember),
		eventCh:    make(chan serf.Event, 256),
		shutdownCh: make(chan struct{}),
	}

	for _, opt := range opts {
		opt(b)
	}

	return b, nil
}

// Bootstrap saves the node metadata and starts the serf agent
// Info of node updates is returned on reconcileCh channel
func (b *Serf) Bootstrap(node *jocko.ClusterMember, reconcileCh chan<- *jocko.ClusterMember) error {
	addr, strPort, err := net.SplitHostPort(b.addr)
	if err != nil {
		return err
	}

	port, err := strconv.Atoi(strPort)
	if err != nil {
		return err
	}
	conf := serf.DefaultConfig()
	conf.Init()
	conf.MemberlistConfig.BindAddr = addr
	conf.MemberlistConfig.BindPort = port
	conf.EventCh = b.eventCh
	conf.EnableNameConflictResolution = false
	conf.NodeName = fmt.Sprintf("jocko-%03d", node.ID)
	conf.Tags["id"] = strconv.Itoa(int(node.ID))
	conf.Tags["port"] = strconv.Itoa(node.Port)
	conf.Tags["raft_port"] = strconv.Itoa(node.RaftPort)
	s, err := serf.Create(conf)
	if err != nil {
		return err
	}
	b.serf = s
	b.reconcileCh = reconcileCh
	if _, err := b.Join(b.initMembers...); err != nil {
		// b.Shutdown()
		return err
	}

	// ingest events for serf
	go b.serfEventHandler()

	return nil
}

// serfEventHandler is used to handle events from the serf cluster
func (b *Serf) serfEventHandler() {
	for {
		select {
		case e := <-b.eventCh:
			switch e.EventType() {
			case serf.EventMemberJoin:
				b.nodeJoinEvent(e.(serf.MemberEvent))
				b.localMemberEvent(e.(serf.MemberEvent))
			case serf.EventMemberLeave, serf.EventMemberFailed:
				b.nodeFailedEvent(e.(serf.MemberEvent))
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

// nodeJoinEvent is used to handle join events on the serf cluster
func (b *Serf) nodeJoinEvent(me serf.MemberEvent) {
	for _, m := range me.Members {
		// TODO: need to change these parts
		peer, err := clusterMember(m)
		if err != nil {
			b.logger.Info("failed to parse peer from serf member: %s", m.Name)
			continue
		}
		b.logger.Info("adding peer: %+v", peer)
		b.peerLock.Lock()
		b.peers[peer.ID] = peer
		b.peerLock.Unlock()
	}
}

// nodeFailedEvent is used to handle fail events on the serf cluster.
func (b *Serf) nodeFailedEvent(me serf.MemberEvent) {
	for _, m := range me.Members {
		b.logger.Info("removing peer: %s", me)
		peer, err := clusterMember(m)
		if err != nil {
			continue
		}
		b.peerLock.Lock()
		delete(b.peers, peer.ID)
		b.peerLock.Unlock()
	}
}

// localMemberEvent is used to reconcile Serf events with the store if we are the leader.
func (b *Serf) localMemberEvent(me serf.MemberEvent) error {
	isReap := me.EventType() == serf.EventMemberReap
	for _, m := range me.Members {
		if isReap {
			m.Status = statusReap
		}
		conn, err := clusterMember(m)
		if err != nil {
			b.logger.Info("failed to parse serf member event: %s", m)
			continue
		}
		b.reconcileCh <- conn
	}
	return nil
}

// Addr of serf agent
func (b *Serf) Addr() string {
	return b.addr
}

// Join an existing serf cluster
func (b *Serf) Join(addrs ...string) (int, error) {
	if len(addrs) == 0 {
		return 0, nil
	}
	return b.serf.Join(addrs, true)
}

// Cluster is the list of all nodes connected to Serf
func (b *Serf) Cluster() []*jocko.ClusterMember {
	b.peerLock.RLock()
	defer b.peerLock.RUnlock()

	cluster := make([]*jocko.ClusterMember, 0, len(b.peers))
	for _, v := range b.peers {
		cluster = append(cluster, v)
	}
	return cluster
}

// Member returns broker details of node with given ID
func (b *Serf) Member(memberID int32) *jocko.ClusterMember {
	b.peerLock.RLock()
	defer b.peerLock.RUnlock()
	return b.peers[memberID]
}

// Leave the serf cluster
func (b *Serf) Leave() error {
	if err := b.serf.Leave(); err != nil {
		return err
	}
	return nil
}

// Shutdown Serf agent
func (b *Serf) Shutdown() error {
	close(b.shutdownCh)
	if err := b.serf.Shutdown(); err != nil {
		return err
	}
	return nil
}

func clusterMember(m serf.Member) (*jocko.ClusterMember, error) {
	portStr := m.Tags["port"]
	port, err := strconv.Atoi(portStr)
	if err != nil {
		return nil, err
	}

	idStr := m.Tags["id"]
	id, err := strconv.Atoi(idStr)
	if err != nil {
		return nil, err
	}

	raftPortStr := m.Tags["raft_port"]
	raftPort, err := strconv.Atoi(raftPortStr)
	if err != nil {
		return nil, err
	}

	conn := &jocko.ClusterMember{
		IP:       m.Addr.String(),
		ID:       int32(id),
		RaftPort: raftPort,
		Port:     port,
		Status:   status(m.Status),
	}

	return conn, nil
}

func status(s serf.MemberStatus) jocko.MemberStatus {
	switch s {
	case serf.StatusAlive:
		return jocko.StatusAlive
	case serf.StatusFailed:
		return jocko.StatusFailed
	case serf.StatusLeaving:
		return jocko.StatusLeaving
	case serf.StatusLeft:
		return jocko.StatusLeft
	case statusReap:
		return jocko.StatusReap
	default:
		return jocko.StatusNone
	}
}
