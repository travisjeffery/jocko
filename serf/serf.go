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
	nodeID      int32
	eventCh     chan serf.Event
	reconcileCh chan<- *jocko.ClusterMember
	initMembers []string
	shutdownCh  chan struct{}

	peers    map[int32]*jocko.ClusterMember
	peerLock sync.RWMutex
}

// New Serf object
func New(opts ...OptionFn) (*Serf, error) {
	b := &Serf{
		peers:      make(map[int32]*jocko.ClusterMember),
		eventCh:    make(chan serf.Event),
		shutdownCh: make(chan struct{}),
	}

	for _, opt := range opts {
		opt(b)
	}

	return b, nil
}

// Configure builds a default serf.Config for a jocko.Serf and
// jocko.ClusterMember pair on a given event channel.
func (s *Serf) Configure(node *jocko.ClusterMember) (*serf.Config, error) {
	config := serf.DefaultConfig()
	config.Init()

	config.EventCh = s.eventCh

	config.NodeName = fmt.Sprintf("jocko-%03d", node.ID)

	config.EnableNameConflictResolution = false

	// setup serf communication address
	memberAddr, memberPort, err := s.HostPort()
	if err != nil {
		return nil, err
	}
	config.MemberlistConfig.BindAddr = memberAddr
	config.MemberlistConfig.BindPort = memberPort

	// add metadata for query filtering
	config.Tags["id"] = strconv.Itoa(int(node.ID))
	config.Tags["port"] = strconv.Itoa(node.Port)
	config.Tags["raft_port"] = strconv.Itoa(node.RaftPort)

	return config, nil
}

// Bootstrap saves the node metadata and starts the serf agent
// Info of node updates is returned on reconcileCh channel
func (b *Serf) Bootstrap(node *jocko.ClusterMember, reconcileCh chan<- *jocko.ClusterMember) error {
	config, err := b.Configure(node)
	if err != nil {
		return err
	}

	s, err := serf.Create(config)
	if err != nil {
		return err
	}
	b.serf = s

	b.nodeID = node.ID
	b.reconcileCh = reconcileCh

	if _, err := b.Join(b.initMembers...); err != nil {
		// b.Shutdown()
		return err
	}

	// ingest events for serf
	go b.serfEventHandler(b.eventCh)

	return nil
}

// serfEventHandler is used to handle events from the serf cluster
func (b *Serf) serfEventHandler(eventCh <-chan serf.Event) {
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

// ID of this serf node
func (b *Serf) ID() int32 {
	return b.nodeID
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

// leave the serf cluster
func (b *Serf) leave() error {
	if err := b.serf.Leave(); err != nil {
		return err
	}
	return nil
}

// Shutdown Serf agent
func (b *Serf) Shutdown() error {
	close(b.shutdownCh)
	if err := b.leave(); err != nil {
		return err
	}
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

func (s *Serf) HostPort() (string, int, error) {
	addr, strPort, err := net.SplitHostPort(s.addr)
	if err != nil {
		return "", 0, err
	}

	port, err := strconv.Atoi(strPort)
	if err != nil {
		return "", 0, err
	}

	return addr, port, nil
}
