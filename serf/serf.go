package serf

import (
	"fmt"
	"net"
	"strconv"
	"sync"

	"github.com/hashicorp/serf/serf"
	"github.com/travisjeffery/jocko"
)

const (
	statusReap = serf.MemberStatus(-1)
)

// Serf manages membership of Jocko cluster using Hashicorp Serf
type Serf struct {
	logger      jocko.Logger
	serf        *serf.Serf
	addr        string
	nodeID      int32
	reconcileCh chan<- *jocko.ClusterMember
	initMembers []string
	shutdownCh  chan struct{}

	peers    map[int32]*jocko.ClusterMember
	peerLock sync.RWMutex
}

// New Serf object
func New(opts ...OptionFn) (*Serf, error) {
	s := &Serf{
		peers:      make(map[int32]*jocko.ClusterMember),
		shutdownCh: make(chan struct{}),
	}

	for _, opt := range opts {
		opt(s)
	}

	return s, nil
}

// Bootstrap saves the node metadata and starts the serf agent
// Info of node updates is returned on reconcileCh channel
func (s *Serf) Bootstrap(node *jocko.ClusterMember, reconcileCh chan<- *jocko.ClusterMember) error {
	addr, strPort, err := net.SplitHostPort(s.addr)
	if err != nil {
		return err
	}
	port, err := strconv.Atoi(strPort)
	if err != nil {
		return err
	}
	s.nodeID = node.ID
	eventCh := make(chan serf.Event, 256)
	conf := serf.DefaultConfig()
	conf.Init()
	conf.MemberlistConfig.BindAddr = addr
	conf.MemberlistConfig.BindPort = port
	conf.EventCh = eventCh
	conf.EnableNameConflictResolution = false
	conf.NodeName = fmt.Sprintf("jocko-%03d", node.ID)
	conf.Tags["id"] = strconv.Itoa(int(node.ID))
	conf.Tags["port"] = strconv.Itoa(node.Port)
	conf.Tags["raft_port"] = strconv.Itoa(node.RaftPort)
	sserf, err := serf.Create(conf)
	if err != nil {
		return err
	}
	s.serf = sserf
	s.reconcileCh = reconcileCh
	if _, err := s.Join(s.initMembers...); err != nil {
		// b.Shutdown()
		return err
	}

	// ingest events for serf
	go s.serfEventHandler(eventCh)

	s.logger = s.logger.With(jocko.String("ctx", "serf"), jocko.Int32("id", s.nodeID), jocko.String("addr", s.addr))
	s.logger.Info("bootstraped serf")

	return nil
}

// serfEventHandler is used to handle events from the serf cluster
func (s *Serf) serfEventHandler(eventCh <-chan serf.Event) {
	for {
		select {
		case e := <-eventCh:
			switch e.EventType() {
			case serf.EventMemberJoin:
				s.nodeJoinEvent(e.(serf.MemberEvent))
				s.localMemberEvent(e.(serf.MemberEvent))
			case serf.EventMemberLeave, serf.EventMemberFailed:
				s.nodeFailedEvent(e.(serf.MemberEvent))
				s.localMemberEvent(e.(serf.MemberEvent))
			case serf.EventMemberUpdate, serf.EventMemberReap, serf.EventUser, serf.EventQuery:
				// ignore
			default:
				s.logger.Info("unhandled serf event", jocko.Any("event", e))
			}
		case <-s.shutdownCh:
			return
		}
	}
}

// nodeJoinEvent is used to handle join events on the serf cluster
func (s *Serf) nodeJoinEvent(me serf.MemberEvent) {
	for _, m := range me.Members {
		// TODO: need to change these parts
		peer, err := clusterMember(m)
		if err != nil {
			s.logger.Error("failed to parse peer from serf member", jocko.Error("error", err), jocko.String("name", m.Name))
			continue
		}
		s.logger.Info("adding peer", jocko.Any("peer", peer))
		s.peerLock.Lock()
		s.peers[peer.ID] = peer
		s.peerLock.Unlock()
	}
}

// nodeFailedEvent is used to handle fail events on the serf cluster.
func (s *Serf) nodeFailedEvent(me serf.MemberEvent) {
	for _, m := range me.Members {
		s.logger.Info("removing peer", jocko.Any("peer", me))
		peer, err := clusterMember(m)
		if err != nil {
			continue
		}
		s.peerLock.Lock()
		delete(s.peers, peer.ID)
		s.peerLock.Unlock()
	}
}

// localMemberEvent is used to reconcile Serf events with the store if we are the leader.
func (s *Serf) localMemberEvent(me serf.MemberEvent) error {
	isReap := me.EventType() == serf.EventMemberReap
	for _, m := range me.Members {
		if isReap {
			m.Status = statusReap
		}
		conn, err := clusterMember(m)
		if err != nil {
			s.logger.Error("failed to parse serf member event", jocko.Error("error", err), jocko.Any("event", me))
			continue
		}
		s.reconcileCh <- conn
	}
	return nil
}

// ID of this serf node
func (s *Serf) ID() int32 {
	return s.nodeID
}

// Addr of serf agent
func (s *Serf) Addr() string {
	return s.addr
}

// Join an existing serf cluster
func (s *Serf) Join(addrs ...string) (int, error) {
	if len(addrs) == 0 {
		return 0, nil
	}
	return s.serf.Join(addrs, true)
}

// Cluster is the list of all nodes connected to Serf
func (s *Serf) Cluster() []*jocko.ClusterMember {
	s.peerLock.RLock()
	defer s.peerLock.RUnlock()

	cluster := make([]*jocko.ClusterMember, 0, len(s.peers))
	for _, v := range s.peers {
		cluster = append(cluster, v)
	}
	return cluster
}

// Member returns broker details of node with given ID
func (s *Serf) Member(memberID int32) *jocko.ClusterMember {
	s.peerLock.RLock()
	defer s.peerLock.RUnlock()
	return s.peers[memberID]
}

// leave the serf cluster
func (s *Serf) leave() error {
	if err := s.serf.Leave(); err != nil {
		return err
	}
	return nil
}

// Shutdown Serf agent
func (s *Serf) Shutdown() error {
	close(s.shutdownCh)
	if err := s.leave(); err != nil {
		return err
	}
	if err := s.serf.Shutdown(); err != nil {
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
