package serf

import (
	"fmt"
	"net"
	"strconv"
	"sync"

	"github.com/hashicorp/serf/serf"
	"github.com/travisjeffery/jocko"
	"github.com/travisjeffery/jocko/log"
)

const (
	statusReap = serf.MemberStatus(-1)
)

type Config struct {
	ID       int32
	Addr     string
	RaftAddr string
	Join     []string
	JoinWAN  []string
}

// Serf manages membership of Jocko cluster using Hashicorp Serf
type Serf struct {
	config      Config
	logger      log.Logger
	serf        *serf.Serf
	reconcileCh chan<- *jocko.ClusterMember
	shutdownCh  chan struct{}
	peers       map[int32]*jocko.ClusterMember
	peerLock    sync.RWMutex
}

// New Serf object
func New(config Config, logger log.Logger) (*Serf, error) {
	b := &Serf{
		config:     config,
		logger:     logger,
		peers:      make(map[int32]*jocko.ClusterMember),
		shutdownCh: make(chan struct{}),
	}
	if err := b.setupSerf(); err != nil {
		return nil, err
	}
	return b, nil
}

func (s *Serf) setupSerf() error {
	addr, strPort, err := net.SplitHostPort(s.config.Addr)
	if err != nil {
		return err
	}
	port, err := strconv.Atoi(strPort)
	if err != nil {
		return err
	}
	eventCh := make(chan serf.Event, 256)
	conf := serf.DefaultConfig()
	conf.Init()
	conf.MemberlistConfig.BindAddr = addr
	conf.MemberlistConfig.BindPort = port
	conf.EventCh = eventCh
	conf.EnableNameConflictResolution = false
	conf.NodeName = fmt.Sprintf("jocko-%03d", s.config.ID)
	conf.Tags["id"] = strconv.Itoa(int(s.config.ID))
	conf.Tags["port"] = strconv.Itoa(int(s.config.ID))
	conf.Tags["raft_addr"] = s.config.RaftAddr
	s.serf, err = serf.Create(conf)
	if err != nil {
		return err
	}
	if _, err := s.Join(s.config.Join...); err != nil {
		// b.Shutdown()
		return err
	}

	// ingest events for serf
	go s.serfEventHandler(eventCh)

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
				s.logger.Info("unhandled serf event", log.Any("event", e))
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
			s.logger.Error("failed to parse peer from serf member", log.Error("error", err), log.String("name", m.Name))
			continue
		}
		s.logger.Info("adding peer", log.Any("peer", peer))
		s.peerLock.Lock()
		s.peers[peer.ID] = peer
		s.peerLock.Unlock()
	}
}

// nodeFailedEvent is used to handle fail events on the serf cluster.
func (s *Serf) nodeFailedEvent(me serf.MemberEvent) {
	for _, m := range me.Members {
		s.logger.Info("removing peer", log.Any("peer", me))
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
			s.logger.Error("failed to parse serf member event", log.Error("error", err), log.Any("event", me))
			continue
		}
		s.reconcileCh <- conn
	}
	return nil
}

// ID of this serf node
func (s *Serf) ID() int32 {
	return s.config.ID
}

// Addr of serf agent
func (s *Serf) Addr() string {
	return s.config.Addr
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

	conn := &jocko.ClusterMember{
		IP:       m.Addr.String(),
		ID:       int32(id),
		RaftAddr: m.Tags["raft_addr"],
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

func addrPort(addr string) (int, error) {
	_, strPort, err := net.SplitHostPort(addr)
	if err != nil {
		return 0, err
	}

	port, err := strconv.Atoi(strPort)
	if err != nil {
		return 0, err
	}
	return port, nil
}
