package broker

import (
	"fmt"
	"path/filepath"

	"github.com/hashicorp/raft"
	"github.com/hashicorp/serf/serf"
	"github.com/travisjeffery/jocko/broker/metadata"
	"github.com/travisjeffery/jocko/log"
)

func (s *Broker) setupSerf(config *serf.Config, ch chan serf.Event, path string) (*serf.Serf, error) {
	config.Init()
	config.NodeName = s.config.NodeName
	config.Tags["role"] = "jocko"
	config.Tags["id"] = fmt.Sprintf("%d", s.config.ID)
	if s.config.Bootstrap {
		config.Tags["bootstrap"] = "1"
	}
	if s.config.BootstrapExpect != 0 {
		config.Tags["expect"] = fmt.Sprintf("%d", s.config.BootstrapExpect)
	}
	if s.config.NonVoter {
		config.Tags["non_voter"] = "1"
	}
	config.Tags["raft_addr"] = s.config.RaftAddr
	config.Tags["serf_lan_addr"] = fmt.Sprintf("%s:%d", s.config.SerfLANConfig.MemberlistConfig.BindAddr, s.config.SerfLANConfig.MemberlistConfig.BindPort)
	config.Tags["broker_addr"] = s.config.Addr
	config.EventCh = ch
	config.EnableNameConflictResolution = false
	if !s.config.DevMode {
		config.SnapshotPath = filepath.Join(s.config.DataDir, path)
	}
	if err := ensurePath(config.SnapshotPath, false); err != nil {
		return nil, err
	}
	return serf.Create(config)
}

func (s *Broker) lanEventHandler() {
	for {
		select {
		case e := <-s.eventChLAN:
			switch e.EventType() {
			case serf.EventMemberJoin:
				s.lanNodeJoin(e.(serf.MemberEvent))
				s.localMemberEvent(e.(serf.MemberEvent))
			case serf.EventMemberLeave, serf.EventMemberFailed:
				s.lanNodeFailed(e.(serf.MemberEvent))
				s.localMemberEvent(e.(serf.MemberEvent))
			}
		case <-s.shutdownCh:
			return
		}
	}
}

// lanNodeJoin is used to handle join events on the LAN pool.
func (s *Broker) lanNodeJoin(me serf.MemberEvent) {
	for _, m := range me.Members {
		b, ok := metadata.IsBroker(m)
		if !ok {
			continue
		}
		s.logger.Info("adding LAN server", log.Any("meta", b))
		// update server lookup
		s.brokerLookup.AddBroker(b)
		if s.config.BootstrapExpect != 0 {
			s.maybeBootstrap()
		}
	}
}

func (s *Broker) lanNodeFailed(me serf.MemberEvent) {
	for _, m := range me.Members {
		meta, ok := metadata.IsBroker(m)
		if !ok {
			continue
		}
		s.logger.Info("removing LAN server", log.Any("member", m))
		s.brokerLookup.RemoveBroker(meta)
	}
}

func (s *Broker) localMemberEvent(me serf.MemberEvent) {
	if !s.isLeader() {
		return
	}

	for _, m := range me.Members {
		select {
		case s.reconcileCh <- m:
		default:
		}
	}
}

func (s *Broker) maybeBootstrap() {
	var index uint64
	var err error
	if s.config.DevMode {
		index, err = s.raftInmem.LastIndex()
	} else {
		index, err = s.raftStore.LastIndex()
	}
	if err != nil {
		s.logger.Error("failed to read last raft index", log.Error("error", err))
		return
	}
	if index != 0 {
		s.logger.Info("raft data found, disabling bootstrap mode")
		s.config.BootstrapExpect = 0
		return
	}

	members := s.LANMembers()
	var brokers []metadata.Broker
	for _, member := range members {
		b, ok := metadata.IsBroker(member)
		if !ok {
			continue
		}
		if b.Expect != 0 && b.Expect != s.config.BootstrapExpect {
			s.logger.Error("members expects conflicting node count", log.Any("member", member))
			return
		}
		if b.Bootstrap {
			s.logger.Error("member has bootstrap mode. expect disabled.", log.Any("member", member))
			return
		}
		brokers = append(brokers, *b)
	}

	if len(brokers) < s.config.BootstrapExpect {
		return
	}

	var configuration raft.Configuration
	var addrs []string
	for _, b := range brokers {
		addr := b.RaftAddr
		addrs = append(addrs, addr)
		peer := raft.Server{
			ID:      raft.ServerID(fmt.Sprintf("%d", b.ID)),
			Address: raft.ServerAddress(addr),
		}
		configuration.Servers = append(configuration.Servers, peer)
	}

	s.logger.Info("found expected number of peers, attempting bootstrap", log.Any("addrs", addrs))
	future := s.raft.BootstrapCluster(configuration)
	if err := future.Error(); err != nil {
		s.logger.Error("failed to bootstrap cluster", log.Error("error", err))
	}
	s.config.BootstrapExpect = 0
}
