package jocko

import (
	"fmt"
	"path/filepath"

	"github.com/hashicorp/raft"
	"github.com/hashicorp/serf/serf"
	"github.com/travisjeffery/jocko/jocko/metadata"
	"github.com/travisjeffery/jocko/log"
)

func (b *Broker) setupSerf(config *serf.Config, ch chan serf.Event, path string) (*serf.Serf, error) {
	config.Init()
	config.NodeName = b.config.NodeName
	config.Tags["role"] = "jocko"
	config.Tags["id"] = fmt.Sprintf("%d", b.config.ID)
	if b.config.Bootstrap {
		config.Tags["bootstrap"] = "1"
	}
	if b.config.BootstrapExpect != 0 {
		config.Tags["expect"] = fmt.Sprintf("%d", b.config.BootstrapExpect)
	}
	if b.config.NonVoter {
		config.Tags["non_voter"] = "1"
	}
	config.Tags["raft_addr"] = b.config.RaftAddr
	config.Tags["serf_lan_addr"] = fmt.Sprintf("%s:%d", b.config.SerfLANConfig.MemberlistConfig.BindAddr, b.config.SerfLANConfig.MemberlistConfig.BindPort)
	config.Tags["broker_addr"] = b.config.Addr
	config.EventCh = ch
	config.EnableNameConflictResolution = false
	if !b.config.DevMode {
		config.SnapshotPath = filepath.Join(b.config.DataDir, path)
	}
	if err := ensurePath(config.SnapshotPath, false); err != nil {
		return nil, err
	}
	return serf.Create(config)
}

func (b *Broker) lanEventHandler() {
	for {
		select {
		case e := <-b.eventChLAN:
			switch e.EventType() {
			case serf.EventMemberJoin:
				b.lanNodeJoin(e.(serf.MemberEvent))
				b.localMemberEvent(e.(serf.MemberEvent))
			case serf.EventMemberLeave, serf.EventMemberFailed:
				b.lanNodeFailed(e.(serf.MemberEvent))
				b.localMemberEvent(e.(serf.MemberEvent))
			}
		case <-b.shutdownCh:
			return
		}
	}
}

// lanNodeJoin is used to handle join events on the LAN pool.
func (b *Broker) lanNodeJoin(me serf.MemberEvent) {
	for _, m := range me.Members {
		meta, ok := metadata.IsBroker(m)
		if !ok {
			continue
		}
		b.logger.Info("adding LAN server", log.Any("meta", meta))
		// update server lookup
		b.brokerLookup.AddBroker(meta)
		if b.config.BootstrapExpect != 0 {
			b.maybeBootstrap()
		}
	}
}

func (b *Broker) lanNodeFailed(me serf.MemberEvent) {
	for _, m := range me.Members {
		meta, ok := metadata.IsBroker(m)
		if !ok {
			continue
		}
		b.logger.Info("removing LAN server", log.Any("member", m))
		b.brokerLookup.RemoveBroker(meta)
	}
}

func (b *Broker) localMemberEvent(me serf.MemberEvent) {
	if !b.isLeader() {
		return
	}

	for _, m := range me.Members {
		select {
		case b.reconcileCh <- m:
		default:
		}
	}
}

func (b *Broker) maybeBootstrap() {
	var index uint64
	var err error
	if b.config.DevMode {
		index, err = b.raftInmem.LastIndex()
	} else {
		index, err = b.raftStore.LastIndex()
	}
	if err != nil {
		b.logger.Error("failed to read last raft index", log.Error("error", err))
		return
	}
	if index != 0 {
		b.logger.Info("raft data found, disabling bootstrap mode", log.String("store path", filepath.Join(b.config.DataDir, raftState)))
		b.config.BootstrapExpect = 0
		return
	}

	members := b.LANMembers()
	brokers := make([]metadata.Broker, 0, len(members))
	for _, member := range members {
		meta, ok := metadata.IsBroker(member)
		if !ok {
			continue
		}
		if meta.Expect != 0 && meta.Expect != b.config.BootstrapExpect {
			b.logger.Error("members expects conflicting node count", log.Any("member", member))
			return
		}
		if meta.Bootstrap {
			b.logger.Error("member has bootstrap mode. expect disabled.", log.Any("member", member))
			return
		}
		brokers = append(brokers, *meta)
	}

	if len(brokers) < b.config.BootstrapExpect {
		b.logger.Debug("maybe bootstrap: need more brokers", log.Int("brokers", len(brokers)), log.Int("bootstrap expect", b.config.BootstrapExpect))
		return
	}

	var configuration raft.Configuration
	addrs := make([]string, len(brokers), 0)
	for _, meta := range brokers {
		addr := meta.RaftAddr
		addrs = append(addrs, addr)
		peer := raft.Server{
			ID:      raft.ServerID(fmt.Sprintf("%d", meta.ID)),
			Address: raft.ServerAddress(addr),
		}
		configuration.Servers = append(configuration.Servers, peer)
	}

	b.logger.Info("found expected number of peers, attempting bootstrap", log.Any("addrs", addrs))
	future := b.raft.BootstrapCluster(configuration)
	if err := future.Error(); err != nil {
		b.logger.Error("failed to bootstrap cluster", log.Error("error", err))
	}
	b.config.BootstrapExpect = 0
}
