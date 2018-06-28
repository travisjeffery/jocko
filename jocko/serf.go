package jocko

import (
	"fmt"
	"path/filepath"

	"github.com/hashicorp/raft"
	"github.com/hashicorp/serf/serf"
	"github.com/travisjeffery/jocko/jocko/metadata"
	"github.com/travisjeffery/jocko/log"
)

const (
	// StatusReap is used to update the status of a node if we
	// are handling a EventMemberReap
	StatusReap = serf.MemberStatus(-1)
)

func (b *Broker) setupSerf(config *serf.Config, ch chan serf.Event, path string) (*serf.Serf, error) {
	config.Init()
	config.NodeName = b.config.NodeName
	config.Tags["role"] = "jocko"
	config.Tags["id"] = fmt.Sprintf("%d", b.config.ID)
	config.Logger = log.NewStdLogger(log.New(log.DebugLevel, fmt.Sprintf("serf/%d: ", b.config.ID)))
	config.MemberlistConfig.Logger = log.NewStdLogger(log.New(log.DebugLevel, fmt.Sprintf("memberlist/%d: ", b.config.ID)))
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
			case serf.EventMemberReap:
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
		log.Info.Printf("broker/%d: adding LAN server: %s", b.config.ID, meta.ID)
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
		log.Info.Printf("broker/%d: removing LAN server: %s", b.config.ID, m.Name)
		b.brokerLookup.RemoveBroker(meta)
	}
}

func (b *Broker) localMemberEvent(me serf.MemberEvent) {
	if !b.isLeader() {
		return
	}

	isReap := me.EventType() == serf.EventMemberReap

	for _, m := range me.Members {
		if isReap {
			m.Status = StatusReap
		}
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
		log.Error.Printf("broker/%d: read last raft index error: %s", b.config.ID, err)
		return
	}
	if index != 0 {
		log.Info.Printf("broker/%d: raft data found, disabling bootstrap mode: index: %d, path: %s", b.config.ID, index, filepath.Join(b.config.DataDir, raftState))
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
			log.Error.Printf("broker/%d: members expects conflicting node count: %s", b.config.ID, member.Name)
			return
		}
		if meta.Bootstrap {
			log.Error.Printf("broker/%d; member %s has bootstrap mode. expect disabled", b.config.ID, member.Name)
			return
		}
		brokers = append(brokers, *meta)
	}

	if len(brokers) < b.config.BootstrapExpect {
		log.Debug.Printf("broker/%d: maybe bootstrap: need more brokers: got: %d: expect: %d", b.config.ID, len(brokers), b.config.BootstrapExpect)
		return
	}

	var configuration raft.Configuration
	addrs := make([]string, 0, len(brokers))
	for _, meta := range brokers {
		addr := meta.RaftAddr
		addrs = append(addrs, addr)
		peer := raft.Server{
			ID:      raft.ServerID(meta.ID.String()),
			Address: raft.ServerAddress(addr),
		}
		configuration.Servers = append(configuration.Servers, peer)
	}

	log.Info.Printf("broker/%d: found expected number of peers, attempting bootstrap: addrs: %v", b.config.ID, addrs)
	future := b.raft.BootstrapCluster(configuration)
	if err := future.Error(); err != nil {
		log.Error.Printf("broker/%d: bootstrap cluster error: %s", b.config.ID, err)
	}
	b.config.BootstrapExpect = 0
}
