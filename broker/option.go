package broker

import (
	"github.com/hashicorp/raft"
	"github.com/travisjeffery/jocko/jocko"
	"github.com/travisjeffery/simplelog"
)

type Option interface {
	modifyBroker(*Broker)
}

func OptionDataDir(dataDir string) Option {
	return optionDataDir(dataDir)
}

type optionDataDir string

func (o optionDataDir) modifyBroker(b *Broker) {
	b.dataDir = string(o)
}

func OptionLogDir(logDir string) Option {
	return optionLogDir(logDir)
}

type optionLogDir string

func (o optionLogDir) modifyBroker(b *Broker) {
	b.logDir = string(o)
}

type optionPort int

func OptionPort(port int) Option {
	return optionPort(port)
}

func (o optionPort) modifyBroker(b *Broker) {
	b.port = int(o)
}

type optionRaftPort int

func OptionRaftPort(raftPort int) Option {
	return optionRaftPort(raftPort)
}

func (o optionRaftPort) modifyBroker(b *Broker) {
	b.raftPort = int(o)
}

type optionSerfPort int

func OptionSerfPort(serfPort int) Option {
	return optionSerfPort(serfPort)
}

func (o optionSerfPort) modifyBroker(b *Broker) {
	b.serfPort = int(o)
}

type optionBindAddr string

func OptionBindAddr(bindAddr string) Option {
	return optionBindAddr(bindAddr)
}

func (o optionBindAddr) modifyBroker(b *Broker) {
	b.bindAddr = string(o)
}

func OptionBrokers(brokers []*jocko.BrokerConn) Option {
	return optionBrokers{brokers}
}

type optionBrokers struct {
	brokers []*jocko.BrokerConn
}

func (o optionBrokers) modifyBroker(b *Broker) {
	b.peerLock.Lock()
	for _, peer := range o.brokers {
		b.peers[peer.ID] = peer
	}
	b.peerLock.Unlock()
}

func OptionLogger(logger *simplelog.Logger) Option {
	return optionLogger{logger}
}

type optionLogger struct {
	logger *simplelog.Logger
}

func (o optionLogger) modifyBroker(b *Broker) {
	b.logger = o.logger
}

type optionRaft struct {
	raftConfig *raft.Config
}

func (o optionRaft) modifyBroker(b *Broker) {
	b.raftConfig = o.raftConfig
}

func OptionRaft(conf *raft.Config) Option {
	return optionRaft{conf}
}

type ReplicatorOption interface {
	modifyReplicator(*PartitionReplicator)
}

type ReplicatorOptionReplicaID int32

func (o ReplicatorOptionReplicaID) modifyReplicator(r *PartitionReplicator) {
	r.replicaID = int32(o)
}

type ReplicatorOptionFetchSize int32

func (o ReplicatorOptionFetchSize) modifyReplicator(r *PartitionReplicator) {
	r.fetchSize = int32(o)
}

type ReplicatorOptionMinBytes int32

func (o ReplicatorOptionMinBytes) modifyReplicator(r *PartitionReplicator) {
	r.minBytes = int32(o)
}

type ReplicatorOptionMaxWaitTime int32

func (o ReplicatorOptionMaxWaitTime) modifyReplicator(r *PartitionReplicator) {
	r.maxWaitTime = int32(o)
}
