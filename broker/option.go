package broker

import (
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

func OptionRaftAddr(raftAddr string) Option {
	return optionRaftAddr(raftAddr)
}

type optionRaftAddr string

func (o optionRaftAddr) modifyBroker(b *Broker) {
	b.raftAddr = string(o)
}

func OptionTCPAddr(tcpAddr string) Option {
	return optionTCPAddr(tcpAddr)
}

type optionTCPAddr string

func (o optionTCPAddr) modifyBroker(b *Broker) {
	b.tcpAddr = string(o)
}

func OptionBrokers(brokers []*jocko.BrokerConn) Option {
	return optionBrokers{brokers}
}

type optionBrokers struct {
	brokers []*jocko.BrokerConn
}

func (o optionBrokers) modifyBroker(b *Broker) {
	b.brokers = o.brokers
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
