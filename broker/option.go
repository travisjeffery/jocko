package broker

import (
	"github.com/travisjeffery/jocko"
	"github.com/travisjeffery/simplelog"
)

type BrokerFn func(b *Broker)

func LogDir(logDir string) BrokerFn {
	return func(b *Broker) {
		b.logDir = logDir
	}
}

func Addr(brokerAddr string) BrokerFn {
	return func(b *Broker) {
		b.brokerAddr = brokerAddr
	}
}

func Logger(logger *simplelog.Logger) BrokerFn {
	return func(b *Broker) {
		b.logger = logger
	}
}

func Serf(serf jocko.Serf) BrokerFn {
	return func(b *Broker) {
		b.serf = serf
	}
}

func Raft(raft jocko.Raft) BrokerFn {
	return func(b *Broker) {
		b.raft = raft
	}
}

type ReplicatorFn func(r *replicator)

func ReplicatorReplicaID(id int32) ReplicatorFn {
	return func(r *replicator) {
		r.replicaID = id
	}
}

func ReplicatorFetchSize(size int32) ReplicatorFn {
	return func(r *replicator) {
		r.fetchSize = size
	}
}

func ReplicatorMinBytes(size int32) ReplicatorFn {
	return func(r *replicator) {
		r.minBytes = size
	}
}

func ReplicatorMaxWaitTime(time int32) ReplicatorFn {
	return func(r *replicator) {
		r.maxWaitTime = time
	}
}

func ReplicatorProxy(proxy jocko.Proxy) ReplicatorFn {
	return func(r *replicator) {
		r.proxy = proxy
	}
}
