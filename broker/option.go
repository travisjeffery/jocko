package broker

import (
	"github.com/travisjeffery/jocko"
	"github.com/travisjeffery/simplelog"
)

// BrokerFn is used to configure brokers.
type BrokerFn func(b *Broker)

// LogDir is used to set the directory the broker stores its data logs.
func LogDir(logDir string) BrokerFn {
	return func(b *Broker) {
		b.logDir = logDir
	}
}

// Addr is used to set the broker's client addr.
func Addr(brokerAddr string) BrokerFn {
	return func(b *Broker) {
		b.brokerAddr = brokerAddr
	}
}

// Logger is used to set the broker's logger.
func Logger(logger *simplelog.Logger) BrokerFn {
	return func(b *Broker) {
		b.logger = logger
	}
}

// Serf is used to set the broker's serf instance.
func Serf(serf jocko.Serf) BrokerFn {
	return func(b *Broker) {
		b.serf = serf
	}
}

// Raft is used to set the broker's raft instance.
func Raft(raft jocko.Raft) BrokerFn {
	return func(b *Broker) {
		b.raft = raft
	}
}

// ReplicatorFn is used to configure replicators.
type ReplicatorFn func(r *Replicator)

// ReplicatorReplicaID is used to set the ID of the broker this replicator should replicate. Similar to the consumer config in Kafka.
func ReplicatorReplicaID(id int32) ReplicatorFn {
	return func(r *Replicator) {
		r.replicaID = id
	}
}

// ReplicatorFetchSize is used to set replicator's fetch request size.
func ReplicatorFetchSize(size int32) ReplicatorFn {
	return func(r *Replicator) {
		r.fetchSize = size
	}
}

// ReplicatorMinBytes is used to set the replicator's min byte request size. Similar to the consumer config in Kafka.
func ReplicatorMinBytes(size int32) ReplicatorFn {
	return func(r *Replicator) {
		r.minBytes = size
	}
}

// ReplicatorMaxWaitTime is used to set the replicator's request's max wait time. Similar to the consumer config in Kakfa.
func ReplicatorMaxWaitTime(time int32) ReplicatorFn {
	return func(r *Replicator) {
		r.maxWaitTime = time
	}
}

// ReplicatorLeader is used to set the replicator's leader to consume from.
func ReplicatorLeader(leader jocko.Client) ReplicatorFn {
	return func(r *Replicator) {
		r.leader = leader
	}
}
