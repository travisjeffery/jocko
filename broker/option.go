package broker

import (
	"github.com/hashicorp/raft"
	"github.com/travisjeffery/jocko/jocko"
	"github.com/travisjeffery/simplelog"
)

type BrokerFn func(b *Broker)

func DataDir(dataDir string) BrokerFn {
	return func(b *Broker) {
		b.dataDir = dataDir
	}
}

func LogDir(logDir string) BrokerFn {
	return func(b *Broker) {
		b.logDir = logDir
	}
}

func RaftAddr(raftAddr string) BrokerFn {
	return func(b *Broker) {
		b.raftAddr = raftAddr
	}
}

func SerfAddr(serfAddr string) BrokerFn {
	return func(b *Broker) {
		b.serfAddr = serfAddr
	}
}

func BrokerAddr(brokerAddr string) BrokerFn {
	return func(b *Broker) {
		b.brokerAddr = brokerAddr
	}
}

func SerfMembers(serfMembers []string) BrokerFn {
	return func(b *Broker) {
		b.serfMembers = serfMembers
	}
}

func Brokers(brokers []*jocko.BrokerConn) BrokerFn {
	return func(b *Broker) {
		b.peerLock.Lock()
		for _, peer := range brokers {
			b.peers[peer.ID] = peer
		}
		b.peerLock.Unlock()
	}
}

func Logger(logger *simplelog.Logger) BrokerFn {
	return func(b *Broker) {
		b.logger = logger
	}
}

func RaftConfig(raft *raft.Config) BrokerFn {
	return func(b *Broker) {
		b.raftConfig = raft
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
