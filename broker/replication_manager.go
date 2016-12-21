package broker

import (
	"github.com/travisjeffery/jocko/jocko"
	"github.com/travisjeffery/jocko/protocol"
)

type replicationManager struct {
	jocko.Broker
	replicators map[*jocko.Partition]*PartitionReplicator
}

func newReplicationManager() *replicationManager {
	return &replicationManager{
		replicators: make(map[*jocko.Partition]*PartitionReplicator),
	}
}

func (rm *replicationManager) BecomeFollower(topic string, pid int32, leader int32) error {
	p, err := rm.Partition(topic, pid)
	if err != nil {
		return err
	}
	// stop replicator to current leader
	if r, ok := rm.replicators[p]; ok {
		if err := r.Close(); err != nil {
			return err
		}
	}
	delete(rm.replicators, p)
	p.Leader = rm.BrokerConn(leader)
	hw := p.HighWatermark()
	if err := p.TruncateTo(hw); err != nil {
		return err
	}
	r := NewPartitionReplicator(p, rm.ID())
	r.Replicate()
	rm.replicators[p] = r
	return nil
}

func (rm *replicationManager) BecomeLeader(topic string, pid int32, command *protocol.PartitionState) error {
	p, err := rm.Partition(topic, pid)
	if err != nil {
		return err
	}
	if r, ok := rm.replicators[p]; ok {
		if err := r.Close(); err != nil {
			return err
		}
	}
	var conns []*jocko.BrokerConn
	for _, isr := range command.ISR {
		conns = append(conns, rm.BrokerConn(isr))
	}
	p.Leader = rm.BrokerConn(rm.ID())
	p.ISR = conns
	p.LeaderandISRVersionInZK = command.ZKVersion
	return nil
}
