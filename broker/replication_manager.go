package broker

import (
	"github.com/travisjeffery/jocko"
	"github.com/travisjeffery/jocko/protocol"
	"github.com/travisjeffery/jocko/server"
)

type replicationManager struct {
	jocko.Broker
	replicators map[*jocko.Partition]*replicator
}

func newReplicationManager() *replicationManager {
	return &replicationManager{
		replicators: make(map[*jocko.Partition]*replicator),
	}
}

func (rm *replicationManager) BecomeFollower(topic string, pid int32, command *protocol.PartitionState) error {
	p, err := rm.Partition(topic, pid)
	if err != nil {
		return err
	}
	// stop replicator to current leader
	if r, ok := rm.replicators[p]; ok {
		if err := r.close(); err != nil {
			return err
		}
	}
	delete(rm.replicators, p)
	p.Leader = command.Leader
	hw := p.HighWatermark()
	if err := p.TruncateTo(hw); err != nil {
		return err
	}
	r := newReplicator(p, rm.ID(),
		ReplicatorProxy(server.NewProxy(p.Conn)))
	r.replicate()
	rm.replicators[p] = r
	return nil
}

func (rm *replicationManager) BecomeLeader(topic string, pid int32, command *protocol.PartitionState) error {
	p, err := rm.Partition(topic, pid)
	if err != nil {
		return err
	}
	if r, ok := rm.replicators[p]; ok {
		if err := r.close(); err != nil {
			return err
		}
	}
	p.Leader = rm.ID()
	p.ISR = command.ISR
	p.LeaderAndISRVersionInZK = command.ZKVersion
	return nil
}
