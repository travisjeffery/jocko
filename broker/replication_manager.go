package broker

import (
	"github.com/travisjeffery/jocko"
	"github.com/travisjeffery/jocko/protocol"
	"github.com/travisjeffery/jocko/server"
)

type replicationManager struct {
	jocko.Broker
	replicators map[*jocko.Partition]*Replicator
}

func newReplicationManager() *replicationManager {
	return &replicationManager{
		replicators: make(map[*jocko.Partition]*Replicator),
	}
}

func (rm *replicationManager) BecomeFollower(topic string, pid int32, command *protocol.PartitionState) protocol.Error {
	p, err := rm.Partition(topic, pid)
	if err != protocol.ErrNone {
		return err
	}
	// stop replicator to current leader
	if r, ok := rm.replicators[p]; ok {
		if err := r.Close(); err != nil {
			return protocol.ErrUnknown.WithErr(err)
		}
	}
	delete(rm.replicators, p)
	hw := p.HighWatermark()
	if err := p.TruncateTo(hw); err != nil {
		return protocol.ErrUnknown.WithErr(err)
	}
	p.Leader = command.Leader
	p.Conn = rm.ClusterMember(p.LeaderID())
	r := NewReplicator(p, rm.ID(),
		ReplicatorLeader(server.NewClient(p.Conn)))
	rm.replicators[p] = r
	return protocol.ErrNone
}

func (rm *replicationManager) BecomeLeader(topic string, pid int32, command *protocol.PartitionState) protocol.Error {
	p, err := rm.Partition(topic, pid)
	if err != protocol.ErrNone {
		return err
	}
	if r, ok := rm.replicators[p]; ok {
		if err := r.Close(); err != nil {
			return protocol.ErrUnknown.WithErr(err)
		}
	}
	p.Leader = rm.ID()
	p.Conn = rm.ClusterMember(p.LeaderID())
	p.ISR = command.ISR
	p.LeaderAndISRVersionInZK = command.ZKVersion
	return protocol.ErrNone
}
