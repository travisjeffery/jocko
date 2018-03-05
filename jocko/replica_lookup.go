package jocko

import (
	"fmt"
	"sync"
)

type replicaLookup struct {
	lock sync.RWMutex
	// topic to partition id to replica id to replica
	replica map[string]map[int32]*Replica
}

func NewReplicaLookup() *replicaLookup {
	return &replicaLookup{
		replica: make(map[string]map[int32]*Replica),
	}
}

func (rl *replicaLookup) AddReplica(replica *Replica) {
	rl.lock.Lock()
	defer rl.lock.Unlock()
ADD:
	if t, ok := rl.replica[replica.Partition.Topic]; ok {
		t[replica.Partition.ID] = replica
	} else {
		rl.replica[replica.Partition.Topic] = make(map[int32]*Replica)
		goto ADD
	}
}

func (rl *replicaLookup) Replica(topic string, partition int32) (*Replica, error) {
	rl.lock.RLock()
	defer rl.lock.RUnlock()
	r, ok := rl.replica[topic][partition]
	if !ok {
		return nil, fmt.Errorf("no replica for topic %s partition %d", topic, partition)
	}
	return r, nil
}

func (rl *replicaLookup) RemoveReplica(replica *Replica) {
	rl.lock.Lock()
	defer rl.lock.Unlock()
	delete(rl.replica[replica.Partition.Topic], replica.Partition.ID)
}
