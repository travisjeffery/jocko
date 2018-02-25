package jocko

import (
	"fmt"
	"sync"

	"github.com/hashicorp/raft"
	"github.com/travisjeffery/jocko/jocko/metadata"
)

type brokerLookup struct {
	lock            sync.RWMutex
	addressToBroker map[raft.ServerAddress]*metadata.Broker
	idToBroker      map[raft.ServerID]*metadata.Broker
}

func NewBrokerLookup() *brokerLookup {
	return &brokerLookup{
		addressToBroker: make(map[raft.ServerAddress]*metadata.Broker),
		idToBroker:      make(map[raft.ServerID]*metadata.Broker),
	}
}

func (sl *brokerLookup) AddBroker(broker *metadata.Broker) {
	sl.lock.Lock()
	defer sl.lock.Unlock()
	sl.addressToBroker[raft.ServerAddress(broker.RaftAddr)] = broker
	sl.idToBroker[raft.ServerID(broker.ID)] = broker
}

func (sl *brokerLookup) BrokerByAddr(addr raft.ServerAddress) *metadata.Broker {
	sl.lock.RLock()
	defer sl.lock.RUnlock()
	svr, _ := sl.addressToBroker[addr]
	return svr
}

func (sl *brokerLookup) BrokerByID(id raft.ServerID) *metadata.Broker {
	sl.lock.RLock()
	defer sl.lock.RUnlock()
	svr, _ := sl.idToBroker[id]
	return svr
}

func (sl *brokerLookup) BrokerAddr(id raft.ServerID) (raft.ServerAddress, error) {
	sl.lock.RLock()
	defer sl.lock.RUnlock()
	svr, ok := sl.idToBroker[id]
	if !ok {
		return "", fmt.Errorf("no broker for id %v", id)
	}
	return raft.ServerAddress(svr.RaftAddr), nil
}

func (sl *brokerLookup) RemoveBroker(broker *metadata.Broker) {
	sl.lock.Lock()
	defer sl.lock.Unlock()
	delete(sl.addressToBroker, raft.ServerAddress(broker.RaftAddr))
	delete(sl.idToBroker, raft.ServerID(broker.ID))
}

func (sl *brokerLookup) Brokers() []*metadata.Broker {
	sl.lock.RLock()
	defer sl.lock.RUnlock()
	var ret []*metadata.Broker
	for _, svr := range sl.addressToBroker {
		ret = append(ret, svr)
	}
	return ret
}
