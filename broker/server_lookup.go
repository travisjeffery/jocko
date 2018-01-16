package broker

import (
	"fmt"
	"sync"

	"github.com/hashicorp/raft"
	"github.com/travisjeffery/jocko/broker/metadata"
)

type serverLookup struct {
	lock            sync.RWMutex
	addressToServer map[raft.ServerAddress]*metadata.Broker
	idToServer      map[raft.ServerID]*metadata.Broker
}

func NewServerLookup() *serverLookup {
	return &serverLookup{
		addressToServer: make(map[raft.ServerAddress]*metadata.Broker),
		idToServer:      make(map[raft.ServerID]*metadata.Broker),
	}
}

func (sl *serverLookup) AddServer(server *metadata.Broker) {
	sl.lock.Lock()
	defer sl.lock.Unlock()
	sl.addressToServer[raft.ServerAddress(server.RaftAddr)] = server
	sl.idToServer[raft.ServerID(server.ID)] = server
}

func (sl *serverLookup) ServerByAddr(addr raft.ServerAddress) *metadata.Broker {
	sl.lock.RLock()
	defer sl.lock.RUnlock()
	svr, _ := sl.addressToServer[addr]
	return svr
}

func (sl *serverLookup) ServerByID(id raft.ServerID) *metadata.Broker {
	sl.lock.RLock()
	defer sl.lock.RUnlock()
	svr, _ := sl.idToServer[id]
	return svr
}

func (sl *serverLookup) ServerAddr(id raft.ServerID) (raft.ServerAddress, error) {
	sl.lock.RLock()
	defer sl.lock.RUnlock()
	svr, ok := sl.idToServer[id]
	if !ok {
		return "", fmt.Errorf("no server for id %v", id)
	}
	return raft.ServerAddress(svr.RaftAddr), nil
}

func (sl *serverLookup) RemoveServer(server *metadata.Broker) {
	sl.lock.Lock()
	defer sl.lock.Unlock()
	delete(sl.addressToServer, raft.ServerAddress(server.RaftAddr))
	delete(sl.idToServer, raft.ServerID(server.ID))
}

func (sl *serverLookup) Servers() []*metadata.Broker {
	sl.lock.RLock()
	sl.lock.RUnlock()
	var ret []*metadata.Broker
	for _, svr := range sl.addressToServer {
		ret = append(ret, svr)
	}
	return ret
}
