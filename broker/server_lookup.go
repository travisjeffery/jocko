package broker

import (
	"fmt"
	"sync"

	"github.com/hashicorp/raft"
)

type serverLookup struct {
	lock            sync.RWMutex
	addressToServer map[raft.ServerAddress]*broker
	idToServer      map[raft.ServerID]*broker
}

func NewServerLookup() *serverLookup {
	return &serverLookup{
		addressToServer: make(map[raft.ServerAddress]*broker),
		idToServer:      make(map[raft.ServerID]*broker),
	}
}

func (sl *serverLookup) AddServer(server *broker) {
	sl.lock.Lock()
	defer sl.lock.Unlock()
	sl.addressToServer[raft.ServerAddress(server.Addr.String())] = server
	sl.idToServer[raft.ServerID(server.ID)] = server
}

func (sl *serverLookup) Server(addr raft.ServerAddress) *broker {
	sl.lock.RLock()
	defer sl.lock.RUnlock()
	svr, _ := sl.addressToServer[addr]
	return svr
}

func (sl *serverLookup) ServerAddr(id raft.ServerID) (raft.ServerAddress, error) {
	sl.lock.RLock()
	defer sl.lock.RUnlock()
	svr, ok := sl.idToServer[id]
	if !ok {
		return "", fmt.Errorf("no server for id %v", id)
	}
	return raft.ServerAddress(svr.Addr.String()), nil
}

func (sl *serverLookup) RemoveServer(server *broker) {
	sl.lock.Lock()
	defer sl.lock.Unlock()
	delete(sl.addressToServer, raft.ServerAddress(server.Addr.String()))
	delete(sl.idToServer, raft.ServerID(server.ID))
}

func (sl *serverLookup) Servers() []*broker {
	sl.lock.RLock()
	sl.lock.RUnlock()
	var ret []*broker
	for _, svr := range sl.addressToServer {
		ret = append(ret, svr)
	}
	return ret
}
