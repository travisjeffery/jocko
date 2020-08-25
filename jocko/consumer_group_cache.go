package jocko

import (
	"sync"

	"github.com/travisjeffery/jocko/jocko/structs"
)

type GroupMetadataCache interface {
	GetOrCreateGroup(string, int32) *structs.Group
	GetGroup(string) *structs.Group
}
type groupMetadataCache struct {
	sync.Mutex
	cache map[string]*structs.Group
}

func NewGroupMetadataCache() GroupMetadataCache {
	return &groupMetadataCache{
		cache: make(map[string]*structs.Group),
	}
}
func (gmc *groupMetadataCache) GetOrCreateGroup(groupId string, coordinatorId int32) *structs.Group {
	gmc.Lock()
	defer gmc.Unlock()
	if res, ok := gmc.cache[groupId]; ok {
		return res
	}
	res := structs.NewGroup(groupId, coordinatorId)
	gmc.cache[groupId] = res
	return res
}

func (gmc *groupMetadataCache) GetGroup(groupId string) *structs.Group {
	gmc.Lock()
	defer gmc.Unlock()
	if res, ok := gmc.cache[groupId]; ok {
		return res
	}
	return nil
}
