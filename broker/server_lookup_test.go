package broker

import (
	"testing"

	"github.com/hashicorp/raft"
	"github.com/stretchr/testify/require"
	"github.com/travisjeffery/jocko/broker/metadata"
)

func TestNewServerLookup(t *testing.T) {
	lookup := NewServerLookup()
	addr := "10.0.0.1:9092"
	id := int32(1)
	svr := &metadata.Broker{ID: id, RaftAddr: addr}

	lookup.AddServer(svr)
	got, err := lookup.ServerAddr(raft.ServerID(id))
	require.NoError(t, err)
	require.Equal(t, raft.ServerAddress(addr), got)

	server := lookup.ServerByAddr(raft.ServerAddress(addr))
	require.NotNil(t, server)
	require.Equal(t, addr, server.RaftAddr)

	server = lookup.ServerByID(raft.ServerID(id))
	require.NotNil(t, server)
	require.Equal(t, addr, server.RaftAddr)

	require.Equal(t, 1, len(lookup.Servers()))

	lookup.RemoveServer(svr)

	got, err = lookup.ServerAddr(raft.ServerID(id))
	require.Error(t, err)
	require.Equal(t, raft.ServerAddress(""), got)

	require.Equal(t, 0, len(lookup.Servers()))
}
