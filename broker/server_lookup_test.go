package broker

import (
	"testing"

	"github.com/hashicorp/raft"
	"github.com/stretchr/testify/require"
)

type testAddr struct {
	addr string
}

func (t *testAddr) Network() string {
	return "tcp"
}

func (t *testAddr) String() string {
	return t.addr
}

func TestNewServerLookup(t *testing.T) {
	lookup := NewServerLookup()
	addr := "10.0.0.1:9092"
	id := int32(1)
	svr := &broker{ID: id, Addr: &testAddr{addr}}

	lookup.AddServer(svr)
	got, err := lookup.ServerAddr(raft.ServerID(id))
	require.NoError(t, err)
	require.Equal(t, raft.ServerAddress(addr), got)

	server := lookup.Server(raft.ServerAddress(addr))
	require.NotNil(t, server)
	require.Equal(t, addr, server.Addr.String())

	lookup.RemoveServer(svr)

	got, err = lookup.ServerAddr(raft.ServerID(id))
	require.Error(t, err)
	require.Equal(t, raft.ServerAddress(""), got)
}
