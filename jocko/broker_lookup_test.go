package jocko

import (
	"testing"

	"github.com/hashicorp/raft"
	"github.com/stretchr/testify/require"
	"github.com/travisjeffery/jocko/jocko/metadata"
)

func TestNewBrokerLookup(t *testing.T) {
	lookup := NewBrokerLookup()
	addr := "10.0.0.1:9092"
	id := metadata.NodeID(1)
	svr := &metadata.Broker{ID: id, RaftAddr: addr}

	lookup.AddBroker(svr)
	got, err := lookup.BrokerAddr(raft.ServerID(id.String()))
	require.NoError(t, err)
	require.Equal(t, raft.ServerAddress(addr), got)

	broker := lookup.BrokerByAddr(raft.ServerAddress(addr))
	require.NotNil(t, broker)
	require.Equal(t, addr, broker.RaftAddr)

	broker = lookup.BrokerByID(raft.ServerID(id.String()))
	require.NotNil(t, broker)
	require.Equal(t, addr, broker.RaftAddr)

	require.Equal(t, 1, len(lookup.Brokers()))

	lookup.RemoveBroker(svr)

	got, err = lookup.BrokerAddr(raft.ServerID(id.String()))
	require.Error(t, err)
	require.Equal(t, raft.ServerAddress(""), got)

	require.Equal(t, 0, len(lookup.Brokers()))
}
