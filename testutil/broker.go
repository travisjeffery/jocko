package testutil

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/travisjeffery/jocko/broker"
)

// NewTestBroker creates a new (non-bootstrapped) Broker instance.
// An entry will be added to TempDirList on success. It is the user's
// responsibility to call Cleanup() on the TempDirList after use.
func NewTestBroker(t *testing.T, id int32, l *TempDirList, opts ...broker.BrokerFn) *broker.Broker {
	opts = append(opts,
		broker.LogDir(l.NewTempDir(t)),
		broker.Addr(NewTestAddr(t)),
		broker.Logger(NewTestLogger()),
		broker.Serf(NewTestSerf(t)),
		broker.Raft(NewTestRaft(t, l)),
	)

	b, err := broker.New(id, opts...)
	assert.NoError(t, err)
	return b
}
