package testutil

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/travisjeffery/jocko/raft"
)

// NewTestRaft creates a new (non-bootstrapped) Raft instance.
// An entry will be added to the TempDirList on success. It is the user's
// responsibility to call Cleanup() on the TempDirList after use.
func NewTestRaft(t *testing.T, l *TempDirList, opts ...raft.OptionFn) *raft.Raft {
	opts = append(opts,
		raft.Logger(NewTestLogger()),
		raft.DataDir(l.NewTempDir(t)),
		raft.Addr(NewTestAddr(t)),
	)

	s, err := raft.New(opts...)
	assert.NoError(t, err)
	return s
}
