package raft_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/travisjeffery/jocko/raft"
	"github.com/travisjeffery/jocko/testutil"
)

func TestNew(t *testing.T) {
	l := testutil.NewTempDirList()
	defer l.Cleanup()

	t.Run("returns Raft instance", func(t *testing.T) {
		assert.IsType(t, &raft.Raft{}, testutil.NewTestRaft(t, l))
	})

	t.Run("yields instance to OptionFns", func(t *testing.T) {
		opt1 := func(r *raft.Raft) {
			assert.IsType(t, &raft.Raft{}, r)
		}

		opt2 := func(r *raft.Raft) {
			assert.IsType(t, &raft.Raft{}, r)
		}

		testutil.NewTestRaft(t, l, opt1, opt2)
	})
}
