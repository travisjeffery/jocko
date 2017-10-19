package broker_test

import (
	"bytes"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/travisjeffery/jocko"
	"github.com/travisjeffery/jocko/broker"
	"github.com/travisjeffery/jocko/testutil"
	"github.com/travisjeffery/jocko/testutil/mock"
)

func TestBroker_Replicate(t *testing.T) {
	clog := mock.NewCommitLog()
	leader := mock.NewClient(4)

	p := &jocko.Partition{
		Topic:           "test",
		ID:              0,
		Leader:          0,
		PreferredLeader: 0,
		Replicas:        []int32{0},
		CommitLog:       clog,
	}

	replicator := broker.NewReplicator(p, 0,
		broker.ReplicatorMinBytes(5),
		broker.ReplicatorMaxWaitTime(int32(250*time.Millisecond)),
		broker.ReplicatorLeader(leader))

	testutil.WaitForResult(func() (bool, error) {
		commitLog := clog.Log()
		if len(commitLog) < 4 {
			return false, nil
		}
		for i, m := range leader.Messages() {
			assert.True(t, bytes.Equal(m, commitLog[i]))
		}
		return true, nil
	}, func(err error) {
		t.Fatalf("err: %v", err)
	})

	assert.NoError(t, replicator.Close())
}
