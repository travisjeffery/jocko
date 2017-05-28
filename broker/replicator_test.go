package broker_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/travisjeffery/jocko"
	"github.com/travisjeffery/jocko/broker"
	"github.com/travisjeffery/jocko/testutil"
	"github.com/travisjeffery/jocko/testutil/mocks"
)

func TestBroker_Replicate(t *testing.T) {
	clog := mocks.NewCommitLog()
	proxy := mocks.NewProxy(4)

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
		broker.ReplicatorProxy(proxy))

	testutil.WaitForResult(func() (bool, error) {
		commitLog := clog.Log()
		if len(commitLog) < 4 {
			return false, nil
		}
		assert.Equal(t, commitLog, proxy.Messages(), "unmatched replicated messages")
		return true, nil
	}, func(err error) {
		t.Fatalf("err: %v", err)
	})

	assert.NoError(t, replicator.Close())
}
