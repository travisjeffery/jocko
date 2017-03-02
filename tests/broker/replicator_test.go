package tests

import (
	"testing"
	"time"

	"github.com/hashicorp/nomad/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/travisjeffery/jocko"
	"github.com/travisjeffery/jocko/broker"
	"github.com/travisjeffery/jocko/testutil/mocks"
)

func TestBroker_Replicate(t *testing.T) {
	mockCommitLog := mocks.NewCommitLog()
	mockProxy := mocks.NewProxyForFetchMessages(4)

	p := &jocko.Partition{
		Topic:           "test",
		ID:              0,
		Leader:          0,
		PreferredLeader: 0,
		Replicas:        []int32{0},
		CommitLog:       mockCommitLog,
	}

	replicator := broker.NewReplicator(p, 0,
		broker.ReplicatorMinBytes(5),
		broker.ReplicatorMaxWaitTime(int32(250*time.Millisecond)),
		broker.ReplicatorProxy(mockProxy))

	testutil.WaitForResult(func() (bool, error) {
		commitLog := mockCommitLog.Log()
		if len(commitLog) < 4 {
			return false, nil
		}
		assert.Equal(t, commitLog, mockProxy.MockedMessages(), "unmatched replicated messages")
		return true, nil
	}, func(err error) {
		t.Fatalf("err: %v", err)
	})

	assert.NoError(t, replicator.Close())
}
