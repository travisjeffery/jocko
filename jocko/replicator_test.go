package jocko_test

import (
	"bytes"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/travisjeffery/jocko/jocko"
	"github.com/travisjeffery/jocko/jocko/structs"
	"github.com/travisjeffery/jocko/mock"
	"github.com/travisjeffery/jocko/protocol"
	"github.com/travisjeffery/jocko/testutil"
)

func TestBroker_Replicate(t *testing.T) {
	c := newCommitLog()
	l := mock.NewClient(4)

	replica := &jocko.Replica{
		Partition: structs.Partition{
			Topic:  "test",
			ID:     0,
			Leader: 0,
			AR:     []int32{0},
		},
		BrokerID: 0,
		Log:      c,
	}

	replicator := jocko.NewReplicator(jocko.ReplicatorConfig{
		MinBytes:    5,
		MaxWaitTime: 250 * time.Millisecond,
	}, replica, l)
	replicator.Replicate()

	testutil.WaitForResult(func() (bool, error) {
		commitLog := c.Log()
		if len(commitLog) < 4 {
			return false, nil
		}
		for i, m := range l.Messages() {
			require.True(t, bytes.Equal(m, commitLog[i]))
		}
		return true, nil
	}, func(err error) {
		t.Fatalf("err: %v", err)
	})

	require.NoError(t, replicator.Close())
}

func TestReplicatorReplicatesOffsetZero(t *testing.T) {
	c := newCommitLog()
	l := &replicatorClient{
		fetch: func(req *protocol.FetchRequest) (*protocol.FetchResponse, error) {
			if req.Topics[0].Partitions[0].FetchOffset > 0 {
				return emptyFetchResponse(req), nil
			}
			return fetchResponse(req, mustMessageSet(t, 0, "first"), 1), nil
		},
	}
	replicator := jocko.NewReplicator(jocko.ReplicatorConfig{
		MinBytes:    1,
		MaxWaitTime: 25 * time.Millisecond,
	}, testReplica(c), l)
	replicator.Replicate()
	defer replicator.Close()

	testutil.WaitForResult(func() (bool, error) {
		return len(c.Log()) == 1, nil
	}, func(err error) {
		t.Fatalf("err: %v", err)
	})
	require.Equal(t, int64(0), l.fetchOffsets[0])
}

func TestReplicatorIgnoresEmptyFetchResponses(t *testing.T) {
	c := newCommitLog()
	l := &replicatorClient{
		fetch: func(req *protocol.FetchRequest) (*protocol.FetchResponse, error) {
			return emptyFetchResponse(req), nil
		},
	}
	replicator := jocko.NewReplicator(jocko.ReplicatorConfig{
		MinBytes:    1,
		MaxWaitTime: 25 * time.Millisecond,
	}, testReplica(c), l)
	replicator.Replicate()
	defer replicator.Close()

	time.Sleep(100 * time.Millisecond)
	require.Empty(t, c.Log())
}

func TestReplicatorAdvancesFetchOffsetAfterAppend(t *testing.T) {
	c := newCommitLog()
	messages := map[int64][]byte{
		0: mustMessageSet(t, 0, "first"),
		1: mustMessageSet(t, 1, "second"),
	}
	l := &replicatorClient{
		fetch: func(req *protocol.FetchRequest) (*protocol.FetchResponse, error) {
			offset := req.Topics[0].Partitions[0].FetchOffset
			recordSet, ok := messages[offset]
			if !ok {
				return emptyFetchResponse(req), nil
			}
			return fetchResponse(req, recordSet, offset+1), nil
		},
	}
	replicator := jocko.NewReplicator(jocko.ReplicatorConfig{
		MinBytes:    1,
		MaxWaitTime: 25 * time.Millisecond,
	}, testReplica(c), l)
	replicator.Replicate()
	defer replicator.Close()

	testutil.WaitForResult(func() (bool, error) {
		return len(c.Log()) == 2, nil
	}, func(err error) {
		t.Fatalf("err: %v", err)
	})
	require.Contains(t, l.fetchOffsets, int64(0))
	require.Contains(t, l.fetchOffsets, int64(1))
}

func TestReplicatorSplitsFetchedRecordSetEntries(t *testing.T) {
	c := newCommitLog()
	l := &replicatorClient{
		fetch: func(req *protocol.FetchRequest) (*protocol.FetchResponse, error) {
			if req.Topics[0].Partitions[0].FetchOffset > 0 {
				return emptyFetchResponse(req), nil
			}
			recordSet := bytes.Join([][]byte{
				mustMessageSet(t, 0, "first"),
				mustMessageSet(t, 1, "second"),
			}, nil)
			return fetchResponse(req, recordSet, 2), nil
		},
	}
	replicator := jocko.NewReplicator(jocko.ReplicatorConfig{
		MinBytes:    1,
		MaxWaitTime: 25 * time.Millisecond,
	}, testReplica(c), l)
	replicator.Replicate()
	defer replicator.Close()

	testutil.WaitForResult(func() (bool, error) {
		return len(c.Log()) == 2, nil
	}, func(err error) {
		t.Fatalf("err: %v", err)
	})
	require.Contains(t, l.fetchOffsets, int64(0))
	require.Contains(t, l.fetchOffsets, int64(2))
}

type replicatorClient struct {
	fetch        func(req *protocol.FetchRequest) (*protocol.FetchResponse, error)
	fetchOffsets []int64
}

func (c *replicatorClient) Fetch(req *protocol.FetchRequest) (*protocol.FetchResponse, error) {
	c.fetchOffsets = append(c.fetchOffsets, req.Topics[0].Partitions[0].FetchOffset)
	return c.fetch(req)
}

func (c *replicatorClient) CreateTopics(req *protocol.CreateTopicRequests) (*protocol.CreateTopicsResponse, error) {
	return nil, nil
}

func (c *replicatorClient) LeaderAndISR(req *protocol.LeaderAndISRRequest) (*protocol.LeaderAndISRResponse, error) {
	return nil, nil
}

func testReplica(c jocko.CommitLog) *jocko.Replica {
	return &jocko.Replica{
		Partition: structs.Partition{
			Topic:  "test",
			ID:     0,
			Leader: 1,
			AR:     []int32{1, 2},
			ISR:    []int32{1, 2},
		},
		BrokerID: 2,
		Log:      c,
	}
}

func fetchResponse(req *protocol.FetchRequest, recordSet []byte, highWatermark int64) *protocol.FetchResponse {
	return &protocol.FetchResponse{
		Responses: protocol.FetchTopicResponses{{
			Topic: req.Topics[0].Topic,
			PartitionResponses: []*protocol.FetchPartitionResponse{{
				Partition:     req.Topics[0].Partitions[0].Partition,
				ErrorCode:     protocol.ErrNone.Code(),
				HighWatermark: highWatermark,
				RecordSet:     recordSet,
			}},
		}},
	}
}

func emptyFetchResponse(req *protocol.FetchRequest) *protocol.FetchResponse {
	return fetchResponse(req, []byte{}, 0)
}

func mustMessageSet(t *testing.T, offset int64, value string) []byte {
	t.Helper()
	b, err := protocol.Encode(&protocol.MessageSet{
		Offset:   offset,
		Messages: []*protocol.Message{{Value: []byte(value)}},
	})
	require.NoError(t, err)
	return b
}

type commitLog struct {
	*mock.CommitLog
	sync.RWMutex
	b [][]byte
}

func (c *commitLog) Log() [][]byte {
	log := [][]byte{}
	c.RLock()
	log = append(log, c.b...)
	c.RUnlock()
	return log
}

func newCommitLog() *commitLog {
	c := &commitLog{}
	c.CommitLog = &mock.CommitLog{
		AppendFunc: func(b []byte) (int64, error) {
			c.Lock()
			c.b = append(c.b, b)
			offset := int64(len(c.b) - 1)
			c.Unlock()
			return offset, nil
		},
		DeleteFunc: func() error {
			return nil
		},
		NewReaderFunc: func(offset int64, maxBytes int32) (io.Reader, error) {
			return nil, nil
		},
		TruncateFunc: func(int64) error {
			return nil
		},

		NewestOffsetFunc: func() int64 {
			c.RLock()
			defer c.RUnlock()
			return int64(len(c.b))
		},

		OldestOffsetFunc: func() int64 {
			return 0
		},
	}
	return c
}
