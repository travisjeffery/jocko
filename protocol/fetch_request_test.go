package protocol

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestFetchRequest(t *testing.T) {
	req := require.New(t)
	exp := &FetchRequest{
		ReplicaID:   1,
		MaxWaitTime: time.Millisecond,
		MinBytes:    3,
		Topics: []*FetchTopic{{
			Topic: "test_topic",
			Partitions: []*FetchPartition{{
				Partition:   1,
				FetchOffset: 2,
				MaxBytes:    3,
			}},
		}},
	}
	b, err := Encode(exp)
	req.NoError(err)
	var act FetchRequest
	err = Decode(b, &act, exp.Version())
	req.NoError(err)
	req.Equal(exp, &act)
}
