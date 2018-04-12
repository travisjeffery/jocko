package protocol

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestFetchResponse(t *testing.T) {
	req := require.New(t)
	exp := &FetchResponses{
		APIVersion:   2,
		ThrottleTime: time.Millisecond,
		Responses: []*FetchResponse{{
			Topic: "test_topic",
			PartitionResponses: []*FetchPartitionResponse{{
				Partition:     1,
				ErrorCode:     ErrReplicaNotAvailable.Code(),
				HighWatermark: 2,
				RecordSet:     []byte("sup"),
			}},
		}},
	}
	b, err := Encode(exp)
	req.NoError(err)
	var act FetchResponses
	err = Decode(b, &act, exp.Version())
	req.NoError(err)
	req.Equal(exp, &act)
}
