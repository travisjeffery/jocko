package protocol

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestFetchResponse(t *testing.T) {
	req := require.New(t)
	exp := &FetchResponses{
		ThrottleTimeMs: 1,
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
	err = Decode(b, &act)
	req.NoError(err)
	req.Equal(exp, &act)
}
