package protocol

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestLeaderAndISRResponse(t *testing.T) {
	req := require.New(t)
	exp := &LeaderAndISRResponse{
		ErrorCode: 3,
		Partitions: []*LeaderAndISRPartition{{
			Topic:     "test_topic_1",
			Partition: 1,
			ErrorCode: 4,
		}, {
			Topic:     "test_topic_2",
			Partition: 3,
			ErrorCode: 5,
		}},
	}
	b, err := Encode(exp)
	req.NoError(err)
	var act LeaderAndISRResponse
	err = Decode(b, &act, exp.Version())
	req.NoError(err)
	req.Equal(exp, &act)
}
