package protocol

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestLeaderAndISRRequest(t *testing.T) {
	req := require.New(t)
	exp := &LeaderAndISRRequest{
		ControllerID:    1,
		ControllerEpoch: 2,
		PartitionStates: []*PartitionState{{
			Topic:           "test_topic",
			Partition:       1,
			ControllerEpoch: 2,
			Leader:          3,
			LeaderEpoch:     4,
			ISR:             []int32{1, 2},
			ZKVersion:       3,
			Replicas:        []int32{1, 2},
		}},
		LiveLeaders: []*LiveLeader{{
			ID:   1,
			Host: "localhost",
			Port: 9092,
		}},
	}
	b, err := Encode(exp)
	req.NoError(err)
	var act LeaderAndISRRequest
	err = Decode(b, &act, exp.Version())
	req.NoError(err)
	req.Equal(exp, &act)
}
