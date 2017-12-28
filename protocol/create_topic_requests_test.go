package protocol

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCreateTopicRequests(t *testing.T) {
	req := require.New(t)
	exp := &CreateTopicRequests{Requests: []*CreateTopicRequest{{
		Topic:             "test",
		NumPartitions:     99,
		ReplicationFactor: 3,
		ReplicaAssignment: map[int32][]int32{
			1: []int32{2, 3, 4},
		},
		Configs: map[string]string{"config_key": "config_val"},
	}}}
	b, err := Encode(exp)
	req.NoError(err)
	var act CreateTopicRequests
	err = Decode(b, &act)
	req.NoError(err)
	req.Equal(exp, &act)
}
