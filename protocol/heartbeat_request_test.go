package protocol

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestHeartbeatRequest(t *testing.T) {
	req := require.New(t)
	exp := &HeartbeatRequest{
		GroupID:           "group",
		GroupGenerationID: 1,
		MemberID:          "member",
	}
	b, err := Encode(exp)
	req.NoError(err)
	var act HeartbeatRequest
	err = Decode(b, &act)
	req.NoError(err)
	req.Equal(exp, &act)
}
