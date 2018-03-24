package protocol

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestFindCoordinatorResponse(t *testing.T) {
	req := require.New(t)
	errMsg := "Shit's broken"
	exp := &FindCoordinatorResponse{
		ThrottleTimeMs: 1,
		ErrorCode:      2,
		ErrorMessage:   &errMsg,
		Coordinator: Coordinator{
			NodeID: 3,
			Host:   "localhost",
			Port:   4,
		},
	}
	b, err := Encode(exp)
	req.NoError(err)
	var act FindCoordinatorResponse
	err = Decode(b, &act)
	req.NoError(err)
	req.Equal(exp, &act)
}
