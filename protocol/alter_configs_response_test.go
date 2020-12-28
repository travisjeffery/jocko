package protocol

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestAlterConfigsResponse(t *testing.T) {
	req := require.New(t)
	code := ErrBrokerNotAvailable
	msg := ErrBrokerNotAvailable.Error()
	exp := &AlterConfigsResponse{
		Resources: []AlterConfigResourceResponse{
			{
				ErrorCode:    code,
				ErrorMessage: &msg,
				Type:         2,
				Name:         "resource",
			},
		},
	}
	b, err := Encode(exp)
	req.NoError(err)
	var act AlterConfigsResponse
	err = Decode(b, &act, exp.Version())
	req.NoError(err)
	req.Equal(exp, &act)
}
