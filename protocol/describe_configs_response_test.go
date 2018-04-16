package protocol

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDescribeConfigsResponse(t *testing.T) {
	req := require.New(t)
	code := ErrBrokerNotAvailable.Code()
	msg := ErrBrokerNotAvailable.String()
	exp := &DescribeConfigsResponse{
		Resources: []DescribeConfigsResourceResponse{
			{
				ErrorCode:     code,
				ErrorMessage:  &msg,
				Type:          2,
				Name:          "resource",
				ConfigEntries: []DescribeConfigsEntry{},
			},
		},
	}
	b, err := Encode(exp)
	req.NoError(err)
	var act DescribeConfigsResponse
	err = Decode(b, &act, exp.Version())
	req.NoError(err)
	req.Equal(exp, &act)
}
