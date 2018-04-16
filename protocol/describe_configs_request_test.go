package protocol

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDescribeConfigsRequest(t *testing.T) {
	req := require.New(t)
	exp := &DescribeConfigsRequest{
		Resources: []DescribeConfigsResource{
			{
				Type:        1,
				Name:        "system",
				ConfigNames: []string{"memory"},
			},
		},
	}
	b, err := Encode(exp)
	req.NoError(err)
	var act DescribeConfigsRequest
	err = Decode(b, &act, exp.Version())
	req.NoError(err)
	req.Equal(exp, &act)
}
