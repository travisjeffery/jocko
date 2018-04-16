package protocol

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestAlterConfigsRequest(t *testing.T) {
	req := require.New(t)
	val := "max"
	exp := &AlterConfigsRequest{
		Resources: []ConfigResource{
			{
				Type: 1,
				Name: "system",
				Entries: []ConfigEntry{{
					Name:  "memory",
					Value: &val,
				}},
			},
		},
	}
	b, err := Encode(exp)
	req.NoError(err)
	var act AlterConfigsRequest
	err = Decode(b, &act, exp.Version())
	req.NoError(err)
	req.Equal(exp, &act)
}
