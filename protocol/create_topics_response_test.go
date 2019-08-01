package protocol

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCreateTopicResponse(t *testing.T) {
	req := require.New(t)
	exp := &CreateTopicsResponse{
		TopicErrorCodes: []*TopicErrorCode{{
			Topic:     "test",
			ErrorCode: ErrCorruptMessage,
		}}}
	b, err := Encode(exp)
	req.NoError(err)
	var act CreateTopicsResponse
	err = Decode(b, &act, exp.Version())
	req.NoError(err)
	req.Equal(exp, &act)
}
