package commitlog

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMessageSet(t *testing.T) {
	msg0 := NewMessage([]byte("hello"))
	msg1 := NewMessage([]byte("world"))
	msgs := []Message{
		msg0,
		msg1,
	}
	ms := NewMessageSet(3, msgs...)
	require.Equal(t, int64(3), ms.Offset())

	payload := ms.Payload()
	var offset int
	for _, msg := range msgs {
		require.Equal(t, []byte(msg), payload[offset:offset+len(msg)])
		offset += len(msg)
	}
}

func TestMessages(t *testing.T) {
	req := require.New(t)
	ms := NewMessageSet(1, emptyMessage, emptyV1Message)
	msgs := ms.Messages()
	req.Equal(2, len(msgs))
	req.Equal(Message(emptyMessage), msgs[0])
	req.Equal(Message(emptyV1Message), msgs[1])
}
