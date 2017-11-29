package commitlog_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/travisjeffery/jocko/commitlog"
)

func TestMessageSet(t *testing.T) {
	msg0 := commitlog.NewMessage([]byte("hello"))
	msg1 := commitlog.NewMessage([]byte("world"))
	msgs := []commitlog.Message{
		msg0,
		msg1,
	}
	ms := commitlog.NewMessageSet(3, msgs...)
	require.Equal(t, int64(3), ms.Offset())

	payload := ms.Payload()
	var offset int
	for _, msg := range msgs {
		require.Equal(t, []byte(msg), payload[offset:offset+len(msg)])
		offset += len(msg)
	}
}
