package commitlog

import (
	"testing"

	"github.com/bmizerany/assert"
)

func TestMessageSet(t *testing.T) {
	msg0 := NewMessage([]byte("hello"))
	msg1 := NewMessage([]byte("world"))
	msgs := []Message{
		msg0,
		msg1,
	}
	ms := NewMessageSet(3, msgs...)
	assert.Equal(t, int64(3), ms.Offset())

	payload := ms.Payload()
	var offset int
	for _, msg := range msgs {
		assert.Equal(t, []byte(msg), payload[offset:offset+len(msg)])
		offset += len(msg)
	}
}
