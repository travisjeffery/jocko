package commitlog

import (
	"testing"

	"github.com/bmizerany/assert"
)

func TestMessageSet(t *testing.T) {
	msg := NewMessage([]byte("hello"))
	msgs := []Message{
		msg,
	}
	ms := NewMessageSet(msgs, 3)
	assert.Equal(t, int64(3), ms.Offset())
	assert.Equal(t, msg.Size(), ms.Size())
}
