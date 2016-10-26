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
	ms := NewMessageSet(msgs, 3)
	assert.Equal(t, int64(3), ms.Offset())
	assert.Equal(t, msg0.Size()+msg1.Size()+msgSetHeaderLen, ms.Size())
	for i, m := range ms.Messages() {
		assert.Equal(t, msgs[i], m)
	}
}
