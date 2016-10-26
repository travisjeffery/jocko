package commitlog

import (
	"testing"

	"github.com/bmizerany/assert"
)

func TestMessage(t *testing.T) {
	msg := NewMessage([]byte("hello"))
	assert.Equal(t, int32(5), msg.PLen())
	assert.Equal(t, int32(9+5), msg.Size())
}
