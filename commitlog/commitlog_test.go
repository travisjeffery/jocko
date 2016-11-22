package commitlog

import (
	"bytes"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewCommitLog(t *testing.T) {
	path := filepath.Join(os.TempDir(), fmt.Sprintf("commitlogtest%d", rand.Int63()))
	os.RemoveAll(path)
	os.MkdirAll(path, 0755)

	opts := Options{
		Path:         path,
		SegmentBytes: 6,
	}
	l, err := New(opts)
	assert.NoError(t, err)

	// remove old data
	assert.NoError(t, l.DeleteAll())

	assert.NoError(t, l.Init())
	assert.NoError(t, l.Open())

	msgs := []Message{
		NewMessage([]byte("one")),
		NewMessage([]byte("two")),
		NewMessage([]byte("three")),
		NewMessage([]byte("four")),
	}
	msgSets := []MessageSet{
		NewMessageSet(0, msgs...),
		NewMessageSet(1, msgs...),
	}
	for _, msgSet := range msgSets {
		_, err = l.Append(msgSet)
		assert.NoError(t, err)
	}
	maxBytes := msgSets[0].Size()
	r, err := l.NewReader(ReaderOptions{
		Offset:   0,
		MaxBytes: maxBytes,
	})
	assert.NoError(t, err)

	for i, _ := range msgSets {
		p := make([]byte, maxBytes)
		_, err = r.Read(p)
		assert.NoError(t, err)

		ms := MessageSet(p)
		assert.Equal(t, int64(i), ms.Offset())

		payload := ms.Payload()
		var offset int
		for _, msg := range msgs {
			assert.Equal(t, []byte(msg), payload[offset:offset+len(msg)])
			offset += len(msg)
		}
	}
}

func check(t *testing.T, got, want []byte) {
	if !bytes.Equal(got, want) {
		t.Errorf("got = %s, want %s", string(got), string(want))
	}
}
