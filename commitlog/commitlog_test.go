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

var (
	msgs = []Message{
		NewMessage([]byte("one")),
		NewMessage([]byte("two")),
		NewMessage([]byte("three")),
		NewMessage([]byte("four")),
	}
	msgSets = []MessageSet{
		NewMessageSet(0, msgs...),
		NewMessageSet(1, msgs...),
	}
	maxBytes = msgSets[0].Size()
	path     = filepath.Join(os.TempDir(), fmt.Sprintf("commitlogtest%d", rand.Int63()))
)

func TestNewCommitLog(t *testing.T) {
	var err error
	l := setup(t)
	defer cleanup(t)

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

func TestNewCommitLogExisting(t *testing.T) {
	var err error
	l0 := setup(t)
	defer cleanup(t)

	for _, msgSet := range msgSets {
		_, err = l0.Append(msgSet)
		assert.NoError(t, err)
	}
	assert.Equal(t, int64(2), l0.NewestOffset())

	l1 := setup(t)
	msgs1 := []Message{
		NewMessage([]byte("five")),
	}
	msgSet1 := NewMessageSet(2, msgs1...)
	return

	offset, err := l1.Append(msgSet1)
	assert.NoError(t, err)
	assert.Equal(t, int64(len(msgSets)), offset)
	assert.Equal(t, int64(3), l1.NewestOffset())

	maxBytes := msgSets[0].Size()
	r, err := l1.NewReader(ReaderOptions{
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

func setup(t *testing.T) *CommitLog {
	opts := Options{
		Path:            path,
		MaxSegmentBytes: 6,
	}
	l, err := New(opts)
	assert.NoError(t, err)

	assert.NoError(t, l.Init())
	assert.NoError(t, l.Open())

	return l
}

func cleanup(t *testing.T) {
	os.RemoveAll(path)
	os.MkdirAll(path, 0755)
}
