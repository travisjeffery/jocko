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
	r, err := l.NewReader(0, maxBytes)
	assert.NoError(t, err)

	for i := range msgSets {
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
	l := setup(t)
	defer cleanup(t)

	for _, msgSet := range msgSets {
		_, err = l.Append(msgSet)
		assert.NoError(t, err)
	}
	assert.Equal(t, int64(2), l.NewestOffset())

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
	r, err := l1.NewReader(0, maxBytes)
	assert.NoError(t, err)

	for i := range msgSets {
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

func TestTruncateTo(t *testing.T) {
	var err error
	l := setup(t)
	defer cleanup(t)

	for _, msgSet := range msgSets {
		_, err = l.Append(msgSet)
		assert.NoError(t, err)
	}
	assert.Equal(t, int64(2), l.NewestOffset())
	assert.Equal(t, 2, len(l.Segments()))

	err = l.TruncateTo(int64(1))
	assert.NoError(t, err)
	assert.Equal(t, 1, len(l.Segments()))

	maxBytes := msgSets[0].Size()
	_, err = l.NewReader(0, maxBytes)
	assert.Error(t, err)

	r, err := l.NewReader(1, maxBytes)
	assert.NoError(t, err)

	for i := range msgSets[1:] {
		p := make([]byte, maxBytes)
		_, err = r.Read(p)
		assert.NoError(t, err)

		ms := MessageSet(p)
		assert.Equal(t, int64(i+1), ms.Offset())

		payload := ms.Payload()
		var offset int
		for _, msg := range msgs {
			assert.Equal(t, []byte(msg), payload[offset:offset+len(msg)])
			offset += len(msg)
		}
	}
}

func TestCleaner(t *testing.T) {
	var err error
	l := setup(t)
	defer cleanup(t)

	for _, msgSet := range msgSets {
		_, err = l.Append(msgSet)
		assert.NoError(t, err)
	}
	segments := l.Segments()
	assert.Equal(t, 2, len(segments))

	for _, msgSet := range msgSets {
		_, err = l.Append(msgSet)
		assert.NoError(t, err)
	}
	assert.Equal(t, 2, len(l.Segments()))
	for i, s := range l.Segments() {
		assert.NotEqual(t, s, segments[i])
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
		MaxLogBytes:     30,
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
