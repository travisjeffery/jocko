package commitlog_test

import (
	"bytes"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/travisjeffery/jocko/commitlog"
)

var (
	msgs = []commitlog.Message{
		commitlog.NewMessage([]byte("one")),
		commitlog.NewMessage([]byte("two")),
		commitlog.NewMessage([]byte("three")),
		commitlog.NewMessage([]byte("four")),
	}
	msgSets = []commitlog.MessageSet{
		commitlog.NewMessageSet(0, msgs...),
		commitlog.NewMessageSet(1, msgs...),
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

		ms := commitlog.MessageSet(p)
		assert.Equal(t, int64(i), ms.Offset())

		payload := ms.Payload()
		var offset int
		for _, msg := range msgs {
			assert.Equal(t, []byte(msg), payload[offset:offset+len(msg)])
			offset += len(msg)
		}
	}
}

func BenchmarkCommitLog(b *testing.B) {
	var err error
	l := setup(b)
	defer cleanup(b)

	msgSet := msgSets[0]

	for i := 0; i < b.N; i++ {
		_, err = l.Append(msgSet)
		assert.NoError(b, err)
	}
}

func TestTruncate(t *testing.T) {
	var err error
	l := setup(t)
	defer cleanup(t)

	for _, msgSet := range msgSets {
		_, err = l.Append(msgSet)
		assert.NoError(t, err)
	}
	assert.Equal(t, int64(2), l.NewestOffset())
	assert.Equal(t, 2, len(l.Segments()))

	err = l.Truncate(int64(1))
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

		ms := commitlog.MessageSet(p)
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

func check(t assert.TestingT, got, want []byte) {
	if !bytes.Equal(got, want) {
		t.Errorf("got = %s, want %s", string(got), string(want))
	}
}

func setup(t assert.TestingT) *commitlog.CommitLog {
	opts := commitlog.Options{
		Path:            path,
		MaxSegmentBytes: 6,
		MaxLogBytes:     30,
	}
	l, err := commitlog.New(opts)
	assert.NoError(t, err)

	return l
}

func cleanup(t assert.TestingT) {
	os.RemoveAll(path)
	os.MkdirAll(path, 0755)
}
