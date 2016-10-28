package commitlog

import (
	"bytes"
	"fmt"
	"io"
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

	// remove old data
	assert.NoError(t, l.DeleteAll())

	assert.NoError(t, l.Init())
	assert.NoError(t, l.Open())

	if err != nil {
		t.Fatal(err)
	}

	msgs := []Message{
		NewMessage([]byte("one")),
		NewMessage([]byte("two")),
		NewMessage([]byte("three")),
		NewMessage([]byte("four")),
	}
	msgSet := NewMessageSet(msgs, 1)
	err = l.Append(msgSet)
	if err != nil {
		t.Error(err)
	}
	maxBytes := msgSet.Size() * 2
	r, err := l.NewReader(ReaderOptions{
		Offset:   0,
		MaxBytes: maxBytes,
	})
	if err != nil {
		t.Error(err)
	}
	p := make([]byte, maxBytes)
	_, err = r.Read(p)
	assert.Equal(t, io.EOF, err)
	ms := MessageSet(p)
	assert.Equal(t, int64(1), ms.Offset())
	for i, m := range ms.Messages() {
		assert.Equal(t, msgs[i], m)
	}
}

func check(t *testing.T, got, want []byte) {
	if !bytes.Equal(got, want) {
		t.Errorf("got = %s, want %s", string(got), string(want))
	}
}
