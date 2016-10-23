package commitlog

import (
	"bytes"
	"fmt"
	"io/ioutil"
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

	err = l.Append(MessageSet{
		Offset:  0,
		Payload: []byte("one"),
	})
	if err != nil {
		t.Error(err)
	}

	err = l.Append(MessageSet{
		Offset:  1,
		Payload: []byte("two"),
	})
	if err != nil {
		t.Error(err)
	}

	err = l.Append(MessageSet{
		Offset:  2,
		Payload: []byte("three"),
	})
	if err != nil {
		t.Error(err)
	}

	err = l.Append(MessageSet{
		Offset:  3,
		Payload: []byte("four"),
	})
	if err != nil {
		t.Error(err)
	}

	r, err := l.NewReader(1)
	if err != nil {
		t.Error(err)
	}
	p, err := ioutil.ReadAll(r)
	if err != nil {
		t.Error(err)
	}

	check(t, p, []byte("twothreefour"))
}

func check(t *testing.T, got, want []byte) {
	if !bytes.Equal(got, want) {
		t.Errorf("got = %s, want %s", string(got), string(want))
	}
}
