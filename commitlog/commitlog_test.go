package commitlog

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"math/rand"
	"os"
	"path/filepath"
	"testing"
)

func TestNewCommitLog(t *testing.T) {
	path := filepath.Join(os.TempDir(), fmt.Sprintf("commitlogtest%d", rand.Int63()))
	fmt.Println(path)
	opts := Options{
		Path:         path,
		SegmentBytes: 6,
	}
	l, err := New(opts)

	// remove old data
	l.deleteAll()

	l.init()
	l.open()

	if err != nil {
		t.Fatal(err)
	}

	_, err = l.Write([]byte("one"))
	if err != nil {
		t.Error(err)
	}

	_, err = l.Write([]byte("two"))
	if err != nil {
		t.Error(err)
	}

	_, err = l.Write([]byte("three"))
	if err != nil {
		t.Error(err)
	}

	_, err = l.Write([]byte("four"))
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
