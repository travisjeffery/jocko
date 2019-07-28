package commitlog

import (
	"io/ioutil"
	"os"
	"testing"

	req "github.com/stretchr/testify/require"
)

func TestLog(t *testing.T) {
	f, err := ioutil.TempFile(os.TempDir(), "log_test")
	req.NoError(t, err)
	defer os.Remove(f.Name())

	b := []byte("hello world")
	_, err = f.Write(b)
	req.NoError(t, err)

	l, err := newLog(f)
	req.NoError(t, err)

	n, pos, err := l.Append(b)
	if err != nil {
		t.Fatalf("err: %s", err)
	}
	req.Equal(t, pos+n, int64(len(b)*2))
	req.Equal(t, l.Size(), int64(len(b)*2))
}

func TestLogScanner(t *testing.T) {
	f, err := ioutil.TempFile(os.TempDir(), "log_test")
	req.NoError(t, err)
	defer os.Remove(f.Name())

	l, err := newLog(f)
	req.NoError(t, err)

	ls := logScanner{l: log}

	ms := NewMessageSet(0, emptyMessage)
	for i := 0; i < 10; i++ {
		_, _, err = l.Append(ms)
		req.NoError(t, err)
	}

	var i int
	for ls.Scan() {
		e := ls.Entry()
		req.Equal(t, i, e.Off)
		req.Equal(t, e.Pos, i*len(ms))
		i++
	}
}
