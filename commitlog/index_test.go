package commitlog

import (
	"io"
	"io/ioutil"
	"os"
	"testing"

	req "github.com/stretchr/testify/require"
)

func TestIndex(t *testing.T) {
	f, err := ioutil.TempFile(os.TempDir(), "index_test")
	req.NoError(t, err)
	defer os.Remove(f.Name())
	_ = f.Truncate(1024)

	testIndex(t, f)

	_, e, err := newIndex(f)
	req.NoError(t, err)
	req.Equal(t, uint64(1), e.Off)
}

func testIndex(t *testing.T, f *os.File) {
	t.Helper()

	idx, e, err := newIndex(f)
	req.NoError(t, err)
	req.True(t, e.IsZero())

	entries := []entry{
		{Off: 0, Pos: 0, Len: 10},
		{Off: 1, Pos: 10, Len: 10},
	}

	is := indexScanner{idx: idx}

	for _, want := range entries {
		err = idx.Write(want)
		req.NoError(t, err)

		got, err := idx.Read(want.Off)
		req.NoError(t, err)
		req.Equal(t, want, got)

		req.True(t, is.Scan())
		req.Equal(t, want, is.Entry())
		req.NoError(t, is.Err())
	}

	_, err = idx.Read(uint64(len(entries)))
	req.Equal(t, io.EOF, err)
	req.False(t, is.Scan())
	req.Equal(t, io.EOF, is.Err())
}
