package commitlog_test

import (
	"io"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/travisjeffery/jocko/commitlog"
)

func TestSegmentScanner(t *testing.T) {
	var err error

	l := setupWithOptions(t, commitlog.Options{
		MaxSegmentBytes: 1000,
		MaxLogBytes:     1000,
	})
	defer cleanup(t, l)

	for _, msgSet := range msgSets {
		_, err = l.Append(msgSet)
		require.NoError(t, err)
	}

	segments := l.Segments()
	segment := segments[0]

	scanner := commitlog.NewSegmentScanner(segment)
	ms, err := scanner.Scan()
	require.NoError(t, err)
	require.Equal(t, msgSets[0], ms)
}
func TestSegmentReader(t *testing.T) {
	var err error

	l := setupWithOptions(t, commitlog.Options{
		MaxSegmentBytes: 1024,
		MaxLogBytes:     1024,
	})
	defer cleanup(t, l)

	message := []byte("one")
	nb := 12
	for i := 0; i < nb; i++ {
		_, err = l.Append(commitlog.NewMessageSet(uint64(i), message))
		require.NoError(t, err)
	}
	t.Run("must read 1 message from offset 11", mustRead(l, 11, 1, message))
}

func mustRead(l *commitlog.CommitLog, offset int64, messagesNumber int, message []byte) func(t *testing.T) {
	return func(t *testing.T) {
		r, err := l.NewReader(offset, 0)
		require.NoError(t, err)
		buf := make([]byte, len(message)+12)

		for messagesNumber > 0 {
			n, err := r.Read(buf)
			require.NoError(t, err)
			require.Equal(t, message, buf[12:])
			require.Equal(t, 15, n)
			messagesNumber--
		}
		n, err := r.Read(buf)
		require.Equal(t, 0, n)
		require.Equal(t, io.EOF, err)
	}
}
