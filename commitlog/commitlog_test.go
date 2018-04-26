package commitlog_test

import (
	"bytes"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
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
	path = filepath.Join(os.TempDir(), fmt.Sprintf("commitlogtest%d", rand.Int63()))
)

func TestNewCommitLog(t *testing.T) {
	var err error
	l := setup(t)
	defer cleanup(t)

	for _, exp := range msgSets {
		_, err = l.Append(exp)
		require.NoError(t, err)
	}
	maxBytes := msgSets[0].Size()
	r, err := l.NewReader(0, maxBytes)
	require.NoError(t, err)

	for i, exp := range msgSets {
		p := make([]byte, maxBytes)
		_, err = r.Read(p)
		require.NoError(t, err)

		act := commitlog.MessageSet(p)
		require.Equal(t, exp, act)
		require.Equal(t, int64(i), act.Offset())

		payload := act.Payload()
		var offset int
		for _, msg := range msgs {
			require.Equal(t, []byte(msg), payload[offset:offset+len(msg)])
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
		require.NoError(b, err)
	}
}

func TestTruncate(t *testing.T) {
	var err error
	l := setup(t)
	defer cleanup(t)

	for _, msgSet := range msgSets {
		_, err = l.Append(msgSet)
		require.NoError(t, err)
	}
	require.Equal(t, int64(2), l.NewestOffset())
	require.Equal(t, 2, len(l.Segments()))

	err = l.Truncate(int64(1))
	require.NoError(t, err)
	require.Equal(t, 1, len(l.Segments()))

	maxBytes := msgSets[0].Size()
	_, err = l.NewReader(0, maxBytes)
	// should find next segment above
	require.NoError(t, err)

	r, err := l.NewReader(1, maxBytes)
	require.NoError(t, err)

	for i := range msgSets[1:] {
		p := make([]byte, maxBytes)
		_, err = r.Read(p)
		require.NoError(t, err)

		ms := commitlog.MessageSet(p)
		require.Equal(t, int64(i+1), ms.Offset())

		payload := ms.Payload()
		var offset int
		for _, msg := range msgs {
			require.Equal(t, []byte(msg), payload[offset:offset+len(msg)])
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
		require.NoError(t, err)
	}
	segments := l.Segments()
	require.Equal(t, 2, len(segments))

	for _, msgSet := range msgSets {
		_, err = l.Append(msgSet)
		require.NoError(t, err)
	}
	require.Equal(t, 2, len(l.Segments()))
	for i, s := range l.Segments() {
		require.NotEqual(t, s, segments[i])
	}
}

func check(t require.TestingT, got, want []byte) {
	if !bytes.Equal(got, want) {
		t.Errorf("got = %s, want %s", string(got), string(want))
	}
}

func setup(t require.TestingT) *commitlog.CommitLog {
	opts := commitlog.Options{
		Path:            path,
		MaxSegmentBytes: 6,
		MaxLogBytes:     30,
	}
	l, err := commitlog.New(opts)
	require.NoError(t, err)

	return l
}

func cleanup(t require.TestingT) {
	os.RemoveAll(path)
	os.MkdirAll(path, 0755)
}
