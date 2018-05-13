package commitlog_test

import (
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/travisjeffery/jocko/commitlog"
	"github.com/travisjeffery/jocko/protocol"
)

func TestDeleteCleaner(t *testing.T) {
	req := require.New(t)
	var err error

	var msgSets []commitlog.MessageSet
	msgSets = append(msgSets, newMessageSet(0, &protocol.Message{
		Key:       []byte("travisjeffery"),
		Value:     []byte("one tj"),
		MagicByte: 2,
		Timestamp: time.Now(),
	}))

	msgSets = append(msgSets, newMessageSet(1, &protocol.Message{
		Key:       []byte("another"),
		Value:     []byte("one another"),
		MagicByte: 2,
		Timestamp: time.Now(),
	}))

	msgSets = append(msgSets, newMessageSet(2, &protocol.Message{
		Key:       []byte("travisjeffery"),
		Value:     []byte("two tj"),
		MagicByte: 2,
		Timestamp: time.Now(),
	}))

	msgSets = append(msgSets, newMessageSet(3, &protocol.Message{
		Key:       []byte("again another"),
		Value:     []byte("again another"),
		MagicByte: 2,
		Timestamp: time.Now(),
	}))

	path := os.TempDir()
	fmt.Println(path)
	defer os.RemoveAll(path)

	opts := commitlog.Options{
		Path:            path,
		MaxSegmentBytes: int64(len(msgSets[0]) + len(msgSets[1])),
		MaxLogBytes:     1000,
	}
	l, err := commitlog.New(opts)
	require.NoError(t, err)

	for _, msgSet := range msgSets {
		_, err = l.Append(msgSet)
		require.NoError(t, err)
	}

	segments := l.Segments()
	req.Equal(2, len(l.Segments()))
	segment := segments[0]

	scanner := commitlog.NewSegmentScanner(segment)
	ms, err := scanner.Scan()
	require.NoError(t, err)
	require.Equal(t, msgSets[0], ms)

	// want to clean the last two msg sets
	cc := commitlog.NewDeleteCleaner(int64(len(msgSets[2]) + len(msgSets[3])))
	cleaned, err := cc.Clean(segments)
	req.NoError(err)
	req.Equal(1, len(cleaned))

	scanner = commitlog.NewSegmentScanner(cleaned[0])
	var count int

	ms, err = scanner.Scan()
	req.NoError(err)
	req.Equal(1, len(ms.Messages()))
	req.Equal(int64(2), ms.Offset())
	req.Equal([]byte("travisjeffery"), ms.Messages()[0].Key())
	req.Equal([]byte("two tj"), ms.Messages()[0].Value())
	count++

	ms, err = scanner.Scan()
	req.NoError(err)
	req.Equal(1, len(ms.Messages()))
	req.Equal(int64(3), ms.Offset())
	req.Equal([]byte("again another"), ms.Messages()[0].Key())
	req.Equal([]byte("again another"), ms.Messages()[0].Value())
	count++

	req.Equal(2, count)

}
