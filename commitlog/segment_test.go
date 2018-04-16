package commitlog_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/travisjeffery/jocko/commitlog"
)

func TestSegmentScanner(t *testing.T) {
	var err error

	opts := commitlog.Options{
		Path:            path,
		MaxSegmentBytes: 1000,
		MaxLogBytes:     1000,
	}
	l, err := commitlog.New(opts)
	require.NoError(t, err)

	defer cleanup(t)

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
