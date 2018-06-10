package commitlog_test

import (
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/travisjeffery/jocko/commitlog"
)

var readerTests = []struct {
	name        string
	segmentSize int64
}{
	{"6", 6},
	{"60", 60},
	{"600", 600},
	{"6000", 6000},
}

func TestReader(t *testing.T) {
	for _, test := range readerTests {
		t.Run(test.name, func(t *testing.T) {
			var err error
			l := setupWithOptions(t, commitlog.Options{
				MaxSegmentBytes: test.segmentSize,
				MaxLogBytes:     -1,
			})
			defer cleanup(t, l)

			numMsgs := 10
			msgs := make([]commitlog.MessageSet, numMsgs)
			for i := 0; i < numMsgs; i++ {
				msgs[i] = commitlog.NewMessageSet(
					uint64(i), commitlog.NewMessage([]byte(strconv.Itoa(i))),
				)
			}
			for _, ms := range msgs {
				_, err := l.Append(ms)
				require.NoError(t, err)
			}
			idx := 4
			maxBytes := msgs[idx].Size()
			r, err := l.NewReader(int64(idx), maxBytes)
			require.NoError(t, err)

			p := make([]byte, maxBytes)
			_, err = r.Read(p)
			require.NoError(t, err)
			act := commitlog.MessageSet(p)
			require.Equal(t, int64(idx), act.Offset())
		})
	}
}
