package commitlog

import (
	"io"
	"sync"

	"github.com/pkg/errors"
)

type Reader struct {
	segment  *segment
	segments []*segment
	idx      int
	mu       sync.Mutex
	offset   int64
}

func (r *Reader) Read(p []byte) (n int, err error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	var readSize int
	for {
		readSize, err = r.segment.ReadAt(p[n:], r.offset)
		n += readSize
		if err != io.EOF {
			break
		}
		r.idx++
		if len(r.segments) <= r.idx {
			err = io.EOF
			break
		}
		r.segment = r.segments[r.idx]
		r.offset = 0
	}

	return n, err
}

func (l *CommitLog) NewReader(offset int64) (r *Reader, err error) {
	segment, idx := findSegment(l.segments, offset)

	if segment == nil {
		return nil, errors.Wrap(err, "segment not found")
	}

	return &Reader{
		segment:  segment,
		segments: l.segments,
		idx:      idx,
		offset:   offset,
	}, nil
}
