package commitlog

import (
	"io"
	"sync"

	"github.com/pkg/errors"
)

type Reader struct {
	cl  *CommitLog
	idx int
	mu  sync.Mutex
	pos int64
}

func (r *Reader) Read(p []byte) (n int, err error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	segments := r.cl.Segments()
	segment := segments[r.idx]

	var readSize int
	for {
		readSize, err = segment.ReadAt(p[n:], r.pos)
		n += readSize
		r.pos += int64(readSize)
		if readSize != 0 && err == nil {
			continue
		}
		if n == len(p) || err != io.EOF {
			break
		}
		if len(segments) <= r.idx+1 {
			err = io.EOF
			break
		}
		r.idx++
		segment = segments[r.idx]
		r.pos = 0
	}

	return n, err
}

func (l *CommitLog) NewReader(offset int64, maxBytes int32) (io.Reader, error) {
	var s *Segment
	var idx int
	if offset == 0 {
		// TODO: seems hackish, should at least check if segments are set.
		s, idx = l.Segments()[0], 0
	} else {
		s, idx = findSegment(l.Segments(), offset)
	}
	if s == nil {
		return nil, errors.Wrapf(ErrSegmentNotFound, "segments: %d, offset: %d", len(l.Segments()), offset)
	}
	e, err := s.findEntry(offset)
	if err != nil {
		return nil, err
	}
	return &Reader{
		cl:  l,
		idx: idx,
		pos: e.Position,
	}, nil
}
