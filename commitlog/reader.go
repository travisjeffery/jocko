package commitlog

import (
	"io"
	"sync"

	"github.com/pkg/errors"
)

type Reader struct {
	ReaderOptions
	segment  *Segment
	segments []*Segment
	idx      int
	mu       sync.Mutex
	position int64
}

func (r *Reader) Read(p []byte) (n int, err error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	var readSize int
	for {
		readSize, err = r.segment.ReadAt(p[n:], r.position)
		n += readSize
		r.position += int64(readSize)
		if readSize != 0 && err == nil {
			continue
		}
		if n == len(p) || err != io.EOF {
			break
		}
		if len(r.segments) <= r.idx+1 {
			err = io.EOF
			break
		}
		r.idx++
		r.segment = r.segments[r.idx]
		r.position = 0
	}

	return n, err
}

type ReaderOptions struct {
	Offset   int64
	MaxBytes int32
	P        []byte
}

func (l *CommitLog) NewReader(options ReaderOptions) (r *Reader, err error) {
	segment, idx := findSegment(l.segments, options.Offset)
	entry, _ := segment.findEntry(options.Offset)
	position := entry.Position

	if segment == nil {
		return nil, errors.Wrap(err, "segment not found")
	}

	return &Reader{
		ReaderOptions: options,
		segment:       segment,
		segments:      l.segments,
		idx:           idx,
		position:      position,
	}, nil
}
