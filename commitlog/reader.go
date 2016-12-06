package commitlog

import (
	"io"
	"sync"
)

type Reader struct {
	ReaderOptions
	commitlog *CommitLog
	idx       int
	mu        sync.Mutex
	position  int64
}

func (r *Reader) Read(p []byte) (n int, err error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	segments := r.commitlog.Segments()
	segment := segments[r.idx]

	var readSize int
	for {
		readSize, err = segment.ReadAt(p[n:], r.position)
		n += readSize
		r.position += int64(readSize)
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
	segment, idx := findSegment(l.Segments(), options.Offset)
	if segment == nil {
		return nil, ErrSegmentNotFound
	}
	entry, _ := segment.findEntry(options.Offset)
	position := entry.Position

	return &Reader{
		ReaderOptions: options,
		commitlog:     l,
		idx:           idx,
		position:      position,
	}, nil
}
