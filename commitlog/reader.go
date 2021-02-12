package commitlog

import (
	"io"
	"os"
	"sync"

	"github.com/travisjeffery/jocko/log"

	"github.com/pkg/errors"
)

type Reader struct {
	cl  *CommitLog
	idx int
	mu  sync.Mutex
	pos int64
}

func (r *Reader) FileAndOffset() (*os.File, int64, int) {
	r.mu.Lock()
	defer r.mu.Unlock()

	segments := r.cl.Segments()
	segment := segments[r.idx]
	file := segment.File()
	//todo size
	size := 0
	return file, r.pos, size
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
	//TODO: offset relative?
	offset = offset - s.BaseOffset
	e, err := s.findEntry(offset)
	if err != nil {
		return nil, err
	}
	{
		log.Info.Printf("entry: %+v err: %v", e, err)
		idx := s.Index
		log.Info.Printf("idx: %p idx.position %d mmap: %v \n", idx, idx.position, idx.mmap[0:idx.position])
	}
	return &Reader{
		cl:  l,
		idx: idx,
		pos: e.Position,
	}, nil
}

func (l *CommitLog) SendfileParams(offset int64, maxBytes int32) (*os.File, int64, int, error) {
	var s *Segment
	var idx int
	if offset == 0 {
		// TODO: seems hackish, should at least check if segments are set.
		s, idx = l.Segments()[0], 0
	} else {
		s, idx = findSegment(l.Segments(), offset)
	}
	if s == nil {
		return nil, 0, 0, errors.Wrapf(ErrSegmentNotFound, "segments: %d, offset: %d", len(l.Segments()), offset)
	}
	//TODO: offset relative?
	offset = offset - s.BaseOffset
	e, err := s.findEntry(offset)
	if err != nil {
		return nil, 0, 0, err
	}
	{
		log.Info.Printf("entry: %+v err: %v", e, err)
		idx := s.Index
		log.Info.Printf("idx: %p idx.position %d mmap: %v \n", idx, idx.position, idx.mmap[0:idx.position])
	}
	file := s.File()
	_ = idx
	//todo : calculate fileOffset and sendSize
	fileOffset := int64(0)
	sendSize := s.Position
	// log.Info.Println("logfile:", file.Name(),
	// 	"fileOffset", fileOffset,
	// 	"sendSize", sendSize)

	return file, fileOffset, int(sendSize), nil
}
