package commitlog

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"

	"github.com/pkg/errors"
)

const (
	logNameFormat   = "%020d.log"
	indexNameFormat = "%020d.index"
)

type Segment struct {
	writer     io.Writer
	reader     io.Reader
	log        *os.File
	Index      *index
	BaseOffset int64
	NextOffset int64
	Position   int64
	maxBytes   int64
}

func NewSegment(path string, baseOffset int64, maxBytes int64) (*Segment, error) {
	logPath := filepath.Join(path, fmt.Sprintf(logNameFormat, baseOffset))
	log, err := os.OpenFile(logPath, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		return nil, errors.Wrap(err, "open file failed")
	}

	fi, err := log.Stat()
	if err != nil {
		return nil, errors.Wrap(err, "file stat failed")
	}

	indexPath := filepath.Join(path, fmt.Sprintf(indexNameFormat, baseOffset))
	index, err := newIndex(options{
		path:       indexPath,
		baseOffset: baseOffset,
	})
	if err != nil {
		return nil, err
	}

	s := &Segment{
		log:        log,
		Index:      index,
		writer:     log,
		reader:     log,
		Position:   fi.Size(),
		maxBytes:   maxBytes,
		BaseOffset: baseOffset,
		NextOffset: baseOffset,
	}

	return s, nil
}

func (s *Segment) IsFull() bool {
	return s.Position >= s.maxBytes
}

func (s *Segment) Write(p []byte) (n int, err error) {
	n, err = s.writer.Write(p)
	if err != nil {
		return n, errors.Wrap(err, "log write failed")
	}
	s.NextOffset++
	s.Position += int64(n)
	return n, nil
}

func (s *Segment) Read(p []byte) (n int, err error) {
	return s.reader.Read(p)
}

func (s *Segment) ReadAt(p []byte, off int64) (n int, err error) {
	return s.log.ReadAt(p, off)
}

func (s *Segment) Close() error {
	if err := s.log.Close(); err != nil {
		return err
	}
	return s.Index.Close()
}

func (s *Segment) findEntry(offset int64) (e *Entry, err error) {
	e = &Entry{}
	idx := sort.Search(int(s.Index.bytes/entryWidth), func(i int) bool {
		_ = s.Index.ReadEntry(e, int64(i*entryWidth))
		return e.Offset > offset || e.Offset == 0
	})
	if idx == -1 {
		return nil, errors.New("entry not found")
	}
	return e, nil
}
