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

type segment struct {
	writer     io.Writer
	reader     io.Reader
	log        *os.File
	index      *index
	baseOffset int64
	nextOffset int64
	bytes      int64
	maxBytes   int64
}

func NewSegment(path string, baseOffset int64, maxBytes int64) (*segment, error) {
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

	s := &segment{
		log:        log,
		index:      index,
		writer:     log,
		reader:     log,
		bytes:      fi.Size(),
		maxBytes:   maxBytes,
		baseOffset: baseOffset,
		nextOffset: baseOffset,
	}

	return s, nil
}

func (s *segment) NextOffset() int64 {
	return s.nextOffset
}

func (s *segment) IsFull() bool {
	return s.bytes >= s.maxBytes
}

func (s *segment) Write(p []byte) (n int, err error) {
	n, err = s.writer.Write(p)
	if err != nil {
		return n, errors.Wrap(err, "log write failed")
	}

	_, err = s.index.Write([]byte(fmt.Sprintf("%d,%d\n", s.nextOffset, s.bytes)))
	if err != nil {
		return 0, errors.Wrap(err, "index write failed")
	}

	err = s.index.WriteEntry(entry{
		Offset:   s.nextOffset,
		Position: s.bytes,
	})
	if err != nil {
		return 0, err
	}

	s.nextOffset += 1
	s.bytes += int64(n)

	return n, nil
}

func (s *segment) Read(p []byte) (n int, err error) {
	return s.reader.Read(p)
}

func (s *segment) ReadAt(p []byte, off int64) (n int, err error) {
	return s.log.ReadAt(p, off)
}

func (s *segment) Close() error {
	if err := s.log.Close(); err != nil {
		return err
	}
	return s.index.Close()
}

func (s *segment) findEntry(offset int64) (e *entry, err error) {
	e = &entry{}
	idx := sort.Search(int(s.index.bytes/entryWidth), func(i int) bool {
		_ = s.index.ReadEntry(e, int64(i*entryWidth))
		return int64(e.Offset) > offset || e.Offset == 0
	})
	if idx == -1 {
		return nil, errors.New("entry not found")
	}
	return e, nil
}
