package commitlog

import (
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/pkg/errors"
)

const (
	logNameFormat   = "%020d.log"
	indexNameFormat = "%020d.index"
)

type segment struct {
	writer       io.Writer
	reader       io.Reader
	log          *os.File
	index        *os.File
	baseOffset   int64
	newestOffset int64
	bytes        int64
	maxBytes     int64
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
	index, err := os.OpenFile(indexPath, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		return nil, errors.Wrap(err, "open file failed")
	}

	s := &segment{
		log:          log,
		index:        index,
		writer:       log,
		reader:       log,
		bytes:        fi.Size(),
		maxBytes:     maxBytes,
		baseOffset:   baseOffset,
		newestOffset: baseOffset,
	}

	return s, nil
}

func (s *segment) NewestOffset() int64 {
	return s.newestOffset
}

func (s *segment) IsFull() bool {
	return s.bytes >= s.maxBytes
}

func (s *segment) Write(p []byte) (n int, err error) {
	n, err = s.writer.Write(p)
	if err != nil {
		return n, errors.Wrap(err, "log write failed")
	}

	_, err = s.index.Write([]byte(fmt.Sprintf("%d,%d\n", s.newestOffset, s.bytes)))
	if err != nil {
		return 0, errors.Wrap(err, "index write failed")
	}

	s.newestOffset += 1
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
