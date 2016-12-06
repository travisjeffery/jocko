package commitlog

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"sync"

	"github.com/pkg/errors"
	"github.com/travisjeffery/jocko/protocol"
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

	sync.Mutex
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

	s := &Segment{
		log:        log,
		writer:     log,
		reader:     log,
		Position:   fi.Size(),
		maxBytes:   maxBytes,
		BaseOffset: baseOffset,
		NextOffset: baseOffset,
	}
	err = s.SetupIndex(path)
	if err == io.EOF {
		return s, nil
	}

	return s, err
}

func (s *Segment) SetupIndex(path string) (err error) {
	indexPath := filepath.Join(path, fmt.Sprintf(indexNameFormat, s.BaseOffset))
	s.Index, err = newIndex(options{
		path:       indexPath,
		baseOffset: s.BaseOffset,
	})
	if err != nil {
		return err
	}

	_, err = s.log.Seek(0, 0)
	if err != nil {
		return err
	}

	b := new(bytes.Buffer)
	for {
		// get offset and size
		_, err := io.CopyN(b, s.log, 8)
		if err != nil {
			return err
		}
		s.NextOffset = int64(protocol.Encoding.Uint64(b.Bytes()[0:8]))

		_, err = io.CopyN(b, s.log, 4)
		if err != nil {
			return err
		}
		size := int64(protocol.Encoding.Uint32(b.Bytes()[8:12]))

		_, err = io.CopyN(b, s.log, size)
		if err != nil {
			return err
		}

		err = s.Index.WriteEntry(Entry{
			Offset:   s.NextOffset,
			Position: s.Position,
		})
		if err != nil {
			return err
		}

		s.Position += size + msgSetHeaderLen
		s.NextOffset++

		_, err = s.log.Seek(size, 1)
		if err != nil {
			return err
		}
	}
}

func (s *Segment) IsFull() bool {
	s.Lock()
	defer s.Unlock()
	return s.Position >= s.maxBytes
}

func (s *Segment) Write(p []byte) (n int, err error) {
	s.Lock()
	defer s.Unlock()
	n, err = s.writer.Write(p)
	if err != nil {
		return n, errors.Wrap(err, "log write failed")
	}
	s.NextOffset++
	s.Position += int64(n)
	return n, nil
}

func (s *Segment) Read(p []byte) (n int, err error) {
	s.Lock()
	defer s.Unlock()
	return s.reader.Read(p)
}

func (s *Segment) ReadAt(p []byte, off int64) (n int, err error) {
	s.Lock()
	defer s.Unlock()
	return s.log.ReadAt(p, off)
}

func (s *Segment) Close() error {
	s.Lock()
	defer s.Unlock()
	if err := s.log.Close(); err != nil {
		return err
	}
	return s.Index.Close()
}

func (s *Segment) findEntry(offset int64) (e *Entry, err error) {
	s.Lock()
	defer s.Unlock()
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

func (s *Segment) Delete() error {
	if err := s.Close(); err != nil {
		return err
	}
	s.Lock()
	defer s.Unlock()
	if err := os.Remove(s.log.Name()); err != nil {
		return err
	}
	if err := os.Remove(s.Index.Name()); err != nil {
		return err
	}
	return nil
}
