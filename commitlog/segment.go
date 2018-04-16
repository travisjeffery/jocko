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
)

const (
	logNameFormat   = "%020d.log"
	indexNameFormat = "%020d.index"
)

type Segment struct {
	writer     io.Writer
	reader     io.Reader
	log        *os.File
	Index      *Index
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

	s := &Segment{
		log:        log,
		writer:     log,
		reader:     log,
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

// SetupIndex creates and initializes an Index.
// Initialization is:
// - Sanity check of the loaded Index
// - Truncates the Index (clears it)
// - Reads the log file from the beginning and re-initializes the Index
func (s *Segment) SetupIndex(path string) (err error) {
	indexPath := filepath.Join(path, fmt.Sprintf(indexNameFormat, s.BaseOffset))
	s.Index, err = NewIndex(options{
		path:       indexPath,
		baseOffset: s.BaseOffset,
	})
	if err != nil {
		return err
	}
	if err = s.Index.SanityCheck(); err != nil {
		return err
	}
	if err := s.Index.TruncateEntries(0); err != nil {
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
		s.NextOffset = int64(Encoding.Uint64(b.Bytes()[0:8]))

		_, err = io.CopyN(b, s.log, 4)
		if err != nil {
			return err
		}
		size := int64(Encoding.Uint32(b.Bytes()[8:12]))

		_, err = io.CopyN(b, s.log, size)
		if err != nil {
			return err
		}

		// Reset the buffer to not get an overflow
		b.Truncate(0)

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

// Write writes a byte slice to the log at the current position.
// It increments the offset as well as sets the position to the new tail.
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
	n := int(s.Index.bytes / entryWidth)
	idx := sort.Search(n, func(i int) bool {
		_ = s.Index.ReadEntryAtFileOffset(e, int64(i*entryWidth))
		return e.Offset >= offset || e.Offset == 0
	})
	if idx == n {
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

type SegmentScanner struct {
	s  *Segment
	is *IndexScanner
}

func NewSegmentScanner(segment *Segment) *SegmentScanner {
	return &SegmentScanner{s: segment, is: NewIndexScanner(segment.Index)}
}

func (s *SegmentScanner) Scan() (ms MessageSet, err error) {
	entry, err := s.is.Scan()
	if err != nil {
		return nil, err
	}
	header := make(MessageSet, msgSetHeaderLen)
	_, err = s.s.ReadAt(header, entry.Position)
	if err != nil {
		return nil, err
	}
	size := int64(header.Size() - msgSetHeaderLen)
	payload := make([]byte, size)
	_, err = s.s.ReadAt(payload, entry.Position+msgSetHeaderLen)
	if err != nil {
		return nil, err
	}
	msgSet := append(header, payload...)
	return msgSet, nil
}
