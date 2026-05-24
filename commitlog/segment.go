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
	fileFormat    = "%020d%s"
	logSuffix     = ".log"
	cleanedSuffix = ".cleaned"
	indexSuffix   = ".index"
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
	path       string
	suffix     string

	sync.Mutex
}

func NewSegment(path string, baseOffset, maxBytes int64, args ...interface{}) (*Segment, error) {
	var suffix string
	if len(args) != 0 {
		suffix = args[0].(string)
	}
	s := &Segment{
		maxBytes:   maxBytes,
		BaseOffset: baseOffset,
		NextOffset: baseOffset,
		path:       path,
		suffix:     suffix,
	}
	log, err := os.OpenFile(s.logPath(), os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		return nil, errors.Wrap(err, "open file failed")
	}
	s.log = log
	s.writer = log
	s.reader = log
	err = s.SetupIndex()
	return s, err
}

// SetupIndex creates and initializes an Index.
// Initialization is:
// - Sanity check of the loaded Index
// - Truncates the Index (clears it)
// - Reads the log file from the beginning and re-initializes the Index
func (s *Segment) SetupIndex() (err error) {
	s.Index, err = NewIndex(options{
		path:       s.indexPath(),
		baseOffset: s.BaseOffset,
	})
	if err != nil {
		return err
	}
	return s.BuildIndex()
}

func (s *Segment) BuildIndex() (err error) {
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

	nextOffset := s.BaseOffset
	position := int64(0)

loop:
	for {
		// get offset and size
		_, err = io.CopyN(b, s.log, 8)
		if err != nil {
			break loop
		}

		_, err = io.CopyN(b, s.log, 4)
		if err != nil {
			break loop
		}
		size := int64(Encoding.Uint32(b.Bytes()[8:12]))

		_, err = io.CopyN(b, s.log, size)
		if err != nil {
			break loop
		}

		// Reset the buffer to not get an overflow
		b.Truncate(0)

		entry := Entry{
			Offset:   nextOffset,
			Position: position,
		}
		err = s.Index.WriteEntry(entry)
		if err != nil {
			break loop
		}

		position += size + msgSetHeaderLen
		nextOffset++

		_, err = s.log.Seek(size, 1)
		if err != nil {
			break loop
		}
	}
	if err == io.EOF {
		s.NextOffset = nextOffset
		s.Position = position
		return nil
	}
	return err
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

// Cleaner creates a cleaner segment for this segment.
func (s *Segment) Cleaner() (*Segment, error) {
	return NewSegment(s.path, s.BaseOffset, s.maxBytes, cleanedSuffix)
}

// Replace replaces the given segment with the callee.
func (s *Segment) Replace(old *Segment) (err error) {
	if err = old.Close(); err != nil {
		return err
	}
	if err = s.Close(); err != nil {
		return err
	}
	if err = os.Rename(s.logPath(), old.logPath()); err != nil {
		return err
	}
	if err = os.Rename(s.indexPath(), old.indexPath()); err != nil {
		return err
	}
	s.suffix = ""
	log, err := os.OpenFile(s.logPath(), os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		return errors.Wrap(err, "open file failed")
	}
	s.log = log
	s.writer = log
	s.reader = log
	return s.SetupIndex()
}

// findEntry returns the nearest entry whose offset is greater than or equal to the given offset.
func (s *Segment) findEntry(offset int64) (e *Entry, err error) {
	s.Lock()
	defer s.Unlock()
	e = &Entry{}
	n := int(s.Index.bytes / entryWidth)
	idx := sort.Search(n, func(i int) bool {
		err = s.Index.ReadEntryAtFileOffset(e, int64(i*entryWidth))
		return e.Offset >= offset || err != nil
	})
	if idx == n {
		return nil, errors.New("entry not found")
	}
	return e, s.Index.ReadEntryAtFileOffset(e, int64(idx*entryWidth))
}

// Delete closes the segment and then deletes its log and index files.
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

// Scan should be called repeatedly to iterate over the messages in the segment, it will return
// io.EOF when there are no more messages.
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

func (s *Segment) logPath() string {
	return filepath.Join(s.path, fmt.Sprintf(fileFormat, s.BaseOffset, logSuffix+s.suffix))
}

func (s *Segment) indexPath() string {
	return filepath.Join(s.path, fmt.Sprintf(fileFormat, s.BaseOffset, indexSuffix+s.suffix))
}
