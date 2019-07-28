package commitlog

import (
	"fmt"
	"os"
	"path"
	"path/filepath"
	"sort"
	"sync"

	"github.com/pkg/errors"
)

const (
	fileFormat    = "%020d%s"
	logSuffix     = ".log"
	indexSuffix   = ".index"
	cleanedSuffix = ".cleaned"
)

type segment struct {
	sync.Mutex

	log        *log
	index      *index
	dir        string
	baseOffset int64
	nextOffset int64
	config     Config
}

func newSegment(dir string, baseOffset int64, c Config) (*segment, error) {
	s := &segment{
		dir:        dir,
		baseOffset: baseOffset,
		config:     c,
	}
	var err error
	logFile, err := os.OpenFile(path.Join(dir, fmt.Sprintf("%d%s", baseOffset, logSuffix)), os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return nil, err
	}
	if s.log, err = newLog(logFile); err != nil {
		return nil, err
	}
	indexFile, err := os.OpenFile(path.Join(dir, fmt.Sprintf("%d%s", baseOffset, indexSuffix)), os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return nil, err
	}
	if err = indexFile.Truncate(
		int64(nearestMultiple(c.MaxIndexBytes, entryWidth)),
	); err != nil {
		return nil, err
	}
	var lastEntry entry
	if s.index, lastEntry, err = newIndex(indexFile, baseOffset); err != nil {
		return nil, err
	}
	if lastEntry.IsZero() {
		s.nextOffset = baseOffset
	} else {
		s.nextOffset = lastEntry.Off + 1
	}
	// TODO: should check integriy and possible clear and reinitialize
	return s, nil
}

// func (s *segment) BuildIndex() (err error) {
// 	if err = s.Index.SanityCheck(); err != nil {
// 		return err
// 	}
// 	if err := s.Index.TruncateEntries(0); err != nil {
// 		return err
// 	}

// 	_, err = s.log.Seek(0, 0)
// 	if err != nil {
// 		return err
// 	}

// 	b := new(bytes.Buffer)

// 	nextOffset := s.baseOffset
// 	position := int64(0)

// loop:
// 	for {
// 		// get offset and size
// 		_, err = io.CopyN(b, s.log, 8)
// 		if err != nil {
// 			break loop
// 		}

// 		_, err = io.CopyN(b, s.log, 4)
// 		if err != nil {
// 			break loop
// 		}
// 		size := int64(Encoding.Uint32(b.Bytes()[8:12]))

// 		_, err = io.CopyN(b, s.log, size)
// 		if err != nil {
// 			break loop
// 		}

// 		// Reset the buffer to not get an overflow
// 		b.Truncate(0)

// 		entry := entry{
// 			Off: nextOffset,
// 			Pos: position,
// 		}
// 		err = s.Index.WriteEntry(entry)
// 		if err != nil {
// 			break loop
// 		}

// 		position += size + msgSetHeaderLen
// 		nextOffset++

// 		_, err = s.log.Seek(size, 1)
// 		if err != nil {
// 			break loop
// 		}
// 	}
// 	if err == io.EOF {
// 		s.nextOffset = nextOffset
// 		s.Position = position
// 		return nil
// 	}
// 	return err
// }

func (s *segment) Append(p []byte) (int64, int64, error) {
	return 0, 0, nil
}

// Write writes a byte slice to the log at the current position.
// It increments the offset as well as sets the position to the new tail.
func (s *segment) Write(p []byte) (n int, err error) {
	s.Lock()
	defer s.Unlock()
	n, err = s.writer.Write(p)
	if err != nil {
		return n, errors.Wrap(err, "log write failed")
	}
	s.nextOffset++
	s.Position += int64(n)
	return n, nil
}

func (s *segment) IsMaxed() bool {
	return s.log.size >= s.config.MaxSegmentBytes ||
		s.index.pos >= s.config.MaxIndexBytes
}

func (s *segment) Read(p []byte) (n int, err error) {
	s.Lock()
	defer s.Unlock()
	return s.reader.Read(p)
}

func (s *segment) ReadAt(p []byte, off int64) (n int, err error) {
	s.Lock()
	defer s.Unlock()
	return s.log.ReadAt(p, off)
}

func (s *segment) Close() error {
	s.Lock()
	defer s.Unlock()
	if err := s.log.Close(); err != nil {
		return err
	}
	return s.Index.Close()
}

// Cleaner creates a cleaner segment for this segment.
func (s *segment) Cleaner() (*segment, error) {
	return NewSegment(s.path, s.baseOffset, s.maxBytes, cleanedSuffix)
}

// Replace replaces the given segment with the callee.
func (s *segment) Replace(old *segment) (err error) {
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
func (s *segment) findEntry(offset int64) (e *entry, err error) {
	s.Lock()
	defer s.Unlock()
	e = &entry{}
	n := int(s.Index.bytes / entryWidth)
	idx := sort.Search(n, func(i int) bool {
		_ = s.Index.ReadEntryAtFileOffset(e, int64(i*entryWidth))
		return e.Off >= offset || e.Off == 0
	})
	if idx == n {
		return nil, errors.New("entry not found")
	}
	_ = s.Index.ReadEntryAtFileOffset(e, int64(idx*entryWidth))
	return e, nil
}

// Delete closes the segment and then deletes its log and index files.
func (s *segment) Delete() error {
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

func (s *segment) Size() int64 {
	return s.log.Size()
}

type SegmentScanner struct {
	s  *segment
	is *indexScanner
}

func NewSegmentScanner(segment *segment) *SegmentScanner {
	return &SegmentScanner{s: segment, is: newIndexScanner(segment.Index)}
}

// Scan should be called repeatedly to iterate over the messages in the segment, it will return
// io.EOF when there are no more messages.
func (s *SegmentScanner) Scan() (ms MessageSet, err error) {
	entry, err := s.is.Scan()
	if err != nil {
		return nil, err
	}
	header := make(MessageSet, msgSetHeaderLen)
	_, err = s.s.ReadAt(header, entry.Pos)
	if err != nil {
		return nil, err
	}
	size := int64(header.Size() - msgSetHeaderLen)
	payload := make([]byte, size)
	_, err = s.s.ReadAt(payload, entry.Pos+msgSetHeaderLen)
	if err != nil {
		return nil, err
	}
	msgSet := append(header, payload...)
	return msgSet, nil
}

func (s *segment) logPath() string {
	return filepath.Join(s.path, fmt.Sprintf(fileFormat, s.baseOffset, logSuffix+s.suffix))
}

func (s *segment) indexPath() string {
	return filepath.Join(s.path, fmt.Sprintf(fileFormat, s.baseOffset, indexSuffix+s.suffix))
}

func nearestMultiple(j, k uint64) uint64 {
	if j >= 0 {
		return (j / k) * k
	}
	return ((j - k + 1) / k) * k

}
