package commitlog

import (
	"bytes"
	"encoding/binary"
	"io"
	"os"
	"sync"

	"github.com/pkg/errors"

	"github.com/tysontate/gommap"
)

var (
	ErrIndexCorrupt = errors.New("corrupt index file")
)

const (
	offsetWidth  = 4
	offsetOffset = 0

	positionWidth  = 4
	positionOffset = offsetWidth

	entryWidth = offsetWidth + positionWidth
)

type Index struct {
	options
	mmap     gommap.MMap
	file     *os.File
	mu       sync.RWMutex
	position int64
}

type Entry struct {
	Offset   int64
	Position int64
}

// relEntry is an Entry relative to the base fileOffset
type relEntry struct {
	Offset   int32
	Position int32
}

func newRelEntry(e Entry, baseOffset int64) relEntry {
	return relEntry{
		Offset:   int32(e.Offset - baseOffset),
		Position: int32(e.Position),
	}
}

func (rel relEntry) fill(e *Entry, baseOffset int64) {
	e.Offset = baseOffset + int64(rel.Offset)
	e.Position = int64(rel.Position)
}

type options struct {
	path       string
	bytes      int64
	baseOffset int64
}

func NewIndex(opts options) (idx *Index, err error) {
	if opts.bytes == 0 {
		opts.bytes = 10 * 1024 * 1024
	}
	if opts.path == "" {
		return nil, errors.New("path is empty")
	}
	idx = &Index{
		options: opts,
	}
	idx.file, err = os.OpenFile(opts.path, os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		return nil, errors.Wrap(err, "open file failed")
	}
	fi, err := idx.file.Stat()
	if err != nil {
		return nil, errors.Wrap(err, "stat file failed")
	} else if fi.Size() > 0 {
		idx.position = fi.Size()
	}
	if err := idx.file.Truncate(roundDown(opts.bytes, entryWidth)); err != nil {
		return nil, err
	}

	idx.mmap, err = gommap.Map(idx.file.Fd(), gommap.PROT_READ|gommap.PROT_WRITE, gommap.MAP_SHARED)
	if err != nil {
		return nil, errors.Wrap(err, "mmap file failed")
	}
	return idx, nil
}

func (idx *Index) WriteEntry(entry Entry) (err error) {
	b := new(bytes.Buffer)
	relEntry := newRelEntry(entry, idx.baseOffset)
	if err = binary.Write(b, Encoding, relEntry); err != nil {
		return errors.Wrap(err, "binary write failed")
	}
	idx.WriteAt(b.Bytes(), idx.position)
	idx.mu.Lock()
	idx.position += entryWidth
	idx.mu.Unlock()
	return nil
}

// ReadEntryAtFileOffset is used to read an Index entry at the given
// byte offset of the Index file. ReadEntryAtLogOffset is generally
// more useful for higher level use.
func (idx *Index) ReadEntryAtFileOffset(e *Entry, fileOffset int64) (err error) {
	p := make([]byte, entryWidth)
	if _, err = idx.ReadAt(p, fileOffset); err != nil {
		return err
	}
	b := bytes.NewReader(p)
	rel := &relEntry{}
	err = binary.Read(b, Encoding, rel)
	if err != nil {
		return errors.Wrap(err, "binary read failed")
	}
	idx.mu.RLock()
	rel.fill(e, idx.baseOffset)
	idx.mu.RUnlock()
	return nil
}

// ReadEntryAtLogOffset is used to read an Index entry at the given
// log offset of the Index file.
func (idx *Index) ReadEntryAtLogOffset(e *Entry, logOffset int64) error {
	return idx.ReadEntryAtFileOffset(e, logOffset*entryWidth)
}

func (idx *Index) ReadAt(p []byte, offset int64) (n int, err error) {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	if idx.position < offset+entryWidth {
		return 0, io.EOF
	}
	n = copy(p, idx.mmap[offset:offset+entryWidth])
	return n, nil
}

func (idx *Index) Write(p []byte) (n int, err error) {
	return idx.WriteAt(p, idx.position), nil
}

func (idx *Index) WriteAt(p []byte, offset int64) (n int) {
	idx.mu.Lock()
	defer idx.mu.Unlock()
	return copy(idx.mmap[offset:offset+entryWidth], p)
}

func (idx *Index) Sync() error {
	idx.mu.Lock()
	defer idx.mu.Unlock()
	if err := idx.file.Sync(); err != nil {
		return errors.Wrap(err, "file sync failed")
	}
	if err := idx.mmap.Sync(gommap.MS_SYNC); err != nil {
		return errors.Wrap(err, "mmap sync failed")
	}
	return nil
}

func (idx *Index) Close() (err error) {
	if err = idx.Sync(); err != nil {
		return
	}
	if err = idx.file.Truncate(idx.position); err != nil {
		return
	}
	return idx.file.Close()
}

func (idx *Index) Name() string {
	return idx.file.Name()
}

func (idx *Index) TruncateEntries(number int) error {
	idx.mu.Lock()
	defer idx.mu.Unlock()
	if int64(number*entryWidth) > idx.position {
		return errors.New("bad truncate number")
	}
	idx.position = int64(number * entryWidth)
	return nil
}

func (idx *Index) SanityCheck() error {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	if idx.position == 0 {
		return nil
	} else if idx.position%entryWidth != 0 {
		return ErrIndexCorrupt
	} else {
		//read last entry
		entry := new(Entry)
		if err := idx.ReadEntryAtFileOffset(entry, idx.position-entryWidth); err != nil {
			return err
		}
		if entry.Offset < idx.baseOffset {
			return ErrIndexCorrupt
		}
		return nil
	}
}

type IndexScanner struct {
	idx    *Index
	entry  *Entry
	offset int64
}

func NewIndexScanner(idx *Index) *IndexScanner {
	return &IndexScanner{idx: idx, entry: &Entry{}}
}

func (s *IndexScanner) Scan() (*Entry, error) {
	err := s.idx.ReadEntryAtLogOffset(s.entry, s.offset)
	if err != nil {
		return nil, err
	}
	if s.entry.Offset == 0 && s.offset != 0 {
		return nil, io.EOF
	}
	s.offset++
	return s.entry, err
}
