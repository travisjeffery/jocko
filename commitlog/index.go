package commitlog

import (
	"bytes"
	"encoding/binary"
	"os"
	"sync"

	"github.com/pkg/errors"

	"launchpad.net/gommap"
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

type index struct {
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

// relEntry is an Entry relative to the base offset
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

func newIndex(opts options) (idx *index, err error) {
	if opts.bytes == 0 {
		opts.bytes = 10 * 1024 * 1024
	}
	if opts.path == "" {
		return nil, errors.New("path is empty")
	}
	idx = &index{
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

func (idx *index) WriteEntry(entry Entry) (err error) {
	b := new(bytes.Buffer)
	relEntry := newRelEntry(entry, idx.baseOffset)
	if err = binary.Write(b, binary.BigEndian, relEntry); err != nil {
		return errors.Wrap(err, "binary write failed")
	}
	idx.WriteAt(b.Bytes(), idx.position)
	idx.mu.Lock()
	idx.position += entryWidth
	idx.mu.Unlock()
	return nil
}

func (idx *index) ReadEntry(e *Entry, offset int64) error {
	p := make([]byte, entryWidth)
	idx.ReadAt(p, offset)
	b := bytes.NewReader(p)
	rel := &relEntry{}
	err := binary.Read(b, binary.BigEndian, rel)
	if err != nil {
		return errors.Wrap(err, "binary read failed")
	}
	idx.mu.RLock()
	rel.fill(e, idx.baseOffset)
	idx.mu.RUnlock()
	return nil
}

func (idx *index) ReadAt(p []byte, offset int64) (n int) {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	return copy(p, idx.mmap[offset:offset+entryWidth])
}

func (idx *index) Write(p []byte) (n int, err error) {
	return idx.WriteAt(p, idx.position), nil
}

func (idx *index) WriteAt(p []byte, offset int64) (n int) {
	idx.mu.Lock()
	defer idx.mu.Unlock()
	return copy(idx.mmap[offset:offset+entryWidth], p)
}

func (idx *index) Sync() error {
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

func (idx *index) Close() (err error) {
	if err = idx.Sync(); err != nil {
		return
	}
	if err = idx.file.Truncate(idx.position); err != nil {
		return
	}
	return idx.file.Close()
}

func (idx *index) Name() string {
	return idx.file.Name()
}

func (idx *index) TruncateEntries(number int) error {
	idx.mu.Lock()
	defer idx.mu.Unlock()
	if int64(number*entryWidth) > idx.position {
		return errors.New("bad truncate number")
	}
	idx.position = int64(number * entryWidth)
	return nil
}

func (idx *index) SanityCheck() error {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	if idx.position == 0 {
		return nil
	} else if idx.position%entryWidth != 0 {
		return ErrIndexCorrupt
	} else {
		//read last entry
		entry := new(Entry)
		if err := idx.ReadEntry(entry, idx.position-entryWidth); err != nil {
			return err
		}
		if entry.Offset < idx.baseOffset {
			return ErrIndexCorrupt
		}
		return nil
	}
}
