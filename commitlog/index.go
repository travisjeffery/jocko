package commitlog

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"sync"

	"github.com/pkg/errors"

	"github.com/tysontate/gommap"
)

var (
	// ErrIndexCorrupts is returned when the index is corrupted and needs rebuilt.
	ErrIndexCorrupt = errors.New("corrupt index file")
)

const (
	offWidth = 4
	offOff   = 0

	posWidth = 4
	posOff   = offWidth

	entryWidth = offWidth + posWidth
)

type index struct {
	mmap gommap.MMap
	file *os.File
	mu   sync.RWMutex
	// position of next write
	pos int64
	// base offset
	baseOff int64
}

// entry is relative to hte index's base offset.
type entry struct {
	Off int32
	Pos int32
}

func (e entry) IsZero() bool {
	return e.Off == 0 &&
		e.Pos == 0
}

// newIndex returns the created index and its last entry.
func newIndex(f *os.File) (*index, entry, error) {
	idx := &index{
		file: f,
	}
	var err error
	if idx.mmap, err = gommap.Map(
		idx.file.Fd(),
		gommap.PROT_READ|gommap.PROT_WRITE,
		gommap.MAP_SHARED,
	); err != nil {
		return nil, entry{}, err
	}
	is := &indexScanner{idx: idx}
	for is.Scan() {
		idx.pos += entryWidth
	}
	return idx, is.Entry(), nil
}

// Read returns the index entry at the given offset.
func (i *index) Read(off int64) (e entry, err error) {
	pos := off * entryWidth
	if int64(len(i.mmap)) < pos+entryWidth {
		return e, io.EOF
	}
	p := make([]byte, entryWidth)
	copy(p, i.mmap[pos:pos+entryWidth])
	b := bytes.NewReader(p)
	if err = binary.Read(b, Encoding, &e); err != nil {
		return e, err
	}
	return e, nil
}

// Write the given index entry.
func (i *index) Write(e entry) error {
	if int64(len(i.mmap)) < i.pos+entryWidth {
		fmt.Println("heyheyhey", len(i.mmap), i.pos+entryWidth)
		return io.EOF
	}
	b := new(bytes.Buffer)
	if err := binary.Write(b, Encoding, e); err != nil {
		return err
	}
	n := copy(i.mmap[i.pos:i.pos+entryWidth], b.Bytes())
	i.pos += int64(n)
	return nil
}

func (idx *index) Close() (err error) {
	if err = idx.sync(); err != nil {
		return err
	}
	return idx.file.Close()
}

func (idx *index) sync() error {
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

type indexScanner struct {
	// idx must be set
	idx *index
	cur entry
	off int64
	err error
}

func (is *indexScanner) Scan() bool {
	var e entry
	if is.err != nil {
		return false
	}
	if e, is.err = is.idx.Read(is.off); is.err != nil {
		return false
	}
	is.cur = e
	is.off++
	return true
}

func (is *indexScanner) Entry() entry {
	return is.cur
}

func (is *indexScanner) Err() error {
	return is.err
}
