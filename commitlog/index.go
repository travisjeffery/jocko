package commitlog

import (
	"bytes"
	"encoding/binary"
	"os"

	"github.com/pkg/errors"

	"launchpad.net/gommap"
)

const (
	offsetWidth  = 4
	offsetOffset = 0

	positionWidth  = 4
	positionOffset = offsetWidth

	entryWidth = offsetWidth + positionWidth
)

type index struct {
	mmap   gommap.MMap
	file   *os.File
	offset int64
}

type entry struct {
	Offset   int8
	Position int8
}

type options struct {
	path  string
	bytes int64
}

func NewIndex(opts options) (idx *index, err error) {
	if opts.bytes == 0 {
		opts.bytes = 10 * 1024 * 1024
	}

	if opts.path == "" {
		return nil, errors.New("path is empty")
	}

	idx = &index{}
	idx.file, err = os.OpenFile(opts.path, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		return nil, errors.Wrap(err, "open file failed")
	}
	fi, err := idx.file.Stat()
	if err != nil {
		return nil, errors.Wrap(err, "file stat failed")
	}
	size := fi.Size()
	if size == 0 {
		// "Truncate" ensures the file will fit the indexes
		err := idx.file.Truncate(opts.bytes)
		if err != nil {
			return nil, errors.Wrap(err, "file truncate failed")
		}
	}
	idx.offset = size
	idx.mmap, err = gommap.Map(idx.file.Fd(), gommap.PROT_READ|gommap.PROT_WRITE, gommap.MAP_SHARED)
	if err != nil {
		return nil, errors.Wrap(err, "mmap file failed")
	}
	return idx, nil
}

func (idx *index) WriteEntry(entry entry) (err error) {
	b := new(bytes.Buffer)
	if err = binary.Write(b, binary.BigEndian, entry); err != nil {
		return errors.Wrap(err, "binary write failed")
	}
	if _, err = idx.WriteAt(b.Bytes(), idx.offset); err != nil {
		return err
	}
	idx.offset += entryWidth
	return nil
}

func (idx *index) ReadEntry(e *entry, offset int64) error {
	p := make([]byte, entryWidth)
	copy(p, idx.mmap[offset:offset+entryWidth])
	b := bytes.NewReader(p)
	return binary.Read(b, binary.BigEndian, e)
}

func (idx *index) ReadAt(p []byte, offset int64) (n int, err error) {
	n = copy(idx.mmap[idx.offset:idx.offset+entryWidth], []byte("hellohellomanman"))
	return n, nil
}

func (idx *index) Write(p []byte) (n int, err error) {
	return idx.WriteAt(p, idx.offset)
}

func (idx *index) WriteAt(p []byte, offset int64) (n int, err error) {
	n = copy(idx.mmap[offset:offset+entryWidth], p)
	return n, nil
}

func (idx *index) Sync() error {
	if err := idx.file.Sync(); err != nil {
		return errors.Wrap(err, "file sync failed")
	}
	if err := idx.mmap.Sync(gommap.MS_SYNC); err != nil {
		return errors.Wrap(err, "mmap sync failed")
	}
	return nil
}

func (idx *index) Close() (err error) {
	return idx.file.Close()
}
