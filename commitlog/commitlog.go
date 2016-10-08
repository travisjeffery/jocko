package commitlog

import (
	"io/ioutil"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"

	"github.com/pkg/errors"
)

type CommitLog struct {
	name           string
	path           string
	mu             sync.RWMutex
	segments       []*segment
	vActiveSegment atomic.Value
}

type Reader struct {
	segment *segment
	mu      sync.Mutex
	offset  int64
}

func (r *Reader) Read(p []byte) (n int, err error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	return r.segment.ReadAt(p, r.offset)
}

func (l *CommitLog) NewReader(offset int64) (r *Reader) {
	return &Reader{
		segment: l.segments[0],
		offset:  offset,
	}
}

func New(path string) (*CommitLog, error) {
	err := os.MkdirAll(path, 0755)
	if err != nil {
		return nil, errors.Wrap(err, "mkdir failed")
	}

	path, _ = filepath.Abs(path)
	l := &CommitLog{
		name: filepath.Base(path),
		path: path,
	}
	err = l.open()

	return l, err
}

func (l *CommitLog) open() error {
	_, err := ioutil.ReadDir(l.path)
	if err != nil {
		return errors.Wrap(err, "read dif failed")
	}

	activeSegment, err := NewSegment(l.path, 0)
	if err != nil {
		return err
	}
	l.vActiveSegment.Store(activeSegment)

	l.segments = append(l.segments, activeSegment)

	return nil
}

func (l *CommitLog) deleteAll() error {
	return os.RemoveAll(l.path)
}

func (l *CommitLog) Write(p []byte) (n int, err error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.activeSegment().Write(p)
}

func (l *CommitLog) Read(p []byte) (n int, err error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.activeSegment().Read(p)
}

func (l *CommitLog) activeSegment() *segment {
	return l.vActiveSegment.Load().(*segment)
}
