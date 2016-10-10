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
	Options
	name           string
	mu             sync.RWMutex
	segments       []*segment
	vActiveSegment atomic.Value
}

type Options struct {
	Path         string
	SegmentBytes int64
}

func New(opts Options) (*CommitLog, error) {
	if opts.Path == "" {
		return nil, errors.New("path is empty")
	}

	if opts.SegmentBytes == 0 {
		// TODO default here
	}

	path, _ := filepath.Abs(opts.Path)
	l := &CommitLog{
		Options: opts,
		name:    filepath.Base(path),
	}

	return l, nil
}

func (l *CommitLog) init() error {
	err := os.MkdirAll(l.Path, 0755)
	if err != nil {
		return errors.Wrap(err, "mkdir failed")
	}
	return nil
}

func (l *CommitLog) open() error {
	_, err := ioutil.ReadDir(l.Path)
	if err != nil {
		return errors.Wrap(err, "read dir failed")
	}

	activeSegment, err := NewSegment(l.Path, 0, l.SegmentBytes)
	if err != nil {
		return err
	}
	l.vActiveSegment.Store(activeSegment)

	l.segments = append(l.segments, activeSegment)

	return nil
}

func (l *CommitLog) deleteAll() error {
	return os.RemoveAll(l.Path)
}

func (l *CommitLog) Write(p []byte) (n int, err error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.checkSplit() {
		if err = l.split(); err != nil {
			return 0, err
		}
	}

	return l.activeSegment().Write(p)
}

func (l *CommitLog) Read(p []byte) (n int, err error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.activeSegment().Read(p)
}

func (l *CommitLog) checkSplit() bool {
	return l.activeSegment().IsFull()
}

func (l *CommitLog) split() error {
	seg, err := NewSegment(l.Path, l.newestOffset(), l.SegmentBytes)
	if err != nil {
		return err
	}
	l.segments = append(l.segments, seg)
	l.vActiveSegment.Store(seg)
	return nil
}

func (l *CommitLog) newestOffset() int64 {
	return l.activeSegment().NewestOffset()
}

func (l *CommitLog) activeSegment() *segment {
	return l.vActiveSegment.Load().(*segment)
}
