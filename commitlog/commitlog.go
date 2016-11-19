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
	segments       []*Segment
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

func (l *CommitLog) Init() error {
	err := os.MkdirAll(l.Path, 0755)
	if err != nil {
		return errors.Wrap(err, "mkdir failed")
	}
	return nil
}

func (l *CommitLog) Open() error {
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

func (l *CommitLog) DeleteAll() error {
	return os.RemoveAll(l.Path)
}

func (l *CommitLog) Append(ms MessageSet) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.checkSplit() {
		if err := l.split(); err != nil {
			return err
		}
	}
	position := l.activeSegment().Position
	offset := l.activeSegment().NextOffset
	ms.PutOffset(offset)
	if _, err := l.activeSegment().Write(ms); err != nil {
		return err
	}
	e := Entry{
		Offset:   offset,
		Position: position,
	}
	if err := l.activeSegment().Index.WriteEntry(e); err != nil {
		return err
	}
	return nil
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
	seg, err := NewSegment(l.Path, l.NewestOffset(), l.SegmentBytes)
	if err != nil {
		return err
	}
	l.segments = append(l.segments, seg)
	l.vActiveSegment.Store(seg)
	return nil
}

func (l *CommitLog) NewestOffset() int64 {
	return l.activeSegment().NextOffset
}

func (l *CommitLog) OldestOffset() int64 {
	return l.segments[0].BaseOffset
}

func (l *CommitLog) activeSegment() *Segment {
	return l.vActiveSegment.Load().(*Segment)
}
