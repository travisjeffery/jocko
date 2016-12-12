package commitlog

import (
	"io/ioutil"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/pkg/errors"
)

var (
	ErrSegmentNotFound = errors.New("segment not found")
)

type CommitLog struct {
	Options
	cleaner        Cleaner
	name           string
	mu             sync.RWMutex
	segments       []*Segment
	vActiveSegment atomic.Value
}

type Options struct {
	Path            string
	MaxSegmentBytes int64
	MaxLogBytes     int64
}

func New(opts Options) (*CommitLog, error) {
	if opts.Path == "" {
		return nil, errors.New("path is empty")
	}

	if opts.MaxSegmentBytes == 0 {
		// TODO default here
	}

	path, _ := filepath.Abs(opts.Path)
	l := &CommitLog{
		Options: opts,
		name:    filepath.Base(path),
		cleaner: NewDeleteCleaner(opts.MaxLogBytes),
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
	files, err := ioutil.ReadDir(l.Path)
	if err != nil {
		return errors.Wrap(err, "read dir failed")
	}
	for _, file := range files {
		if !strings.HasSuffix(file.Name(), ".log") {
			continue
		}
		offsetStr := strings.TrimSuffix(file.Name(), ".log")
		baseOffset, err := strconv.Atoi(offsetStr)
		segment, err := NewSegment(l.Path, int64(baseOffset), l.MaxSegmentBytes)
		if err != nil {
			return err
		}
		l.segments = append(l.segments, segment)
	}
	if len(l.segments) == 0 {
		segment, err := NewSegment(l.Path, 0, l.MaxSegmentBytes)
		if err != nil {
			return err
		}
		l.segments = append(l.segments, segment)
	}
	l.vActiveSegment.Store(l.segments[len(l.segments)-1])
	return nil
}

func (l *CommitLog) Append(ms MessageSet) (offset int64, err error) {
	if l.checkSplit() {
		if err := l.split(); err != nil {
			return offset, err
		}
	}
	position := l.activeSegment().Position
	offset = l.activeSegment().NextOffset
	ms.PutOffset(offset)
	if _, err := l.activeSegment().Write(ms); err != nil {
		return offset, err
	}
	e := Entry{
		Offset:   offset,
		Position: position,
	}
	if err := l.activeSegment().Index.WriteEntry(e); err != nil {
		return offset, err
	}
	return offset, nil
}

func (l *CommitLog) Read(p []byte) (n int, err error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.activeSegment().Read(p)
}
func (l *CommitLog) NewestOffset() int64 {
	return l.activeSegment().NextOffset
}

func (l *CommitLog) OldestOffset() int64 {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.segments[0].BaseOffset
}

func (l *CommitLog) activeSegment() *Segment {
	return l.vActiveSegment.Load().(*Segment)
}

func (l *CommitLog) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	for _, segment := range l.segments {
		if err := segment.Close(); err != nil {
			return err
		}
	}
	return nil
}

func (l *CommitLog) DeleteAll() error {
	if err := l.Close(); err != nil {
		return err
	}
	return os.RemoveAll(l.Path)
}

func (l *CommitLog) TruncateTo(offset int64) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	var segments []*Segment
	for _, segment := range l.segments {
		if segment.BaseOffset < offset {
			if err := segment.Delete(); err != nil {
				return err
			}
		} else {
			segments = append(segments, segment)
		}
	}
	l.segments = segments
	return nil
}

func (l *CommitLog) Segments() []*Segment {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.segments
}

func (l *CommitLog) checkSplit() bool {
	return l.activeSegment().IsFull()
}

func (l *CommitLog) split() error {
	segment, err := NewSegment(l.Path, l.NewestOffset(), l.MaxSegmentBytes)
	if err != nil {
		return err
	}
	l.mu.Lock()
	segments := append(l.segments, segment)
	segments, err = l.cleaner.Clean(segments)
	if err != nil {
		return err
	}
	l.segments = segments
	l.mu.Unlock()
	l.vActiveSegment.Store(segment)
	return nil
}
