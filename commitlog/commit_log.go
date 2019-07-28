package commitlog

import (
	"encoding/binary"
	"io/ioutil"
	"os"
	"path"
	"sort"
	"strconv"
	"strings"
	"sync"

	"github.com/pkg/errors"
)

var (
	ErrSegmentNotFound = errors.New("segment not found")
	Encoding           = binary.BigEndian
)

type CleanupPolicy string

const (
	DeleteCleanupPolicy  = "delete"
	CompactCleanupPolicy = "compact"

	LogFileSuffix   = ".log"
	IndexFileSuffix = ".index"
)

type CommitLog struct {
	mu sync.RWMutex

	Dir    string
	Config Config

	cleaner       cleaner
	segments      []*segment
	activeSegment *segment
}

type Config struct {
	// MaxSegmentBytes is the max number of bytes a segment can contain, once the limit is hit a
	// new segment will be split off.
	MaxSegmentBytes int64
	MaxIndexBytes   int64
	MaxLogBytes     int64
	CleanupPolicy   CleanupPolicy
}

func New(dir string, c Config) (*CommitLog, error) {
	if dir == "" {
		return nil, errors.New("path is empty")
	}

	// create dir if it doesn't exist yet
	err := os.MkdirAll(dir, 0755)
	if err != nil {
		return nil, err
	}

	if c.MaxSegmentBytes == 0 {
		c.MaxSegmentBytes = 1073741824
	}
	if c.MaxIndexBytes == 0 {
		c.MaxIndexBytes = 10485760
	}

	if c.CleanupPolicy == "" {
		c.CleanupPolicy = DeleteCleanupPolicy
	}

	var cleaner cleaner
	if c.CleanupPolicy == DeleteCleanupPolicy {
		cleaner = NewDeleteCleaner(c.MaxLogBytes)
	} else {
		cleaner = NewCompactCleaner()
	}

	l := &CommitLog{
		Dir:     dir,
		Config:  c,
		cleaner: cleaner,
	}

	files, err := ioutil.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	var baseOffsets []int64
	for _, file := range files {
		off, _ := strconv.ParseInt(trimSuffix(file.Name()), 10, 0)
		baseOffsets = append(baseOffsets, off)
	}
	sort.Slice(baseOffsets, func(i, j int) bool {
		return baseOffsets[i] < baseOffsets[j]
	})
	for i := 0; i < len(baseOffsets); i++ {
		if err = l.newSegment(baseOffsets[i]); err != nil {
			return nil, err
		}
		// The baseOffset slice contains dup offsets for the index and log files of the
		// segment so we skip the dup.
		i++
	}
	if l.segments == nil {
		if err = l.newSegment(0); err != nil {
			return nil, err
		}
	}

	return l, nil
}

func (l *CommitLog) newSegment(baseOffset int64) error {
	s, err := newSegment(l.Dir, baseOffset, l.Config)
	if err != nil {
		return err
	}
	l.segments = append(l.segments, s)
	l.activeSegment = s
	return nil
}

func (l *CommitLog) Append(b []byte) (offset int64, err error) {
	ms := MessageSet(b)
	off := l.activeSegment.nextOffset
	next, _, err := l.activeSegment.Append(b)
	if err != nil {
		return 0, err
	}
	if l.activeSegment.IsMaxed() {
		err = l.newSegment(next)
	}
	return off, err
}

func (l *CommitLog) NewestOffset() int64 {
	return l.activeSegment.nextOffset
}

func (l *CommitLog) OldestOffset() int64 {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.segments[0].baseOffset
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

func (l *CommitLog) Delete() error {
	if err := l.Close(); err != nil {
		return err
	}
	return os.RemoveAll(l.Dir)
}

func (l *CommitLog) Truncate(offset int64) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	var segments []*segment
	for _, segment := range l.segments {
		if segment.baseOffset < offset {
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

func trimSuffix(name string) string {
	return strings.TrimSuffix(name, path.Ext(name))
}
