package commitlog

import (
	"errors"
	"sort"
)

func findSegment(segments []*segment, offset int64) (*segment, int) {
	idx := sort.Search(len(segments), func(i int) bool {
		return segments[i].baseOffset > offset
	}) - 1

	if idx < 0 {
		return nil, idx
	}

	return segments[idx], idx
}

func findEntry(segment *segment, offset int64) (e *entry, err error) {
	e = &entry{}
	idx := sort.Search(int(segment.index.bytes/entryWidth), func(i int) bool {
		_ = segment.index.ReadEntry(e, int64(i*entryWidth))
		return int64(e.Offset) > offset || e.Offset == 0
	})
	if idx == -1 {
		return nil, errors.New("entry not found")
	}
	return e, nil
}
