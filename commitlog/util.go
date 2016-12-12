package commitlog

import "sort"

func findSegment(segments []*Segment, offset int64) (*Segment, int) {
	idx := sort.Search(len(segments), func(i int) bool {
		return segments[i].BaseOffset > offset
	}) - 1

	if idx < 0 {
		return nil, idx
	}

	return segments[idx], idx
}

func roundDown(total, factor int64) int64 {
	return factor * (total / factor)
}
