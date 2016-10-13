package commitlog

import "sort"

func findSegment(segments []*segment, offset int64) (*segment, int) {
	idx := sort.Search(len(segments), func(i int) bool {
		return segments[i].baseOffset > offset
	}) - 1

	if idx < 0 {
		return nil, idx
	}

	return segments[idx], idx
}
