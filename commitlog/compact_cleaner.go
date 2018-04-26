package commitlog

import (
	"fmt"

	"github.com/cespare/xxhash"
)

type CompactCleaner struct {
	// map from key hash to offset
	m map[uint64]int64
}

func NewCompactCleaner() *CompactCleaner {
	return &CompactCleaner{
		m: make(map[uint64]int64),
	}
}

func (c *CompactCleaner) Clean(segments []*Segment) (cleaned []*Segment, err error) {
	if len(segments) == 0 {
		return segments, nil
	}

	var ss *SegmentScanner
	var ms MessageSet
	var offset int64

	// build the map of keys to their latest offsets
	for _, segment := range segments {
		ss = NewSegmentScanner(segment)

		for ms, err = ss.Scan(); err == nil; ms, err = ss.Scan() {
			offset = ms.Offset()
			for _, msg := range ms.Messages() {
				fmt.Printf("msg: %s\n", msg)
				c.m[Hash(msg.Key())] = offset
			}
		}
	}

	// TODO: handle joining segments when they're smaller than max segment size
	for _, ds := range segments {
		ss = NewSegmentScanner(ds)

		cs, err := NewSegment(ds.path, ds.BaseOffset, ds.maxBytes, cleanedSuffix)
		if err != nil {
			return nil, err
		}
		fmt.Printf("\n\n\ncleaning\n\n\n")
		for ms, err = ss.Scan(); err == nil; ms, err = ss.Scan() {
			var retain bool
			offset = ms.Offset()
			for _, msg := range ms.Messages() {
				if c.m[Hash(msg.Key())] <= offset {
					retain = true
				}
			}
			fmt.Printf("retain %v %s\n", retain, ms)
			if retain {
				if _, err = cs.Write(ms); err != nil {
					return nil, err
				}
			}
		}

		fmt.Println("yo")
		if err = cs.Replace(ds); err != nil {
			return nil, err
		}

		fmt.Println(cs.path)

		cleaned = append(cleaned, cs)
	}

	// TODO: swap segments in place

	return cleaned, nil
}

func Hash(b []byte) uint64 {
	h := xxhash.New()
	if _, err := h.Write(b); err != nil {
		panic(err)
	}
	return h.Sum64()
}
