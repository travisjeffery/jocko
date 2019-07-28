package commitlog

type cleaner interface {
	Clean([]*segment) ([]*segment, error)
}

// The delete cleaner implements the delete cleanup policy which
// deletes old log segments.

type DeleteCleaner struct {
	Retention struct {
		Bytes int64
	}
}

func NewDeleteCleaner(bytes int64) *DeleteCleaner {
	c := &DeleteCleaner{}
	c.Retention.Bytes = bytes
	return c
}

func (c *DeleteCleaner) Clean(segments []*segment) ([]*segment, error) {
	if len(segments) == 0 || c.Retention.Bytes == -1 {
		return segments, nil
	}
	// we start at the most recent segment and work our way backwards until we meet the
	// retention size.
	cleanedSegments := []*segment{segments[len(segments)-1]}
	totalBytes := cleanedSegments[0].Size()
	if len(segments) > 1 {
		var i int
		for i = len(segments) - 2; i > -1; i-- {
			s := segments[i]
			totalBytes += s.Position
			if totalBytes > c.Retention.Bytes {
				break
			}
			cleanedSegments = append([]*segment{s}, cleanedSegments...)
		}
		if i > -1 {
			for ; i > -1; i-- {
				s := segments[i]
				if err := s.Delete(); err != nil {
					return nil, err
				}
			}
		}
	}
	return cleanedSegments, nil
}
