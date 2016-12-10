package commitlog

type Cleaner interface {
	Clean([]*Segment) ([]*Segment, error)
}

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

func (c *DeleteCleaner) Clean(segments []*Segment) ([]*Segment, error) {
	if len(segments) == 0 || c.Retention.Bytes == -1 {
		return segments, nil
	}
	cleanedSegments := []*Segment{segments[len(segments)-1]}
	totalBytes := cleanedSegments[0].Position
	if len(segments) > 1 {
		var i int
		for i = len(segments) - 2; i > -1; i-- {
			s := segments[i]
			totalBytes += s.Position
			if totalBytes > c.Retention.Bytes {
				break
			}
			cleanedSegments = append([]*Segment{s}, cleanedSegments...)
		}
		if i > -1 {
			for ; i != 0; i-- {
				s := segments[i]
				if err := s.Delete(); err != nil {
					return nil, err
				}
			}
		}
	}
	return cleanedSegments, nil
}
