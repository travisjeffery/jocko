package protocol

type OffsetsPartition struct {
	Partition int32
	Timestamp int64 // -1 to receive latest offset, -2 to receive earliest offset
}

type OffsetsTopic struct {
	Topic      string
	Partitions []*OffsetsPartition
}

type OffsetsRequest struct {
	ReplicaID     int32
	Topics        []*OffsetsTopic
	MaxNumOffsets int32
}

func (r *OffsetsRequest) Encode(e PacketEncoder) error {
	var err error
	e.PutInt32(-1)
	err = e.PutArrayLength(len(r.Topics))
	if err != nil {
		return err
	}
	for _, t := range r.Topics {
		err = e.PutString(t.Topic)
		if err != nil {
			return err
		}
		err = e.PutArrayLength(len(t.Partitions))
		if err != nil {
			return err
		}
		for _, p := range t.Partitions {
			e.PutInt32(p.Partition)
			e.PutInt64(p.Timestamp)
		}
	}
	e.PutInt32(r.MaxNumOffsets)
	return nil
}

func (r *OffsetsRequest) Decode(d PacketDecoder) error {
	var err error
	r.ReplicaID, err = d.Int32()
	if err != nil {
		return err
	}
	topicCount, err := d.ArrayLength()
	if err != nil {
		return err
	}
	r.Topics = make([]*OffsetsTopic, topicCount)
	for i := range r.Topics {
		ot := new(OffsetsTopic)
		ot.Topic, err = d.String()
		if err != nil {
			return err
		}
		partitionCount, err := d.ArrayLength()
		if err != nil {
			return err
		}
		ot.Partitions = make([]*OffsetsPartition, partitionCount)
		for j := range ot.Partitions {
			p := new(OffsetsPartition)
			p.Partition, err = d.Int32()
			if err != nil {
				return err
			}
			p.Timestamp, err = d.Int64()
			if err != nil {
				return err
			}
			ot.Partitions[j] = p
		}
		r.Topics[i] = ot
	}
	r.MaxNumOffsets, err = d.Int32()
	return err
}

func (r *OffsetsRequest) Key() int16 {
	return OffsetsKey
}

func (r *OffsetsRequest) Version() int16 {
	return 0
}
