package protocol

type OffsetsPartition struct {
	Partition     int32
	Timestamp     int64 // -1 to receive latest offset, -2 to receive earliest offset
	MaxNumOffsets int32
}

type OffsetsTopic struct {
	Topic      string
	Partitions []*OffsetsPartition
}

type OffsetsRequest struct {
	APIVersion int16

	ReplicaID      int32
	IsolationLevel int8
	Topics         []*OffsetsTopic
}

func (r *OffsetsRequest) Encode(e PacketEncoder) (err error) {
	e.PutInt32(-1)
	if r.APIVersion >= 2 {
		e.PutInt8(r.IsolationLevel)
	}
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

			if r.APIVersion == 0 {
				e.PutInt32(p.MaxNumOffsets)
			}
		}
	}
	return nil
}

func (r *OffsetsRequest) Decode(d PacketDecoder, version int16) (err error) {
	r.APIVersion = version

	r.ReplicaID, err = d.Int32()
	if err != nil {
		return err
	}
	if version >= 2 {
		r.IsolationLevel, err = d.Int8()
		if err != nil {
			return err
		}
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
			if version == 0 {
				p.MaxNumOffsets, err = d.Int32()
				if err != nil {
					return err
				}
			}
			ot.Partitions[j] = p
		}
		r.Topics[i] = ot
	}
	return err
}

func (r *OffsetsRequest) Key() int16 {
	return OffsetsKey
}

func (r *OffsetsRequest) Version() int16 {
	return r.APIVersion
}
