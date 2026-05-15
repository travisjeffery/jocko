package protocol

type OffsetCommitRequest struct {
	APIVersion int16

	GroupID       string
	GenerationID  int32
	MemberID      string
	RetentionTime int64
	Topics        []OffsetCommitTopicRequest
}

type OffsetCommitTopicRequest struct {
	Topic      string
	Partitions []OffsetCommitPartitionRequest
}

type OffsetCommitPartitionRequest struct {
	Partition int32
	Offset    int64
	Timestamp int64
	Metadata  *string
}

func (r *OffsetCommitRequest) Encode(e PacketEncoder) (err error) {
	if err = e.PutString(r.GroupID); err != nil {
		return err
	}
	if r.APIVersion >= 1 {
		e.PutInt32(r.GenerationID)
		if err := e.PutString(r.MemberID); err != nil {
			return err
		}
	}
	if r.APIVersion >= 2 {
		e.PutInt64(r.RetentionTime)
	}
	if err := e.PutArrayLength(len(r.Topics)); err != nil {
		return err
	}
	for _, t := range r.Topics {
		if err := e.PutString(t.Topic); err != nil {
			return err
		}
		if err := e.PutArrayLength(len(t.Partitions)); err != nil {
			return err
		}
		for _, p := range t.Partitions {
			e.PutInt32(p.Partition)
			e.PutInt64(p.Offset)
			if r.APIVersion == 1 {
				e.PutInt64(p.Timestamp)
			}
			metadata := ""
			if p.Metadata != nil {
				metadata = *p.Metadata
			}
			if err := e.PutString(metadata); err != nil {
				return err
			}
		}
	}
	return nil
}

func (r *OffsetCommitRequest) Decode(d PacketDecoder, version int16) (err error) {
	r.APIVersion = version

	if r.GroupID, err = d.String(); err != nil {
		return err
	}
	if version >= 1 {
		if r.GenerationID, err = d.Int32(); err != nil {
			return err
		}
		if r.MemberID, err = d.String(); err != nil {
			return err
		}
	}
	if version >= 2 {
		if r.RetentionTime, err = d.Int64(); err != nil {
			return err
		}
	}
	topicCount, err := d.ArrayLength()
	if err != nil {
		return err
	}
	r.Topics = make([]OffsetCommitTopicRequest, topicCount)
	for i := range r.Topics {
		if r.Topics[i].Topic, err = d.String(); err != nil {
			return err
		}
		partitionCount, err := d.ArrayLength()
		if err != nil {
			return err
		}
		r.Topics[i].Partitions = make([]OffsetCommitPartitionRequest, partitionCount)
		for j := range r.Topics[i].Partitions {
			if r.Topics[i].Partitions[j].Partition, err = d.Int32(); err != nil {
				return err
			}
			if r.Topics[i].Partitions[j].Offset, err = d.Int64(); err != nil {
				return err
			}
			if version == 1 {
				if r.Topics[i].Partitions[j].Timestamp, err = d.Int64(); err != nil {
					return err
				}
			}
			metadata, err := d.String()
			if err != nil {
				return err
			}
			r.Topics[i].Partitions[j].Metadata = &metadata
		}
	}
	return nil
}

func (r *OffsetCommitRequest) Version() int16 {
	return r.APIVersion
}

func (r *OffsetCommitRequest) Key() int16 {
	return OffsetCommitKey

}
