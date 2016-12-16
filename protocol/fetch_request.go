package protocol

type FetchPartition struct {
	Partition   int32
	FetchOffset int64
	MaxBytes    int32
}

type FetchTopic struct {
	Topic      string
	Partitions []*FetchPartition
}

type FetchRequest struct {
	ReplicaID   int32
	MaxWaitTime int32
	MinBytes    int32
	// MaxBytes    int32
	Topics []*FetchTopic
}

func (r *FetchRequest) Encode(e PacketEncoder) error {
	if r.ReplicaID == 0 {
		e.PutInt32(-1) // replica ID is -1 for clients
	} else {
		e.PutInt32(r.ReplicaID)
	}
	e.PutInt32(r.MaxWaitTime)
	e.PutInt32(r.MinBytes)
	// e.PutInt32(r.MaxBytes)
	e.PutArrayLength(len(r.Topics))
	for _, t := range r.Topics {
		e.PutString(t.Topic)
		e.PutArrayLength(len(t.Partitions))
		for _, p := range t.Partitions {
			e.PutInt32(p.Partition)
			e.PutInt64(p.FetchOffset)
			e.PutInt32(p.MaxBytes)
		}
	}
	return nil
}

func (r *FetchRequest) Decode(d PacketDecoder) error {
	var err error
	r.ReplicaID, err = d.Int32()
	if err != nil {
		return err
	}
	r.MaxWaitTime, err = d.Int32()
	if err != nil {
		return err
	}
	r.MinBytes, err = d.Int32()
	if err != nil {
		return err
	}
	// r.MaxBytes, err = d.Int32()
	// if err != nil {
	// 	return err
	// }
	topicCount, err := d.ArrayLength()
	if err != nil {
		return err
	}
	topics := make([]*FetchTopic, topicCount)
	for i := range topics {
		t := &FetchTopic{}
		t.Topic, err = d.String()
		if err != nil {
			return err
		}
		partitionCount, err := d.ArrayLength()
		if err != nil {
			return err
		}
		ps := make([]*FetchPartition, partitionCount)
		for j := range ps {
			p := &FetchPartition{}
			p.Partition, err = d.Int32()
			if err != nil {
				return err
			}
			p.FetchOffset, err = d.Int64()
			if err != nil {
				return err
			}
			p.MaxBytes, err = d.Int32()
			if err != nil {
				return err
			}
			ps[j] = p
		}
		t.Partitions = ps
		topics[i] = t
	}
	r.Topics = topics
	return nil
}

func (r *FetchRequest) Key() int16 {
	return FetchKey
}

func (r *FetchRequest) Version() int16 {
	return 1
}
