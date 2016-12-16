package protocol

type LeaderAndISRPartition struct {
	Topic     string
	Partition int32
	ErrorCode int16
}

type LeaderAndISRResponse struct {
	ErrorCode  int16
	Partitions []*LeaderAndISRPartition
}

func (r *LeaderAndISRResponse) Encode(e PacketEncoder) error {
	var err error
	e.PutInt16(r.ErrorCode)
	for _, p := range r.Partitions {
		if err = e.PutString(p.Topic); err != nil {
			return err
		}
		e.PutInt32(p.Partition)
		e.PutInt16(p.ErrorCode)
	}
	return nil
}

func (r *LeaderAndISRResponse) Decode(d PacketDecoder) error {
	var err error
	if r.ErrorCode, err = d.Int16(); err != nil {
		return err
	}
	partitionCount, err := d.ArrayLength()
	if err != nil {
		return err
	}
	r.Partitions = make([]*LeaderAndISRPartition, partitionCount)
	for i := range r.Partitions {
		p := new(LeaderAndISRPartition)
		if p.Topic, err = d.String(); err != nil {
			return err
		}
		if p.Partition, err = d.Int32(); err != nil {
			return err
		}
		if p.ErrorCode, err = d.Int16(); err != nil {
			return err
		}
		r.Partitions[i] = p
	}
	return nil
}

func (r *LeaderAndISRResponse) Version() int16 {
	return 0
}

func (r *LeaderAndISRResponse) Key() int16 {
	return 4
}
