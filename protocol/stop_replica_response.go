package protocol

type StopReplicaResponsePartition struct {
	Topic     string
	Partition int32
	ErrorCode Error
}

type StopReplicaResponse struct {
	ErrorCode  Error
	Partitions []*StopReplicaResponsePartition
}

func (r *StopReplicaResponse) Encode(e PacketEncoder) (err error) {
	e.PutInt16FromError(r.ErrorCode)
	if err = e.PutArrayLength(len(r.Partitions)); err != nil {
		return err
	}
	for _, partition := range r.Partitions {
		if err = e.PutString(partition.Topic); err != nil {
			return err
		}
		e.PutInt32(partition.Partition)
		e.PutInt16FromError(partition.ErrorCode)
	}
	return nil
}

func (r *StopReplicaResponse) Decode(d PacketDecoder, version int16) (err error) {
	if r.ErrorCode, err = d.Int16AsError(); err != nil {
		return err
	}
	partitionCount, err := d.ArrayLength()
	if err != nil {
		return err
	}
	r.Partitions = make([]*StopReplicaResponsePartition, partitionCount)
	for i := range r.Partitions {
		if r.Partitions[i].Topic, err = d.String(); err != nil {
			return err
		}
		if r.Partitions[i].Partition, err = d.Int32(); err != nil {
			return err
		}
		if r.Partitions[i].ErrorCode, err = d.Int16AsError(); err != nil {
			return err
		}
	}
	return err
}
