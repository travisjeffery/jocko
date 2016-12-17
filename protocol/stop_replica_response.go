package protocol

type StopReplicaPartitionAndErrorCode struct {
	StopReplicaPartition
	ErrorCode int16
}

type StopReplicaResponse struct {
	ErrorCode  int16
	Partitions []*StopReplicaPartitionAndErrorCode
}

func (r *StopReplicaResponse) Encode(e PacketEncoder) (err error) {
	e.PutInt16(r.ErrorCode)
	if err = e.PutArrayLength(len(r.Partitions)); err != nil {
		return
	}
	for _, partition := range r.Partitions {
		if err = e.PutString(partition.Topic); err != nil {
			return
		}
		e.PutInt32(partition.Partition)
		e.PutInt16(partition.ErrorCode)
	}
	return
}

func (r *StopReplicaResponse) Decode(d PacketDecoder) (err error) {
	if r.ErrorCode, err = d.Int16(); err != nil {
		return
	}
	length, err := d.ArrayLength()
	if err != nil {
		return
	}
	r.Partitions = make([]*StopReplicaPartitionAndErrorCode, length)
	for index := range r.Partitions {
		if r.Partitions[index].Topic, err = d.String(); err != nil {
			return
		}
		if r.Partitions[index].Partition, err = d.Int32(); err != nil {
			return
		}
		if r.Partitions[index].ErrorCode, err = d.Int16(); err != nil {
			return
		}
	}
	return
}
