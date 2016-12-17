package protocol

type StopReplicaPartition struct {
	Topic     string
	Partition int32
}

type StopReplicaRequest struct {
	ControllerID     int32
	ControllerEpoch  int32
	DeletePartitions bool
	Partitions       []*StopReplicaPartition
}

func (r *StopReplicaRequest) Encode(e PacketEncoder) (err error) {
	e.PutInt32(r.ControllerID)
	e.PutInt32(r.ControllerEpoch)
	if r.DeletePartitions {
		e.PutInt8(1)
	} else {
		e.PutInt8(0)
	}
	if err = e.PutArrayLength(len(r.Partitions)); err != nil {
		return err
	}
	for _, partition := range r.Partitions {
		if err = e.PutString(partition.Topic); err != nil {
			return err
		}
		e.PutInt32(partition.Partition)
	}
	return nil
}

func (r *StopReplicaRequest) Decode(d PacketDecoder) (err error) {
	if r.ControllerID, err = d.Int32(); err != nil {
		return err
	}
	if r.ControllerEpoch, err = d.Int32(); err != nil {
		return err
	}
	if r.DeletePartitions, err = d.Bool(); err != nil {
		return err
	}
	partitionCount, err := d.ArrayLength()
	if err != nil {
		return
	}
	r.Partitions = make([]*StopReplicaPartition, partitionCount)
	for index := range r.Partitions {
		r.Partitions[index] = new(StopReplicaPartition)
		if r.Partitions[index].Topic, err = d.String(); err != nil {
			return err
		}
		if r.Partitions[index].Partition, err = d.Int32(); err != nil {
			return err
		}
	}
	return nil
}

func (r *StopReplicaRequest) Key() int16 {
	return StopReplicaKey
}

func (r *StopReplicaRequest) Version() int16 {
	return 0
}
