package protocol

type PartitionState struct {
	Topic           string
	Partition       int32
	ControllerEpoch int32
	Leader          int32
	LeaderEpoch     int32
	ISR             []int32
	ZKVersion       int32
	Replicas        []int32
}

type LiveLeader struct {
	ID   int32
	Host string
	Port int32
}

type LeaderAndISRRequest struct {
	ControllerID    int32
	ControllerEpoch int32
	PartitionStates []*PartitionState
	LiveLeaders     []*LiveLeader
}

func (r *LeaderAndISRRequest) Encode(e PacketEncoder) error {
	var err error
	e.PutInt32(r.ControllerID)
	e.PutInt32(r.ControllerEpoch)
	for _, p := range r.PartitionStates {
		if err = e.PutString(p.Topic); err != nil {
			return err
		}
		e.PutInt32(p.Partition)
		e.PutInt32(p.ControllerEpoch)
		e.PutInt32(p.Leader)
		e.PutInt32(p.LeaderEpoch)
		if err = e.PutInt32Array(p.ISR); err != nil {
			return err
		}
		e.PutInt32(p.ZKVersion) // TODO: hardcode this?
		if err = e.PutInt32Array(p.Replicas); err != nil {
			return err
		}
	}
	return nil
}

func (r *LeaderAndISRRequest) Decode(d PacketDecoder) error {
	var err error
	if r.ControllerID, err = d.Int32(); err != nil {
		return err
	}
	if r.ControllerEpoch, err = d.Int32(); err != nil {
		return err
	}
	stateCount, err := d.ArrayLength()
	if err != nil {
		return err
	}
	r.PartitionStates = make([]*PartitionState, stateCount)
	for i := range r.PartitionStates {
		ps := new(PartitionState)
		if ps.Topic, err = d.String(); err != nil {
			return err
		}
		if ps.Partition, err = d.Int32(); err != nil {
			return err
		}
		if ps.Leader, err = d.Int32(); err != nil {
			return err
		}
		if ps.LeaderEpoch, err = d.Int32(); err != nil {
			return err
		}
		if ps.ISR, err = d.Int32Array(); err != nil {
			return err
		}
		if ps.ZKVersion, err = d.Int32(); err != nil {
			return err
		}
		if ps.Replicas, err = d.Int32Array(); err != nil {
			return err
		}
		r.PartitionStates[i] = ps
	}
	leaderCount, err := d.ArrayLength()
	r.LiveLeaders = make([]*LiveLeader, leaderCount)
	for i := range r.LiveLeaders {
		ll := new(LiveLeader)
		if ll.ID, err = d.Int32(); err != nil {
			return err
		}
		if ll.Host, err = d.String(); err != nil {
			return err
		}
		if ll.Port, err = d.Int32(); err != nil {
			return err
		}
		r.LiveLeaders[i] = ll
	}
	return nil
}

func (r *LeaderAndISRRequest) Key() int16 {
	return LeaderAndISRKey
}

func (r *LeaderAndISRRequest) Version() int16 {
	return 0
}
