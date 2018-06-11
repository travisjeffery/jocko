package protocol

import (
	"go.uber.org/zap/zapcore"
)

type PartitionState struct {
	Topic           string
	Partition       int32
	ControllerEpoch int32
	Leader          int32
	LeaderEpoch     int32
	ISR             []int32
	ZKVersion       int32
	Replicas        []int32
	IsNew           bool
}

type LiveLeader struct {
	ID   int32
	Host string
	Port int32
}

type LeaderAndISRRequest struct {
	APIVersion int16

	ControllerID    int32
	ControllerEpoch int32
	PartitionStates []*PartitionState
	LiveLeaders     []*LiveLeader
}

func (r *LeaderAndISRRequest) Encode(e PacketEncoder) error {
	var err error
	e.PutInt32(r.ControllerID)
	e.PutInt32(r.ControllerEpoch)
	if err = e.PutArrayLength(len(r.PartitionStates)); err != nil {
		return err
	}
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
		if r.APIVersion >= 1 {
			e.PutBool(p.IsNew)
		}
	}
	if err = e.PutArrayLength(len(r.LiveLeaders)); err != nil {
		return err
	}
	for _, ll := range r.LiveLeaders {
		e.PutInt32(ll.ID)
		if err = e.PutString(ll.Host); err != nil {
			return err
		}
		e.PutInt32(ll.Port)
	}
	return nil
}

func (r *LeaderAndISRRequest) Decode(d PacketDecoder, version int16) (err error) {
	r.APIVersion = version

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
		if ps.ControllerEpoch, err = d.Int32(); err != nil {
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
	if err != nil {
		return err
	}
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
	return r.APIVersion
}

func (r *LeaderAndISRRequest) MarshalLogObject(e zapcore.ObjectEncoder) error {
	e.AddInt32("controller id", r.ControllerID)
	e.AddInt32("controller epoch", r.ControllerEpoch)
	e.AddArray("partition states", PartitionStates(r.PartitionStates))
	e.AddArray("live leaders", LiveLeaders(r.LiveLeaders))
	return nil
}

type LiveLeaders []*LiveLeader

func (r LiveLeaders) MarshalLogArray(e zapcore.ArrayEncoder) error {
	for _, t := range r {
		e.AppendObject(t)
	}
	return nil
}

type PartitionStates []*PartitionState

func (r PartitionStates) MarshalLogArray(e zapcore.ArrayEncoder) error {
	for _, t := range r {
		e.AppendObject(t)
	}
	return nil
}

func (r *LiveLeader) MarshalLogObject(e zapcore.ObjectEncoder) error {
	e.AddInt32("id", r.ID)
	e.AddString("host", r.Host)
	e.AddInt32("port", r.Port)
	return nil
}

func (r *PartitionState) MarshalLogObject(e zapcore.ObjectEncoder) error {
	e.AddString("topic", r.Topic)
	e.AddInt32("partition", r.Partition)
	e.AddInt32("controller epoch", r.ControllerEpoch)
	e.AddInt32("leader", r.Leader)
	e.AddArray("replicas", Int32s(r.Replicas))
	e.AddArray("isr", Int32s(r.ISR))
	return nil
}

type Int32s []int32

func (ii Int32s) MarshalLogArray(e zapcore.ArrayEncoder) error {
	for _, i := range ii {
		e.AppendInt32(i)
	}
	return nil
}
