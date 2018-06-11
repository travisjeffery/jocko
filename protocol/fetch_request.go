package protocol

import (
	"go.uber.org/zap/zapcore"
)

type IsolationLevel int8

const (
	ReadUncommitted IsolationLevel = 0
	ReadCommitted   IsolationLevel = 1
)

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
	APIVersion int16

	ReplicaID      int32
	MaxWaitTime    int32
	MinBytes       int32
	MaxBytes       int32
	IsolationLevel IsolationLevel
	Topics         []*FetchTopic
}

func (r *FetchRequest) Encode(e PacketEncoder) (err error) {
	if r.ReplicaID == 0 {
		e.PutInt32(-1) // replica ID is -1 for clients
	} else {
		e.PutInt32(r.ReplicaID)
	}
	e.PutInt32(r.MaxWaitTime)
	e.PutInt32(r.MinBytes)
	if r.APIVersion >= 3 {
		e.PutInt32(r.MaxBytes)
	}
	if r.APIVersion >= 4 {
		e.PutInt8(int8(r.IsolationLevel))
	}
	if err = e.PutArrayLength(len(r.Topics)); err != nil {
		return err
	}
	for _, t := range r.Topics {
		if err = e.PutString(t.Topic); err != nil {
			return err
		}
		if err = e.PutArrayLength(len(t.Partitions)); err != nil {
			return err
		}
		for _, p := range t.Partitions {
			e.PutInt32(p.Partition)
			e.PutInt64(p.FetchOffset)
			e.PutInt32(p.MaxBytes)
		}
	}
	return nil
}

func (r *FetchRequest) Decode(d PacketDecoder, version int16) (err error) {
	r.APIVersion = version
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
	if r.APIVersion >= 3 {
		r.MaxBytes, err = d.Int32()
		if err != nil {
			return err
		}
	}
	if r.APIVersion >= 4 {
		isolationLevel, err := d.Int8()
		if err != nil {
			return err
		}
		r.IsolationLevel = IsolationLevel(isolationLevel)
	}
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
	return r.APIVersion
}

func (r *FetchRequest) MarshalLogObject(e zapcore.ObjectEncoder) error {
	e.AddInt32("replica id", r.ReplicaID)
	e.AddInt32("min bytes", r.MinBytes)
	e.AddInt32("max bytes", r.MaxBytes)
	e.AddArray("topic", FetchTopics(r.Topics))
	return nil
}

func (f *FetchPartition) MarshalLogObject(e zapcore.ObjectEncoder) error {
	e.AddInt32("partition", f.Partition)
	e.AddInt64("fetch offset", f.FetchOffset)
	e.AddInt32("max bytes", f.MaxBytes)
	return nil
}

type FetchPartitions []*FetchPartition

func (f FetchPartitions) MarshalLogArray(e zapcore.ArrayEncoder) error {
	for _, t := range f {
		e.AppendObject(t)
	}
	return nil
}

func (f *FetchTopic) MarshalLogObject(e zapcore.ObjectEncoder) error {
	e.AddString("topic", f.Topic)
	e.AddArray("partitions", FetchPartitions(f.Partitions))
	return nil
}

type FetchTopics []*FetchTopic

func (f FetchTopics) MarshalLogArray(e zapcore.ArrayEncoder) error {
	for _, t := range f {
		e.AppendObject(t)
	}
	return nil
}
