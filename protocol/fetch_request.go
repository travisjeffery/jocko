package protocol

import "time"

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
	MaxWaitTime    time.Duration
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
	e.PutInt32(int32(r.MaxWaitTime / time.Millisecond))
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
	maxWaitTime, err := d.Int32()
	if err != nil {
		return err
	}
	r.MaxWaitTime = time.Duration(maxWaitTime) * time.Millisecond
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

type FetchPartitions []*FetchPartition

type FetchTopics []*FetchTopic
