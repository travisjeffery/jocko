package protocol

import (
	"go.uber.org/zap/zapcore"
)

import "time"

type Data struct {
	Partition int32
	RecordSet []byte
}

type TopicData struct {
	Topic string
	Data  []*Data
}

type ProduceRequest struct {
	APIVersion int16

	TransactionalID *string
	Acks            int16
	Timeout         time.Duration
	TopicData       []*TopicData
}

func (r *ProduceRequest) Encode(e PacketEncoder) (err error) {
	if r.APIVersion >= 3 {
		if err = e.PutNullableString(r.TransactionalID); err != nil {
			return err
		}
	}
	e.PutInt16(r.Acks)
	e.PutInt32(int32(r.Timeout / time.Millisecond))
	if err = e.PutArrayLength(len(r.TopicData)); err != nil {
		return err
	}
	for _, td := range r.TopicData {
		if err = e.PutString(td.Topic); err != nil {
			return err
		}
		if err = e.PutArrayLength(len(td.Data)); err != nil {
			return err
		}
		for _, d := range td.Data {
			e.PutInt32(d.Partition)
			if err = e.PutBytes(d.RecordSet); err != nil {
				return err
			}
		}
	}
	return nil
}

func (r *ProduceRequest) Decode(d PacketDecoder, version int16) (err error) {
	r.APIVersion = version

	if version >= 3 {
		r.TransactionalID, err = d.NullableString()
		if err != nil {
			return err
		}
	}
	r.Acks, err = d.Int16()
	if err != nil {
		return err
	}
	timeout, err := d.Int32()
	if err != nil {
		return err
	}
	r.Timeout = time.Duration(timeout) * time.Millisecond
	topicCount, err := d.ArrayLength()
	if err != nil {
		return err
	}
	r.TopicData = make([]*TopicData, topicCount)
	for i := range r.TopicData {
		td := new(TopicData)
		r.TopicData[i] = td
		td.Topic, err = d.String()
		if err != nil {
			return err
		}
		dataCount, err := d.ArrayLength()
		if err != nil {
			return err
		}
		td.Data = make([]*Data, dataCount)
		for j := range td.Data {
			data := new(Data)
			td.Data[j] = data
			data.Partition, err = d.Int32()
			if err != nil {
				return err
			}
			data.RecordSet, err = d.Bytes()
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func (r *ProduceRequest) Key() int16 {
	return ProduceKey
}

func (r *ProduceRequest) Version() int16 {
	return r.APIVersion
}

func (r *ProduceRequest) MarshalLogObject(e zapcore.ObjectEncoder) error {
	if r.TransactionalID != nil {
		e.AddString("transactional id", *r.TransactionalID)
	}
	e.AddInt16("acks", r.Acks)
	e.AddDuration("timeout", r.Timeout)
	e.AddArray("topic data", TopicDatas(r.TopicData))
	return nil
}

type TopicDatas []*TopicData

func (r TopicDatas) MarshalLogArray(e zapcore.ArrayEncoder) error {
	for _, t := range r {
		e.AppendObject(t)
	}
	return nil
}

func (r *TopicData) MarshalLogObject(e zapcore.ObjectEncoder) error {
	e.AddString("topic", r.Topic)
	e.AddArray("data", Datas(r.Data))
	return nil
}

type Datas []*Data

func (r Datas) MarshalLogArray(e zapcore.ArrayEncoder) error {
	for _, t := range r {
		e.AppendObject(t)
	}
	return nil
}

func (r *Data) MarshalLogObject(e zapcore.ObjectEncoder) error {
	e.AddInt32("partition", r.Partition)
	return nil
}
