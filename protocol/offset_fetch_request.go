package protocol

import (
	"go.uber.org/zap/zapcore"
)

// https://kafka.apache.org/protocol#The_Messages_OffsetFetch

type OffsetFetchRequest struct {
	APIVersion int16

	GroupID string
	Topics  []OffsetFetchTopicRequest
}

type OffsetFetchTopicRequest struct {
	Topic      string
	Partitions []int32
}

func (r *OffsetFetchRequest) Encode(e PacketEncoder) (err error) {
	if err = e.PutString(r.GroupID); err != nil {
		return err
	}
	if err = e.PutArrayLength(len(r.Topics)); err != nil {
		return err
	}
	for _, oft := range r.Topics {
		if err = e.PutString(oft.Topic); err != nil {
			return err
		}
		if err = e.PutInt32Array(oft.Partitions); err != nil {
			return err
		}
	}
	return nil
}

func (r *OffsetFetchRequest) Decode(d PacketDecoder, version int16) (err error) {
	r.APIVersion = version
	r.GroupID, err = d.String()
	if err != nil {
		return err
	}
	topicCount, err := d.ArrayLength()
	if err != nil {
		return err
	}
	r.Topics = make([]OffsetFetchTopicRequest, topicCount)
	for i := range r.Topics {
		oft := OffsetFetchTopicRequest{}
		if oft.Topic, err = d.String(); err != nil {
			return err
		}
		if oft.Partitions, err = d.Int32Array(); err != nil {
			return err
		}
		r.Topics[i] = oft
	}
	return nil
}

func (r *OffsetFetchRequest) Key() int16 {
	return OffsetFetchKey
}

func (r *OffsetFetchRequest) Version() int16 {
	return r.APIVersion
}

func (r *OffsetFetchRequest) MarshalLogObject(e zapcore.ObjectEncoder) error {
	return nil
}
