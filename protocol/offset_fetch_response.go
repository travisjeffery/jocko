package protocol

import (
	"go.uber.org/zap/zapcore"
)

type OffsetFetchTopicResponse struct {
	Topic      string
	Partitions []OffsetFetchPartition
}

type OffsetFetchPartition struct {
	Partition int32
	Offset    int16
	Metadata  *string
	ErrorCode int16
}

type OffsetFetchResponse struct {
	APIVersion int16

	Responses []OffsetFetchTopicResponse
}

func (r *OffsetFetchResponse) Encode(e PacketEncoder) (err error) {
	if err := e.PutArrayLength(len(r.Responses)); err != nil {
		return err
	}
	for _, resp := range r.Responses {
		if err := e.PutString(resp.Topic); err != nil {
			return err
		}
		if err := e.PutArrayLength(len(resp.Partitions)); err != nil {
			return err
		}
		for _, p := range resp.Partitions {
			e.PutInt32(p.Partition)
			e.PutInt16(p.Offset)
			if err := e.PutNullableString(p.Metadata); err != nil {
				return err
			}
			e.PutInt16(p.ErrorCode)
		}
	}
	return nil
}

func (r *OffsetFetchResponse) Decode(d PacketDecoder, version int16) (err error) {
	r.APIVersion = version

	responses, err := d.ArrayLength()
	if err != nil {
		return err
	}
	r.Responses = make([]OffsetFetchTopicResponse, responses)
	for _, resp := range r.Responses {
		if resp.Topic, err = d.String(); err != nil {
			return err
		}
		partitions, err := d.ArrayLength()
		if err != nil {
			return err
		}
		resp.Partitions = make([]OffsetFetchPartition, partitions)
		for _, p := range resp.Partitions {
			if p.Partition, err = d.Int32(); err != nil {
				return err
			}
			if p.Offset, err = d.Int16(); err != nil {
				return err
			}
			if p.Metadata, err = d.NullableString(); err != nil {
				return err
			}
			if p.ErrorCode, err = d.Int16(); err != nil {
				return err
			}
		}
	}
	return nil
}

func (r *OffsetFetchResponse) Version() int16 {
	return r.APIVersion
}

func (r *OffsetFetchResponse) MarshalLogObject(e zapcore.ObjectEncoder) error {
	return nil
}
