package protocol

import (
	"go.uber.org/zap/zapcore"
)

import "time"

type ProducePartitionResponse struct {
	Partition      int32
	ErrorCode      int16
	BaseOffset     int64
	LogAppendTime  time.Time
	LogStartOffset int64
}

type ProduceTopicResponse struct {
	Topic              string
	PartitionResponses []*ProducePartitionResponse
}

type ProduceResponse struct {
	APIVersion int16

	Responses    []*ProduceTopicResponse
	ThrottleTime time.Duration
}

func (r *ProduceResponse) Encode(e PacketEncoder) (err error) {
	if err = e.PutArrayLength(len(r.Responses)); err != nil {
		return err
	}
	for _, resp := range r.Responses {
		if err = e.PutString(resp.Topic); err != nil {
			return err
		}
		if err = e.PutArrayLength(len(resp.PartitionResponses)); err != nil {
			return err
		}
		for _, p := range resp.PartitionResponses {
			e.PutInt32(p.Partition)
			e.PutInt16(p.ErrorCode)
			if r.APIVersion >= 2 {
				e.PutInt64(p.BaseOffset)
				e.PutInt64(int64(p.LogAppendTime.UnixNano() / int64(time.Millisecond)))
			}
			if r.APIVersion >= 5 {
				e.PutInt64(p.LogStartOffset)
			}
		}
	}
	if r.APIVersion >= 1 {
		e.PutInt32(int32(r.ThrottleTime / time.Millisecond))
	}
	return nil
}

func (r *ProduceResponse) Decode(d PacketDecoder, version int16) (err error) {
	r.APIVersion = version
	l, err := d.ArrayLength()
	if err != nil {
		return err
	}
	r.Responses = make([]*ProduceTopicResponse, l)
	for i := range r.Responses {
		resp := new(ProduceTopicResponse)
		r.Responses[i] = resp
		resp.Topic, err = d.String()
		if err != nil {
			return err
		}
		pl, err := d.ArrayLength()
		if err != nil {
			return err
		}

		ps := make([]*ProducePartitionResponse, pl)
		for j := range ps {
			p := new(ProducePartitionResponse)
			ps[j] = p
			p.Partition, err = d.Int32()
			if err != nil {
				return err
			}
			p.ErrorCode, err = d.Int16()
			if err != nil {
				return err
			}
			p.BaseOffset, err = d.Int64()
			if err != nil {
				return err
			}
			if r.APIVersion >= 2 {
				millis, err := d.Int64()
				if err != nil {
					return err
				}
				p.LogAppendTime = time.Unix(millis/1000, (millis%1000)*int64(time.Millisecond))
			}
			if r.APIVersion >= 5 {
				p.LogStartOffset, err = d.Int64()
				if err != nil {
					return err
				}
			}
		}
		resp.PartitionResponses = ps
	}
	if r.APIVersion >= 1 {
		throttle, err := d.Int32()
		if err != nil {
			return err
		}
		r.ThrottleTime = time.Duration(throttle) * time.Millisecond
	}
	return nil

}

func (r *ProduceResponse) MarshalLogObject(e zapcore.ObjectEncoder) error {
	e.AddArray("topics", ProduceTopicResponses(r.Responses))
	return nil
}

type ProduceTopicResponses []*ProduceTopicResponse

func (r ProduceTopicResponses) MarshalLogArray(e zapcore.ArrayEncoder) error {
	for _, t := range r {
		e.AppendObject(t)
	}
	return nil
}

func (r *ProduceTopicResponse) MarshalLogObject(e zapcore.ObjectEncoder) error {
	e.AddString("topic", r.Topic)
	e.AddArray("partitions", ProducePartitionResponses(r.PartitionResponses))
	return nil
}

type ProducePartitionResponses []*ProducePartitionResponse

func (r ProducePartitionResponses) MarshalLogArray(e zapcore.ArrayEncoder) error {
	for _, t := range r {
		e.AppendObject(t)
	}
	return nil
}

func (r *ProducePartitionResponse) MarshalLogObject(e zapcore.ObjectEncoder) error {
	e.AddInt32("partition", r.Partition)
	e.AddInt16("error code", r.ErrorCode)
	e.AddInt64("base offset", r.BaseOffset)
	return nil
}
