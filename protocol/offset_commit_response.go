package protocol

import "time"

type OffsetCommitResponse struct {
	APIVersion int16

	ThrottleTime time.Duration
	Responses    []OffsetCommitTopicResponse
}

type OffsetCommitTopicResponse struct {
	Topic              string
	PartitionResponses []OffsetCommitPartitionResponse
}

type OffsetCommitPartitionResponse struct {
	Partition int32
	ErrorCode int16
}

func (r *OffsetCommitResponse) Encode(e PacketEncoder) (err error) {
	if r.APIVersion >= 3 {
		e.PutInt32(int32(r.ThrottleTime / time.Millisecond))
	}
	if err := e.PutArrayLength(len(r.Responses)); err != nil {
		return err
	}
	for _, t := range r.Responses {
		if err := e.PutString(t.Topic); err != nil {
			return err
		}
		if err := e.PutArrayLength(len(t.PartitionResponses)); err != nil {
			return err
		}
		for _, p := range t.PartitionResponses {
			e.PutInt32(p.Partition)
			e.PutInt16(p.ErrorCode)
		}
	}
	return nil
}

func (r *OffsetCommitResponse) Decode(d PacketDecoder, version int16) (err error) {
	r.APIVersion = version

	if version >= 3 {
		throttle, err := d.Int32()
		if err != nil {
			return err
		}
		r.ThrottleTime = time.Duration(throttle) * time.Millisecond
	}

	topicCount, err := d.ArrayLength()
	if err != nil {
		return err
	}
	r.Responses = make([]OffsetCommitTopicResponse, topicCount)
	for _, t := range r.Responses {
		if t.Topic, err = d.String(); err != nil {
			return err
		}
		partitionCount, err := d.ArrayLength()
		if err != nil {
			return err
		}
		t.PartitionResponses = make([]OffsetCommitPartitionResponse, partitionCount)
		for _, p := range t.PartitionResponses {
			p.Partition, err = d.Int32()
			if err != nil {
				return err
			}
			p.ErrorCode, err = d.Int16()
			if err != nil {
				return err
			}
		}

	}
	return nil
}
