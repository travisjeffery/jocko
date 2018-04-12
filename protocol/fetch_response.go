package protocol

import "time"

type FetchPartitionResponse struct {
	Partition     int32
	ErrorCode     int16
	HighWatermark int64
	RecordSet     []byte
}

type FetchResponse struct {
	Topic              string
	PartitionResponses []*FetchPartitionResponse
}

type FetchResponses struct {
	APIVersion int16

	ThrottleTime time.Duration
	Responses    []*FetchResponse
}

func (r *FetchResponses) Encode(e PacketEncoder) (err error) {
	if r.APIVersion >= 1 {
		e.PutInt32(int32(r.ThrottleTime / time.Millisecond))
	}

	if err = e.PutArrayLength(len(r.Responses)); err != nil {
		return err
	}
	for _, r := range r.Responses {
		if err = e.PutString(r.Topic); err != nil {
			return err
		}
		if err = e.PutArrayLength(len(r.PartitionResponses)); err != nil {
			return err
		}
		for _, p := range r.PartitionResponses {
			e.PutInt32(p.Partition)
			e.PutInt16(p.ErrorCode)
			e.PutInt64(p.HighWatermark)
			if err = e.PutBytes(p.RecordSet); err != nil {
				return err
			}
		}
	}
	return nil
}

func (r *FetchResponses) Decode(d PacketDecoder, version int16) (err error) {
	r.APIVersion = version

	if r.APIVersion >= 1 {
		throttleTimeMs, err := d.Int32()
		if err != nil {
			return err
		}
		r.ThrottleTime = time.Duration(throttleTimeMs) * time.Millisecond
	}

	responseCount, err := d.ArrayLength()
	if err != nil {
		return err
	}
	r.Responses = make([]*FetchResponse, responseCount)

	for i := range r.Responses {
		resp := &FetchResponse{}
		resp.Topic, err = d.String()
		if err != nil {
			return err
		}
		partitionCount, err := d.ArrayLength()
		if err != nil {
			return err
		}
		ps := make([]*FetchPartitionResponse, partitionCount)
		for j := range ps {
			p := &FetchPartitionResponse{}
			p.Partition, err = d.Int32()
			if err != nil {
				return err
			}
			p.ErrorCode, err = d.Int16()
			if err != nil {
				return err
			}
			p.HighWatermark, err = d.Int64()
			if err != nil {
				return err
			}
			p.RecordSet, err = d.Bytes()
			if err != nil {
				return err
			}
			ps[j] = p
		}
		resp.PartitionResponses = ps
		r.Responses[i] = resp
	}
	return nil
}

func (r *FetchResponses) Version() int16 {
	return r.APIVersion
}
