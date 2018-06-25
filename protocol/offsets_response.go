package protocol

import "time"

type PartitionResponse struct {
	Partition int32
	ErrorCode int16
	Timestamp time.Time
	Offsets   []int64
	Offset    int64
}

type OffsetResponse struct {
	Topic              string
	PartitionResponses []*PartitionResponse
}

type OffsetsResponse struct {
	APIVersion int16

	ThrottleTime time.Duration
	Responses    []*OffsetResponse
}

func (r *OffsetsResponse) Encode(e PacketEncoder) (err error) {
	if r.APIVersion >= 2 {
		e.PutInt32(int32(r.ThrottleTime / time.Millisecond))
	}
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
			if r.APIVersion == 0 {
				if err = e.PutInt64Array(p.Offsets); err != nil {
					return err
				}
			}
			if r.APIVersion >= 1 {
				e.PutInt64(p.Timestamp.UnixNano() / int64(time.Millisecond))
				e.PutInt64(p.Offset)
			}
		}
	}
	return nil
}

func (r *OffsetsResponse) Decode(d PacketDecoder, version int16) (err error) {
	if version >= 2 {
		throttle, err := d.Int32()
		if err != nil {
			return err
		}
		r.ThrottleTime = time.Duration(throttle) * time.Millisecond
	}
	l, err := d.ArrayLength()
	if err != nil {
		return err
	}
	r.Responses = make([]*OffsetResponse, l)
	for i := range r.Responses {
		resp := new(OffsetResponse)
		r.Responses[i] = resp
		resp.Topic, err = d.String()
		if err != nil {
			return err
		}
		pl, err := d.ArrayLength()
		if err != nil {
			return err
		}
		ps := make([]*PartitionResponse, pl)
		for j := range ps {
			p := new(PartitionResponse)
			p.Partition, err = d.Int32()
			if err != nil {
				return err
			}
			p.ErrorCode, err = d.Int16()
			if err != nil {
				return err
			}

			if version == 0 {
				p.Offsets, err = d.Int64Array()
				if err != nil {
					return err
				}
			}
			if version >= 1 {
				t, err := d.Int64()
				if err != nil {
					return err
				}
				p.Timestamp = time.Unix(t/1000, (t%1000)*int64(time.Millisecond))
				p.Offset, err = d.Int64()
				if err != nil {
					return err
				}
			}

			ps[j] = p
		}
		resp.PartitionResponses = ps
	}
	return err
}

func (r *OffsetsResponse) Version() int16 {
	return r.APIVersion
}
