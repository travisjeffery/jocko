package protocol

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
	ThrottleTimeMs int32
	Responses      []*FetchResponse
}

func (r *FetchResponses) Encode(e PacketEncoder) error {
	e.PutInt32(r.ThrottleTimeMs)
	e.PutArrayLength(len(r.Responses))
	for _, r := range r.Responses {
		e.PutString(r.Topic)
		e.PutArrayLength(len(r.PartitionResponses))

		for _, p := range r.PartitionResponses {
			e.PutInt32(p.Partition)
			e.PutInt16(p.ErrorCode)
			e.PutInt64(p.HighWatermark)
			e.PutBytes(p.RecordSet)
		}
	}
	return nil
}

func (r *FetchResponses) Decode(d PacketDecoder) error {
	var err error
	r.ThrottleTimeMs, err = d.Int32()
	if err != nil {
		return err
	}
	rlen, err := d.ArrayLength()
	r.Responses = make([]*FetchResponse, rlen)

	for i := range r.Responses {
		resp := &FetchResponse{}
		resp.Topic, err = d.String()
		if err != nil {
			return err
		}
		plen, err := d.ArrayLength()
		ps := make([]*FetchPartitionResponse, plen)
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
