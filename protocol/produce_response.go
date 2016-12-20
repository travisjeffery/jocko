package protocol

type ProducePartitionResponse struct {
	Partition  int32
	ErrorCode  int16
	BaseOffset int64
	Timestamp  int64
}

type ProduceResponse struct {
	Topic              string
	PartitionResponses []*ProducePartitionResponse
}

type ProduceResponses struct {
	Responses      []*ProduceResponse
	ThrottleTimeMs int32
}

func (r *ProduceResponses) Encode(e PacketEncoder) error {
	e.PutArrayLength(len(r.Responses))
	for _, r := range r.Responses {
		e.PutString(r.Topic)
		e.PutArrayLength(len(r.PartitionResponses))
		for _, p := range r.PartitionResponses {
			e.PutInt32(p.Partition)
			e.PutInt16(p.ErrorCode)
			e.PutInt64(p.BaseOffset)
			e.PutInt64(p.Timestamp)
		}
	}
	e.PutInt32(r.ThrottleTimeMs)
	return nil
}

func (r *ProduceResponses) Decode(d PacketDecoder) error {
	var err error
	l, err := d.ArrayLength()
	if err != nil {
		return err
	}
	r.Responses = make([]*ProduceResponse, l)
	for i := range r.Responses {
		resp := new(ProduceResponse)
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
			p.Timestamp, err = d.Int64()
			if err != nil {
				return err
			}
		}
		resp.PartitionResponses = ps
	}
	r.ThrottleTimeMs, err = d.Int32()
	if err != nil {
		return err
	}
	return nil
}

func (r *ProduceResponses) Version() int16 {
	return 2
}
