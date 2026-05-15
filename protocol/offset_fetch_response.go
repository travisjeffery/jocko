package protocol

type OffsetFetchTopicResponse struct {
	Topic      string
	Partitions []OffsetFetchPartition
}

type OffsetFetchPartition struct {
	Partition int32
	Offset    int64
	Metadata  string
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
			e.PutInt64(p.Offset)
			if err := e.PutString(p.Metadata); err != nil {
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
	for i := range r.Responses {
		if r.Responses[i].Topic, err = d.String(); err != nil {
			return err
		}
		partitions, err := d.ArrayLength()
		if err != nil {
			return err
		}
		r.Responses[i].Partitions = make([]OffsetFetchPartition, partitions)
		for j := range r.Responses[i].Partitions {
			if r.Responses[i].Partitions[j].Partition, err = d.Int32(); err != nil {
				return err
			}
			if r.Responses[i].Partitions[j].Offset, err = d.Int64(); err != nil {
				return err
			}
			if r.Responses[i].Partitions[j].Metadata, err = d.String(); err != nil {
				return err
			}
			if r.Responses[i].Partitions[j].ErrorCode, err = d.Int16(); err != nil {
				return err
			}
		}
	}
	return nil
}

func (r *OffsetFetchResponse) Version() int16 {
	return r.APIVersion
}
