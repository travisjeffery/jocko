package protocol

import "time"

type AbortedTransaction struct {
	ProducerID  int64
	FirstOffset int64
}

func (t *AbortedTransaction) Decode(d PacketDecoder, version int16) (err error) {
	if t.ProducerID, err = d.Int64(); err != nil {
		return err
	}
	if t.FirstOffset, err = d.Int64(); err != nil {
		return err
	}
	return nil
}

func (t *AbortedTransaction) Encode(e PacketEncoder) (err error) {
	e.PutInt64(t.ProducerID)
	e.PutInt64(t.FirstOffset)
	return nil
}

type FetchPartitionResponse struct {
	Partition           int32
	ErrorCode           int16
	HighWatermark       int64
	LastStableOffset    int64
	AbortedTransactions []*AbortedTransaction
	RecordSet           []byte
}

func (r *FetchPartitionResponse) Decode(d PacketDecoder, version int16) (err error) {
	if r.Partition, err = d.Int32(); err != nil {
		return err
	}
	if r.ErrorCode, err = d.Int16(); err != nil {
		return err
	}
	if r.HighWatermark, err = d.Int64(); err != nil {
		return err
	}

	if version >= 4 {
		if r.LastStableOffset, err = d.Int64(); err != nil {
			return err
		}

		transactionCount, err := d.ArrayLength()
		if err != nil {
			return err
		}

		r.AbortedTransactions = make([]*AbortedTransaction, transactionCount)
		for i := 0; i < transactionCount; i++ {
			t := &AbortedTransaction{}
			if err = t.Decode(d, version); err != nil {
				return err
			}
			r.AbortedTransactions[i] = t
		}
	}

	if r.RecordSet, err = d.Bytes(); err != nil {
		return err
	}

	return nil
}

func (r *FetchPartitionResponse) Encode(e PacketEncoder, version int16) (err error) {
	e.PutInt32(r.Partition)
	e.PutInt16(r.ErrorCode)
	e.PutInt64(r.HighWatermark)

	if version >= 4 {
		e.PutInt64(r.LastStableOffset)

		if err = e.PutArrayLength(len(r.AbortedTransactions)); err != nil {
			return err
		}
		for _, t := range r.AbortedTransactions {
			t.Encode(e)
		}
	}

	if err = e.PutBytes(r.RecordSet); err != nil {
		return err
	}

	return nil
}

type FetchTopicResponse struct {
	Topic              string
	PartitionResponses FetchPartitionResponses
}

type FetchPartitionResponses []*FetchPartitionResponse

type FetchResponse struct {
	APIVersion int16

	ThrottleTime time.Duration
	Responses    FetchTopicResponses
}

type FetchTopicResponses []*FetchTopicResponse

func (r *FetchResponse) Encode(e PacketEncoder) (err error) {
	if r.APIVersion >= 1 {
		e.PutInt32(int32(r.ThrottleTime / time.Millisecond))
	}

	if err = e.PutArrayLength(len(r.Responses)); err != nil {
		return err
	}
	for _, response := range r.Responses {
		if err = e.PutString(response.Topic); err != nil {
			return err
		}
		if err = e.PutArrayLength(len(response.PartitionResponses)); err != nil {
			return err
		}
		for _, p := range response.PartitionResponses {
			if err = p.Encode(e, r.APIVersion); err != nil {
				return err
			}
		}
	}
	return nil
}

func (r *FetchResponse) Decode(d PacketDecoder, version int16) (err error) {
	r.APIVersion = version

	if r.APIVersion >= 1 {
		throttle, err := d.Int32()
		if err != nil {
			return err
		}
		r.ThrottleTime = time.Duration(throttle) * time.Millisecond
	}

	responseCount, err := d.ArrayLength()
	if err != nil {
		return err
	}
	r.Responses = make([]*FetchTopicResponse, responseCount)

	for i := range r.Responses {
		resp := &FetchTopicResponse{}
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
			if err = p.Decode(d, version); err != nil {
				return err
			}
			ps[j] = p
		}
		resp.PartitionResponses = ps
		r.Responses[i] = resp
	}
	return nil
}

func (r *FetchResponse) Version() int16 {
	return r.APIVersion
}
