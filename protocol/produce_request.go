package protocol

import "time"

type Data struct {
	Partition int32
	RecordSet []byte
}

type TopicData struct {
	Topic string
	Data  []*Data
}

type ProduceRequest struct {
	APIVersion int16

	TransactionalID *string
	Acks            int16
	Timeout         time.Duration
	TopicData       []*TopicData
}

func (r *ProduceRequest) Encode(e PacketEncoder) (err error) {
	if r.APIVersion >= 3 {
		if err = e.PutNullableString(r.TransactionalID); err != nil {
			return err
		}
	}
	e.PutInt16(r.Acks)
	e.PutInt32(int32(r.Timeout / time.Millisecond))
	if err = e.PutArrayLength(len(r.TopicData)); err != nil {
		return err
	}
	for _, td := range r.TopicData {
		if err = e.PutString(td.Topic); err != nil {
			return err
		}
		if err = e.PutArrayLength(len(td.Data)); err != nil {
			return err
		}
		for _, d := range td.Data {
			e.PutInt32(d.Partition)
			if err = e.PutBytes(d.RecordSet); err != nil {
				return err
			}
		}
	}
	return nil
}

func (r *ProduceRequest) Decode(d PacketDecoder, version int16) (err error) {
	r.APIVersion = version

	if version >= 3 {
		r.TransactionalID, err = d.NullableString()
		if err != nil {
			return err
		}
	}
	r.Acks, err = d.Int16()
	if err != nil {
		return err
	}
	timeout, err := d.Int32()
	if err != nil {
		return err
	}
	r.Timeout = time.Duration(timeout) * time.Millisecond
	topicCount, err := d.ArrayLength()
	if err != nil {
		return err
	}
	r.TopicData = make([]*TopicData, topicCount)
	for i := range r.TopicData {
		td := new(TopicData)
		r.TopicData[i] = td
		td.Topic, err = d.String()
		if err != nil {
			return err
		}
		dataCount, err := d.ArrayLength()
		if err != nil {
			return err
		}
		td.Data = make([]*Data, dataCount)
		for j := range td.Data {
			data := new(Data)
			td.Data[j] = data
			data.Partition, err = d.Int32()
			if err != nil {
				return err
			}
			data.RecordSet, err = d.Bytes()
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func (r *ProduceRequest) Key() int16 {
	return ProduceKey
}

func (r *ProduceRequest) Version() int16 {
	return r.APIVersion
}

type TopicDatas []*TopicData

type Datas []*Data
