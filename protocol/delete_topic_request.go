package protocol

import "time"

type DeleteTopicsRequest struct {
	APIVersion int16

	Topics  []string
	Timeout time.Duration
}

func (r *DeleteTopicsRequest) Encode(e PacketEncoder) (err error) {
	if err = e.PutStringArray(r.Topics); err != nil {
		return err
	}
	e.PutInt32(int32(r.Timeout / time.Millisecond))
	return nil
}

func (r *DeleteTopicsRequest) Decode(d PacketDecoder, version int16) (err error) {
	r.APIVersion = version
	r.Topics, err = d.StringArray()
	if err != nil {
		return err
	}
	timeout, err := d.Int32()
	if err != nil {
		return err
	}
	r.Timeout = time.Duration(timeout) * time.Millisecond
	return err
}

func (r *DeleteTopicsRequest) Key() int16 {
	return DeleteTopicsKey
}

func (r *DeleteTopicsRequest) Version() int16 {
	return r.APIVersion
}
