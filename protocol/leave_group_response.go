package protocol

type LeaveGroupResponse struct {
	ErrorCode int16
}

func (r *LeaveGroupResponse) Encode(e PacketEncoder) error {
	e.PutInt16(r.ErrorCode)
	return nil
}

func (r *LeaveGroupResponse) Decode(d PacketDecoder) (err error) {
	r.ErrorCode, err = d.Int16()
	return err
}

func (r *LeaveGroupResponse) Key() int16 {
	return 13
}

func (r *LeaveGroupResponse) Version() int16 {
	return 0
}
