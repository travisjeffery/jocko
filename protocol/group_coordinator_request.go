package protocol

type GroupCoordinatorRequest struct {
	GroupID string
}

func (r *GroupCoordinatorRequest) Encode(e PacketEncoder) error {
	return e.PutString(r.GroupID)
}

func (r *GroupCoordinatorRequest) Decode(d PacketDecoder) (err error) {
	r.GroupID, err = d.String()
	return err
}

func (r *GroupCoordinatorRequest) Version() int16 {
	return 0
}

func (r *GroupCoordinatorRequest) Key() int16 {
	return 10
}
