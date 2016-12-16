package protocol

type ListGroupsRequest struct {
}

func (r *ListGroupsRequest) Encode(e PacketEncoder) error {
	return nil
}

func (r *ListGroupsRequest) Decode(d PacketDecoder) (err error) {
	return nil
}

func (r *ListGroupsRequest) Key() int16 {
	return ListGroupsKey
}

func (r *ListGroupsRequest) Version() int16 {
	return 0
}
