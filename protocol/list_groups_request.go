package protocol

type ListGroupsRequest struct {
	APIVersion int16
}

func (r *ListGroupsRequest) Encode(e PacketEncoder) error {
	return nil
}

func (r *ListGroupsRequest) Decode(d PacketDecoder, version int16) (err error) {
	r.APIVersion = version
	return nil
}

func (r *ListGroupsRequest) Key() int16 {
	return ListGroupsKey
}

func (r *ListGroupsRequest) Version() int16 {
	return r.APIVersion
}
