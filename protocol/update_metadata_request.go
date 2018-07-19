package protocol

type UpdateMetadataRequest struct {
	APIVersion int16
}

func (r *UpdateMetadataRequest) Encode(e PacketEncoder) (err error) {
	return nil
}

func (r *UpdateMetadataRequest) Decode(d PacketDecoder, version int16) (err error) {
	return nil
}

func (r *UpdateMetadataRequest) Key() int16 {
	return UpdateMetadataKey
}

func (r *UpdateMetadataRequest) Version() int16 {
	return r.APIVersion
}
