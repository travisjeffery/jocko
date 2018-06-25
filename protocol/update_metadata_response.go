package protocol

type UpdateMetadataResponse struct {
}

func (r *UpdateMetadataResponse) Encode(e PacketEncoder) (err error) {
	return nil
}

func (r *UpdateMetadataResponse) Decode(d PacketDecoder, version int16) (err error) {
	return nil
}
