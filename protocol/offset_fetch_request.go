package protocol

type OffsetFetchRequest struct{}

func (r *OffsetFetchRequest) Encode(e PacketEncoder) (err error) {
	return nil
}

func (r *OffsetFetchRequest) Decode(d PacketDecoder, version int16) (err error) {
	return nil
}
