package protocol

type OffsetFetchResponse struct{}

func (r *OffsetFetchResponse) Encode(e PacketEncoder) (err error) {
	return nil
}

func (r *OffsetFetchResponse) Decode(d PacketDecoder, version int16) (err error) {
	return nil
}
