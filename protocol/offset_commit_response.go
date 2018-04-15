package protocol

type OffsetCommitResponse struct{}

func (r *OffsetCommitResponse) Encode(e PacketEncoder) (err error) {
	return nil
}

func (r *OffsetCommitResponse) Decode(d PacketDecoder, version int16) (err error) {
	return nil
}
