package protocol

type OffsetCommitRequest struct{}

func (r *OffsetCommitRequest) Encode(e PacketEncoder) (err error) {
	return nil
}

func (r *OffsetCommitRequest) Decode(d PacketDecoder, version int16) (err error) {
	return nil
}
