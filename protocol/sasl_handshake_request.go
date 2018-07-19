package protocol

type SaslHandshakeRequest struct {
	APIVersion int16
}

func (r *SaslHandshakeRequest) Encode(e PacketEncoder) (err error) {
	return nil
}

func (r *SaslHandshakeRequest) Decode(d PacketDecoder, version int16) (err error) {
	return nil
}

func (r *SaslHandshakeRequest) Key() int16 {
	return SaslHandshakeKey
}

func (r *SaslHandshakeRequest) Version() int16 {
	return r.APIVersion
}
