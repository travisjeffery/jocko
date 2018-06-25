package protocol

type SaslHandshakeResponse struct{}

func (r *SaslHandshakeResponse) Encode(e PacketEncoder) (err error) {
	return nil
}

func (r *SaslHandshakeResponse) Decode(d PacketDecoder, version int16) (err error) {
	return nil
}
