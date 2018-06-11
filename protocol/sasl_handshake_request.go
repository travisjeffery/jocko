package protocol

import (
	"go.uber.org/zap/zapcore"
)

type SaslHandshakeRequest struct{}

func (r *SaslHandshakeRequest) Encode(e PacketEncoder) (err error) {
	return nil
}

func (r *SaslHandshakeRequest) Decode(d PacketDecoder, version int16) (err error) {
	return nil
}

func (r *SaslHandshakeRequest) MarshalLogObject(e zapcore.ObjectEncoder) error {
	return nil
}
