package protocol

import (
	"go.uber.org/zap/zapcore"
)

type UpdateMetadataResponse struct {
}

func (r *UpdateMetadataResponse) Encode(e PacketEncoder) (err error) {
	return nil
}

func (r *UpdateMetadataResponse) Decode(d PacketDecoder, version int16) (err error) {
	return nil
}

func (r *UpdateMetadataResponse) MarshalLogObject(e zapcore.ObjectEncoder) error {
	return nil
}
