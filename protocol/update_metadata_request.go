package protocol

import (
	"go.uber.org/zap/zapcore"
)

type UpdateMetadataRequest struct {
}

func (r *UpdateMetadataRequest) Encode(e PacketEncoder) (err error) {
	return nil
}

func (r *UpdateMetadataRequest) Decode(d PacketDecoder, version int16) (err error) {
	return nil
}

func (r *UpdateMetadataRequest) MarshalLogObject(e zapcore.ObjectEncoder) error {
	return nil
}
