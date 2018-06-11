package protocol

import (
	"go.uber.org/zap/zapcore"
)

type ControlledShutdownRequest struct {
	Version int16
}

func (r *ControlledShutdownRequest) Encode(e PacketEncoder) (err error) {
	return nil
}

func (r *ControlledShutdownRequest) Decode(d PacketDecoder, version int16) (err error) {
	return nil
}

func (r *ControlledShutdownRequest) MarshalLogObject(e zapcore.ObjectEncoder) error {
	return nil
}
