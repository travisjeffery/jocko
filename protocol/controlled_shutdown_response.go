package protocol

import (
	"go.uber.org/zap/zapcore"
)

type ControlledShutdownResponse struct {
	APIVersion int16
}

func (r *ControlledShutdownResponse) Encode(e PacketEncoder) (err error) {
	return nil
}

func (r *ControlledShutdownResponse) Decode(d PacketDecoder, version int16) (err error) {
	r.APIVersion = version

	return nil
}

func (r *ControlledShutdownResponse) Version() int16 {
	return r.APIVersion
}

func (r *ControlledShutdownResponse) MarshalLogObject(e zapcore.ObjectEncoder) error {
	return nil
}
