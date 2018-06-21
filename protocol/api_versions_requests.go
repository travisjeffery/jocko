package protocol

import (
	"go.uber.org/zap/zapcore"
)

type APIVersionsRequest struct {
	APIVersion int16
}

func (c *APIVersionsRequest) Encode(_ PacketEncoder) error {
	return nil
}

func (c *APIVersionsRequest) Decode(_ PacketDecoder, version int16) error {
	c.APIVersion = version
	return nil
}

func (c *APIVersionsRequest) Key() int16 {
	return APIVersionsKey
}

func (r *APIVersionsRequest) Version() int16 {
	return r.APIVersion
}

func (r *APIVersionsRequest) MarshalLogObject(e zapcore.ObjectEncoder) error {
	return nil
}
