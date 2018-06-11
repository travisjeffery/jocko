package protocol

import (
	"go.uber.org/zap/zapcore"
)

import "time"

type AlterConfigsResponse struct {
	APIVersion int16

	ThrottleTime time.Duration
	Resources    []AlterConfigResourceResponse
}

type AlterConfigResourceResponse struct {
	ErrorCode    int16
	ErrorMessage *string
	Type         int8
	Name         string
}

func (r *AlterConfigsResponse) Encode(e PacketEncoder) error {
	e.PutInt32(int32(r.ThrottleTime / time.Millisecond))
	e.PutArrayLength(len(r.Resources))
	for _, resource := range r.Resources {
		e.PutInt16(resource.ErrorCode)
		if err := e.PutNullableString(resource.ErrorMessage); err != nil {
			return err
		}
		e.PutInt8(resource.Type)
		if err := e.PutString(resource.Name); err != nil {
			return err
		}
	}
	return nil
}

func (r *AlterConfigsResponse) Decode(d PacketDecoder, version int16) (err error) {
	r.APIVersion = version
	throttle, err := d.Int32()
	if err != nil {
		return err
	}
	r.ThrottleTime = time.Duration(throttle) * time.Millisecond
	resourceCount, err := d.ArrayLength()
	if err != nil {
		return err
	}
	r.Resources = make([]AlterConfigResourceResponse, resourceCount)
	for i := 0; i < resourceCount; i++ {
		resource := AlterConfigResourceResponse{}
		if resource.ErrorCode, err = d.Int16(); err != nil {
			return err
		}
		if resource.ErrorMessage, err = d.NullableString(); err != nil {
			return err
		}
		if resource.Type, err = d.Int8(); err != nil {
			return err
		}
		if resource.Name, err = d.String(); err != nil {
			return err
		}
		r.Resources[i] = resource
	}
	return nil
}

func (r *AlterConfigsResponse) Version() int16 {
	return r.APIVersion
}

func (r *AlterConfigsResponse) MarshalLogObject(e zapcore.ObjectEncoder) error {
	return nil
}
