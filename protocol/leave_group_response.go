package protocol

import (
	"go.uber.org/zap/zapcore"
)

import "time"

type LeaveGroupResponse struct {
	APIVersion int16

	ThrottleTime time.Duration
	ErrorCode    int16
}

func (r *LeaveGroupResponse) Encode(e PacketEncoder) error {
	if r.APIVersion >= 1 {
		e.PutInt32(int32(r.ThrottleTime / time.Millisecond))
	}
	e.PutInt16(r.ErrorCode)
	return nil
}

func (r *LeaveGroupResponse) Decode(d PacketDecoder, version int16) (err error) {
	r.APIVersion = version
	if r.APIVersion >= 1 {
		throttle, err := d.Int32()
		if err != nil {
			return err
		}
		r.ThrottleTime = time.Duration(throttle) * time.Millisecond
	}
	r.ErrorCode, err = d.Int16()
	return err
}

func (r *LeaveGroupResponse) Key() int16 {
	return 13
}

func (r *LeaveGroupResponse) Version() int16 {
	return r.APIVersion
}

func (r *LeaveGroupResponse) MarshalLogObject(e zapcore.ObjectEncoder) error {
	return nil
}
