package protocol

import (
	"go.uber.org/zap/zapcore"
)

import "time"

type SyncGroupResponse struct {
	APIVersion int16

	ThrottleTime     time.Duration
	ErrorCode        int16
	MemberAssignment []byte
}

func (r *SyncGroupResponse) Encode(e PacketEncoder) error {
	if r.APIVersion >= 1 {
		e.PutInt32(int32(r.ThrottleTime / time.Millisecond))
	}
	e.PutInt16(r.ErrorCode)
	return e.PutBytes(r.MemberAssignment)
}

func (r *SyncGroupResponse) Decode(d PacketDecoder, version int16) (err error) {
	r.APIVersion = version
	if version >= 1 {
		throttle, err := d.Int32()
		if err != nil {
			return err
		}
		r.ThrottleTime = time.Duration(throttle) * time.Millisecond
	}
	if r.ErrorCode, err = d.Int16(); err != nil {
		return err
	}
	r.MemberAssignment, err = d.Bytes()
	return err
}

func (r *SyncGroupResponse) Key() int16 {
	return 14
}

func (r *SyncGroupResponse) MarshalLogObject(e zapcore.ObjectEncoder) error {
	return nil
}
