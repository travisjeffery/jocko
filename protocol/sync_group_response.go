package protocol

import "time"

type SyncGroupResponse struct {
	APIVersion int16

	ThrottleTime     time.Duration
	ErrorCode        Error
	MemberAssignment []byte
}

func (r *SyncGroupResponse) Encode(e PacketEncoder) error {
	if r.APIVersion >= 1 {
		e.PutInt32(int32(r.ThrottleTime / time.Millisecond))
	}
	e.PutInt16FromError(r.ErrorCode)
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
	if r.ErrorCode, err = d.Int16AsError(); err != nil {
		return err
	}
	r.MemberAssignment, err = d.Bytes()
	return err
}

func (r *SyncGroupResponse) Key() int16 {
	return 14
}
