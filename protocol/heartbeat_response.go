package protocol

import "time"

type HeartbeatResponse struct {
	APIVersion int16

	ThrottleTime time.Duration
	ErrorCode    Error
}

func (r *HeartbeatResponse) Encode(e PacketEncoder) error {
	e.PutInt16FromError(r.ErrorCode)
	return nil
}

func (r *HeartbeatResponse) Decode(d PacketDecoder, version int16) (err error) {
	r.APIVersion = version

	if version >= 1 {
		throttle, err := d.Int32()
		if err != nil {
			return err
		}
		r.ThrottleTime = time.Duration(throttle) / time.Millisecond
	}
	r.ErrorCode, err = d.Int16AsError()
	return err
}

func (r *HeartbeatResponse) Key() int16 {
	return HeartbeatKey
}

func (r *HeartbeatResponse) Version() int16 {
	return r.APIVersion
}
