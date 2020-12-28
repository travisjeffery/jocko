package protocol

import "time"

type Coordinator struct {
	NodeID int32
	Host   string
	Port   int32
}

type FindCoordinatorResponse struct {
	APIVersion int16

	ThrottleTime time.Duration
	ErrorCode    Error
	ErrorMessage *string
	Coordinator  Coordinator
}

func (r *FindCoordinatorResponse) Encode(e PacketEncoder) (err error) {
	if r.APIVersion >= 1 {
		e.PutInt32(int32(r.ThrottleTime / time.Millisecond))
	}
	e.PutInt16FromError(r.ErrorCode)
	if r.APIVersion >= 1 {
		if err = e.PutNullableString(r.ErrorMessage); err != nil {
			return err
		}
	}
	e.PutInt32(r.Coordinator.NodeID)
	if err = e.PutString(r.Coordinator.Host); err != nil {
		return err
	}
	e.PutInt32(r.Coordinator.Port)
	return nil
}

func (r *FindCoordinatorResponse) Decode(d PacketDecoder, version int16) (err error) {
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
	if version >= 1 {
		if r.ErrorMessage, err = d.NullableString(); err != nil {
			return err
		}
	}
	if r.Coordinator.NodeID, err = d.Int32(); err != nil {
		return err
	}
	if r.Coordinator.Host, err = d.String(); err != nil {
		return err
	}
	if r.Coordinator.Port, err = d.Int32(); err != nil {
		return err
	}

	return nil
}

func (r *FindCoordinatorResponse) Version() int16 {
	return r.APIVersion
}
