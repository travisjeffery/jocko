package protocol

type Coordinator struct {
	NodeID int32
	Host   string
	Port   int32
}

type FindCoordinatorResponse struct {
	ThrottleTimeMs int32
	ErrorCode      int16
	ErrorMessage   *string
	Coordinator    Coordinator
}

func (r *FindCoordinatorResponse) Encode(e PacketEncoder) (err error) {
	e.PutInt32(r.ThrottleTimeMs)
	e.PutInt16(r.ErrorCode)
	if err = e.PutNullableString(r.ErrorMessage); err != nil {
		return err
	}
	e.PutInt32(r.Coordinator.NodeID)
	if err = e.PutString(r.Coordinator.Host); err != nil {
		return err
	}
	e.PutInt32(r.Coordinator.Port)
	return nil
}

func (r *FindCoordinatorResponse) Decode(d PacketDecoder) (err error) {
	if r.ThrottleTimeMs, err = d.Int32(); err != nil {
		return err
	}
	if r.ErrorCode, err = d.Int16(); err != nil {
		return err
	}
	if r.ErrorMessage, err = d.NullableString(); err != nil {
		return err
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
