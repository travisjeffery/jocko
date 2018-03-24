package protocol

type FindCoordinatorRequest struct {
	CoordinatorKey  string
	CoordinatorType int8
}

func (r *FindCoordinatorRequest) Encode(e PacketEncoder) (err error) {
	if err = e.PutString(r.CoordinatorKey); err != nil {
		return err
	}
	if r.Version() >= 1 {
		e.PutInt8(r.CoordinatorType)
	}
	return nil
}

func (r *FindCoordinatorRequest) Decode(d PacketDecoder) (err error) {
	if r.CoordinatorKey, err = d.String(); err != nil {
		return err
	}
	if r.CoordinatorType, err = d.Int8(); err != nil {
		return err
	}
	return nil
}

func (r *FindCoordinatorRequest) Version() int16 {
	return 1
}

func (r *FindCoordinatorRequest) Key() int16 {
	return 10
}
