package protocol

type CoordinatorType int8

const (
	CoordinatorGroup       CoordinatorType = 0
	CoordinatorTransaction CoordinatorType = 0
)

type FindCoordinatorRequest struct {
	CoordinatorKey  string
	CoordinatorType CoordinatorType
}

func (r *FindCoordinatorRequest) Encode(e PacketEncoder) (err error) {
	if err = e.PutString(r.CoordinatorKey); err != nil {
		return err
	}
	if r.Version() >= 1 {
		e.PutInt8(int8(r.CoordinatorType))
	}
	return nil
}

func (r *FindCoordinatorRequest) Decode(d PacketDecoder) (err error) {
	if r.CoordinatorKey, err = d.String(); err != nil {
		return err
	}
	coordinatorType, err := d.Int8()
	if err != nil {
		return err
	}
	r.CoordinatorType = CoordinatorType(coordinatorType)
	return nil
}

func (r *FindCoordinatorRequest) Version() int16 {
	return 1
}

func (r *FindCoordinatorRequest) Key() int16 {
	return 10
}
