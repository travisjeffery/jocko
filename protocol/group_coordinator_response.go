package protocol

type Coordinator struct {
	NodeID int32
	Host   string
	Port   int32
}

type GroupCoordinatorResponse struct {
	ErrorCode   int16
	Coordinator *Coordinator
}

func (r *GroupCoordinatorResponse) Encode(e PacketEncoder) error {
	e.PutInt16(r.ErrorCode)
	e.PutInt32(r.Coordinator.NodeID)
	if err := e.PutString(r.Coordinator.Host); err != nil {
		return err
	}
	e.PutInt32(r.Coordinator.Port)
	return nil
}

func (r *GroupCoordinatorResponse) Decode(d PacketDecoder) (err error) {
	if r.ErrorCode, err = d.Int16(); err != nil {
		return err
	}
	r.Coordinator = new(Coordinator)
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
