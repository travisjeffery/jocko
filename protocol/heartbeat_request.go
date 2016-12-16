package protocol

type HeartbeatRequest struct {
	GroupID           string
	GroupGenerationID int32
	MemberID          string
}

func (r *HeartbeatRequest) encode(e PacketEncoder) error {
	if err := e.PutString(r.GroupID); err != nil {
		return err
	}
	e.PutInt32(r.GroupGenerationID)
	if err := e.PutString(r.MemberID); err != nil {
		return err
	}
	return nil
}

func (r *HeartbeatRequest) Decode(d PacketDecoder) (err error) {
	if r.GroupID, err = d.String(); err != nil {
		return
	}
	if r.GroupGenerationID, err = d.Int32(); err != nil {
		return
	}
	if r.MemberID, err = d.String(); err != nil {
		return
	}
	return nil
}

func (r *HeartbeatRequest) Key() int16 {
	return HeartbeatKey
}

func (r *HeartbeatRequest) Version() int16 {
	return 0
}
