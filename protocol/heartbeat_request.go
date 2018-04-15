package protocol

type HeartbeatRequest struct {
	APIVersion int16

	GroupID           string
	GroupGenerationID int32
	MemberID          string
}

func (r *HeartbeatRequest) Encode(e PacketEncoder) (err error) {
	if err = e.PutString(r.GroupID); err != nil {
		return err
	}
	e.PutInt32(r.GroupGenerationID)
	return e.PutString(r.MemberID)
}

func (r *HeartbeatRequest) Decode(d PacketDecoder, version int16) (err error) {
	r.APIVersion = version

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
	return r.APIVersion
}
