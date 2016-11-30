package protocol

type Member struct {
	MemberID       string
	MemberMetadata []byte
}

type JoinGroupResponse struct {
	ErrorCode     int16
	GenerationID  int32
	GroupProtocol string
	LeaderID      string
	MemberID      string
	Members       map[string][]byte
}

func (r *JoinGroupResponse) Encode(e PacketEncoder) error {
	var err error
	e.PutInt16(r.ErrorCode)
	e.PutInt32(r.GenerationID)
	if err = e.PutString(r.GroupProtocol); err != nil {
		return err
	}
	if err = e.PutString(r.LeaderID); err != nil {
		return err
	}
	if err = e.PutString(r.MemberID); err != nil {
		return err
	}
	for memberID, metadata := range r.Members {
		if err = e.PutString(memberID); err != nil {
			return err
		}
		if err = e.PutBytes(metadata); err != nil {
			return err
		}
	}
	return nil
}

func (r *JoinGroupResponse) Decode(d PacketDecoder) error {
	var err error
	if r.ErrorCode, err = d.Int16(); err != nil {
		return err
	}
	if r.GenerationID, err = d.Int32(); err != nil {
		return err
	}
	if r.GroupProtocol, err = d.String(); err != nil {
		return err
	}

	if r.LeaderID, err = d.String(); err != nil {
		return err
	}
	if r.MemberID, err = d.String(); err != nil {
		return err
	}
	memberCount, err := d.ArrayLength()
	if err != nil {
		return err
	}
	r.Members = make(map[string][]byte)
	for i := 0; i < memberCount; i++ {
		k, err := d.String()
		if err != nil {
			return err
		}
		v, err := d.Bytes()
		if err != nil {
			return err
		}
		r.Members[k] = v
	}
	return nil
}

func (r *JoinGroupResponse) Key() int16 {
	return 11
}

func (r *JoinGroupResponse) Version() int16 {
	return 0
}
