package protocol

type LeaveGroupRequest struct {
	APIVersion int16

	GroupID  string
	MemberID string
}

func (r *LeaveGroupRequest) Encode(e PacketEncoder) error {
	if err := e.PutString(r.GroupID); err != nil {
		return err
	}
	return e.PutString(r.MemberID)
}

func (r *LeaveGroupRequest) Decode(d PacketDecoder, version int16) (err error) {
	r.APIVersion = version
	if r.GroupID, err = d.String(); err != nil {
		return err
	}
	r.MemberID, err = d.String()
	return err
}

func (r *LeaveGroupRequest) Key() int16 {
	return LeaveGroupKey
}

func (r *LeaveGroupRequest) Version() int16 {
	return r.APIVersion
}
