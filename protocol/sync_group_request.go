package protocol

type SyncGroupRequest struct {
	GroupID          string
	GenerationID     int32
	MemberID         string
	GroupAssignments map[string][]byte
}

func (r *SyncGroupRequest) Encode(e PacketEncoder) error {
	if err := e.PutString(r.GroupID); err != nil {
		return err
	}
	e.PutInt32(r.GenerationID)
	if err := e.PutString(r.MemberID); err != nil {
		return err
	}
	if err := e.PutArrayLength(len(r.GroupAssignments)); err != nil {
		return err
	}
	for memberID, memberAssignment := range r.GroupAssignments {
		if err := e.PutString(memberID); err != nil {
			return err
		}
		if err := e.PutBytes(memberAssignment); err != nil {
			return err
		}
	}
	return nil
}

func (r *SyncGroupRequest) Decode(d PacketDecoder) (err error) {
	if r.GroupID, err = d.String(); err != nil {
		return
	}
	if r.GenerationID, err = d.Int32(); err != nil {
		return
	}
	if r.MemberID, err = d.String(); err != nil {
		return
	}
	groupAssignmentCount, err := d.ArrayLength()
	if err != nil {
		return err
	}
	r.GroupAssignments = make(map[string][]byte)
	for i := 0; i < groupAssignmentCount; i++ {
		memberID, err := d.String()
		if err != nil {
			return err
		}
		memberAssignment, err := d.Bytes()
		if err != nil {
			return err
		}
		r.GroupAssignments[memberID] = memberAssignment
	}
	return nil
}

func (r *SyncGroupRequest) Key() int16 {
	return SyncGroupKey
}

func (r *SyncGroupRequest) Version() int16 {
	return 0
}
