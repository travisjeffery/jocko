package protocol

import "time"

type Member struct {
	MemberID       string
	MemberMetadata []byte
}

type JoinGroupResponse struct {
	APIVersion int16

	ThrottleTime  time.Duration
	ErrorCode     int16
	GenerationID  int32
	GroupProtocol string
	LeaderID      string
	MemberID      string
	Members       []Member
}

func (r *JoinGroupResponse) Encode(e PacketEncoder) (err error) {
	if r.APIVersion >= 1 {
		e.PutInt32(int32(r.ThrottleTime / time.Millisecond))
	}
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
	if err = e.PutArrayLength(len(r.Members)); err != nil {
		return err
	}
	for _, member := range r.Members {
		if err = e.PutString(member.MemberID); err != nil {
			return err
		}
		if err = e.PutBytes(member.MemberMetadata); err != nil {
			return err
		}
	}
	return nil
}

func (r *JoinGroupResponse) Decode(d PacketDecoder, version int16) (err error) {
	r.APIVersion = version

	if version >= 2 {
		timeout, err := d.Int32()
		if err != nil {
			return err
		}
		r.ThrottleTime = time.Duration(timeout) * time.Millisecond
	}
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
	r.Members = make([]Member, memberCount)
	for i := 0; i < memberCount; i++ {
		id, err := d.String()
		if err != nil {
			return err
		}
		metadata, err := d.Bytes()
		if err != nil {
			return err
		}
		r.Members[i] = Member{MemberID: id, MemberMetadata: metadata}
	}
	return nil
}

func (r *JoinGroupResponse) Key() int16 {
	return 11
}

func (r *JoinGroupResponse) Version() int16 {
	return r.APIVersion
}
