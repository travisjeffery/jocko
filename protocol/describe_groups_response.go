package protocol

type DescribeGroupsResponse struct {
	Groups []*Group
}

func (r *DescribeGroupsResponse) Encode(e PacketEncoder) error {
	if err := e.PutArrayLength(len(r.Groups)); err != nil {
		return err
	}
	for _, group := range r.Groups {
		if err := group.Encode(e); err != nil {
			return err
		}
	}
	return nil
}

func (r *DescribeGroupsResponse) Decode(d PacketDecoder) (err error) {
	groupCount, err := d.ArrayLength()
	r.Groups = make([]*Group, groupCount)
	for i := 0; i < groupCount; i++ {
		r.Groups[i] = new(Group)
		if err := r.Groups[i].Decode(d); err != nil {
			return err
		}
	}
	return nil
}

func (r *DescribeGroupsResponse) Key() int16 {
	return 15
}

func (r *DescribeGroupsResponse) Version() int16 {
	return 0
}

type Group struct {
	ErrorCode    int16
	GroupID      string
	State        string
	ProtocolType string
	Protocol     string
	GroupMembers map[string]*GroupMember
}

func (r *Group) Encode(e PacketEncoder) error {
	e.PutInt16(r.ErrorCode)
	if err := e.PutString(r.GroupID); err != nil {
		return err
	}
	if err := e.PutString(r.State); err != nil {
		return err
	}
	if err := e.PutString(r.ProtocolType); err != nil {
		return err
	}
	if err := e.PutString(r.Protocol); err != nil {
		return err
	}
	if err := e.PutArrayLength(len(r.GroupMembers)); err != nil {
		return err
	}
	for memberID, member := range r.GroupMembers {
		if err := e.PutString(memberID); err != nil {
			return err
		}
		if err := member.Encode(e); err != nil {
			return err
		}
	}
	return nil
}

func (r *Group) Decode(d PacketDecoder) (err error) {
	if r.ErrorCode, err = d.Int16(); err != nil {
		return err
	}
	if r.GroupID, err = d.String(); err != nil {
		return
	}
	if r.State, err = d.String(); err != nil {
		return
	}
	if r.ProtocolType, err = d.String(); err != nil {
		return
	}
	if r.Protocol, err = d.String(); err != nil {
		return
	}
	groupCount, err := d.ArrayLength()
	if err != nil {
		return err
	}
	if groupCount == 0 {
		return nil
	}
	r.GroupMembers = make(map[string]*GroupMember)
	for i := 0; i < groupCount; i++ {
		memberID, err := d.String()
		if err != nil {
			return err
		}
		r.GroupMembers[memberID] = new(GroupMember)
		if err := r.GroupMembers[memberID].Decode(d); err != nil {
			return err
		}
	}
	return nil
}

type GroupMember struct {
	ClientID              string
	ClientHost            string
	GroupMemberMetadata   []byte
	GroupMemberAssignment []byte
}

func (r *GroupMember) Encode(e PacketEncoder) error {
	if err := e.PutString(r.ClientID); err != nil {
		return err
	}
	if err := e.PutString(r.ClientHost); err != nil {
		return err
	}
	if err := e.PutBytes(r.GroupMemberMetadata); err != nil {
		return err
	}
	if err := e.PutBytes(r.GroupMemberAssignment); err != nil {
		return err
	}

	return nil
}

func (r *GroupMember) Decode(d PacketDecoder) (err error) {
	if r.ClientID, err = d.String(); err != nil {
		return err
	}
	if r.ClientHost, err = d.String(); err != nil {
		return err
	}
	if r.GroupMemberMetadata, err = d.Bytes(); err != nil {
		return err
	}
	if r.GroupMemberAssignment, err = d.Bytes(); err != nil {
		return err
	}
	return nil
}
