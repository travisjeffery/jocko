package protocol

import "time"

type DescribeGroupsResponse struct {
	APIVersion int16

	ThrottleTime time.Duration
	Groups       []Group
}

func (r *DescribeGroupsResponse) Encode(e PacketEncoder) error {
	if r.APIVersion >= 1 {
		e.PutInt32(int32(r.ThrottleTime / time.Millisecond))
	}
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

func (r *DescribeGroupsResponse) Decode(d PacketDecoder, version int16) (err error) {
	r.APIVersion = version
	if version >= 1 {
		throttle, err := d.Int32()
		if err != nil {
			return err
		}
		r.ThrottleTime = time.Duration(throttle) * time.Millisecond
	}
	groupCount, err := d.ArrayLength()
	if err != nil {
		return err
	}
	r.Groups = make([]Group, groupCount)
	for i := 0; i < groupCount; i++ {
		r.Groups[i] = Group{}
		if err := r.Groups[i].Decode(d, version); err != nil {
			return err
		}
	}
	return nil
}

func (r *DescribeGroupsResponse) Key() int16 {
	return 15
}

type Group struct {
	ErrorCode    Error
	GroupID      string
	State        string
	ProtocolType string
	Protocol     string
	GroupMembers map[string]*GroupMember
}

func (r *Group) Encode(e PacketEncoder) error {
	e.PutInt16FromError(r.ErrorCode)
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

func (r *Group) Decode(d PacketDecoder, version int16) (err error) {
	if r.ErrorCode, err = d.Int16AsError(); err != nil {
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
		if err := r.GroupMembers[memberID].Decode(d, version); err != nil {
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

func (r *GroupMember) Decode(d PacketDecoder, version int16) (err error) {
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
