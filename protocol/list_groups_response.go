package protocol

import "time"

type ListGroup struct {
	GroupID      string
	ProtocolType string
}

type ListGroupsResponse struct {
	APIVersion int16

	ThrottleTime time.Duration
	ErrorCode    Error
	Groups       []ListGroup
}

func (r *ListGroupsResponse) Encode(e PacketEncoder) error {
	if r.APIVersion >= 1 {
		e.PutInt32(int32(r.ThrottleTime / time.Millisecond))
	}
	e.PutInt16FromError(r.ErrorCode)
	if err := e.PutArrayLength(len(r.Groups)); err != nil {
		return err
	}
	for _, group := range r.Groups {
		if err := e.PutString(group.GroupID); err != nil {
			return err
		}
		if err := e.PutString(group.ProtocolType); err != nil {
			return err
		}
	}
	return nil
}

func (r *ListGroupsResponse) Decode(d PacketDecoder, version int16) (err error) {
	r.APIVersion = version
	if version >= 1 {
		throttle, err := d.Int32()
		if err != nil {
			return err
		}
		r.ThrottleTime = time.Duration(throttle) * time.Millisecond
	}
	if r.ErrorCode, err = d.Int16AsError(); err != nil {
		return err
	}
	groupCount, err := d.ArrayLength()
	if err != nil {
		return err
	}
	r.Groups = make([]ListGroup, groupCount)
	for i := 0; i < groupCount; i++ {
		groupID, err := d.String()
		if err != nil {
			return err
		}
		protocolType, err := d.String()
		if err != nil {
			return err
		}
		r.Groups[i] = ListGroup{GroupID: groupID, ProtocolType: protocolType}
	}
	return nil
}

func (r *ListGroupsResponse) Key() int16 {
	return 16
}

func (r *ListGroupsResponse) Version() int16 {
	return r.APIVersion
}
