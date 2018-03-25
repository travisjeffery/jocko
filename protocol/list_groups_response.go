package protocol

type ListGroup struct {
	GroupID      string
	ProtocolType string
}

type ListGroupsResponse struct {
	ErrorCode int16
	Groups    []ListGroup
}

func (r *ListGroupsResponse) Encode(e PacketEncoder) error {
	e.PutInt16(r.ErrorCode)
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

func (r *ListGroupsResponse) Decode(d PacketDecoder) (err error) {
	if r.ErrorCode, err = d.Int16(); err != nil {
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
	return 0
}
