package protocol

type ListGroupsResponse struct {
	ErrorCode int16
	Groups    map[string]string
}

func (r *ListGroupsResponse) Encode(e PacketEncoder) error {
	e.PutInt16(r.ErrorCode)
	if err := e.PutArrayLength(len(r.Groups)); err != nil {
		return err
	}
	for groupID, protocolType := range r.Groups {
		if err := e.PutString(groupID); err != nil {
			return err
		}
		if err := e.PutString(protocolType); err != nil {
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
	r.Groups = make(map[string]string)
	for i := 0; i < groupCount; i++ {
		groupID, err := d.String()
		if err != nil {
			return err
		}
		protocolType, err := d.String()
		if err != nil {
			return err
		}
		r.Groups[groupID] = protocolType
	}
	return nil
}

func (r *ListGroupsResponse) Key() int16 {
	return 16
}

func (r *ListGroupsResponse) Version() int16 {
	return 0
}
