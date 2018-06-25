package protocol

type GroupProtocol struct {
	ProtocolName     string
	ProtocolMetadata []byte
}

type JoinGroupRequest struct {
	APIVersion int16

	GroupID          string
	SessionTimeout   int32
	RebalanceTimeout int32
	MemberID         string
	ProtocolType     string
	GroupProtocols   []*GroupProtocol
}

func (r *JoinGroupRequest) Encode(e PacketEncoder) (err error) {
	if err = e.PutString(r.GroupID); err != nil {
		return err
	}
	e.PutInt32(r.SessionTimeout)
	if r.APIVersion >= 1 {
		e.PutInt32(r.RebalanceTimeout)
	}
	if err = e.PutString(r.MemberID); err != nil {
		return err
	}
	if err = e.PutString(r.ProtocolType); err != nil {
		return err
	}
	for _, groupProtocol := range r.GroupProtocols {
		if err = e.PutString(groupProtocol.ProtocolName); err != nil {
			return err
		}
		if err = e.PutBytes(groupProtocol.ProtocolMetadata); err != nil {
			return err
		}
	}
	return nil
}

func (r *JoinGroupRequest) Decode(d PacketDecoder, version int16) (err error) {
	r.APIVersion = version

	if r.GroupID, err = d.String(); err != nil {
		return err
	}
	if r.SessionTimeout, err = d.Int32(); err != nil {
		return err
	}
	if r.APIVersion >= 1 {
		if r.RebalanceTimeout, err = d.Int32(); err != nil {
			return err
		}
	}
	if r.MemberID, err = d.String(); err != nil {
		return err
	}
	if r.ProtocolType, err = d.String(); err != nil {
		return err
	}
	groupProtocolCount, err := d.ArrayLength()
	if err != nil {
		return err
	}
	r.GroupProtocols = make([]*GroupProtocol, groupProtocolCount)
	for i := 0; i < groupProtocolCount; i++ {
		r.GroupProtocols[i] = &GroupProtocol{}
		if r.GroupProtocols[i].ProtocolName, err = d.String(); err != nil {
			return err
		}
		if r.GroupProtocols[i].ProtocolMetadata, err = d.Bytes(); err != nil {
			return err
		}
	}
	return nil
}

func (r *JoinGroupRequest) Key() int16 {
	return JoinGroupKey
}

func (r *JoinGroupRequest) Version() int16 {
	return r.APIVersion
}
