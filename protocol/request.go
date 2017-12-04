package protocol

import "fmt"

type Body interface {
	Encoder
	Decoder
	Key() int16
	Version() int16
}

type Request struct {
	CorrelationID int32
	ClientID      string
	Body          Body
}

func (r *Request) Encode(pe PacketEncoder) (err error) {
	pe.Push(&SizeField{})
	pe.PutInt16(r.Body.Key())
	pe.PutInt16(r.Body.Version())
	pe.PutInt32(r.CorrelationID)
	pe.PutString(r.ClientID)
	if err != nil {
		return err
	}
	r.Body.Encode(pe)
	pe.Pop()
	return nil
}

func (r *Request) Decode(pd PacketDecoder) (err error) {
	var key int16
	if key, err = pd.Int16(); err != nil {
		return err
	}
	var version int16
	if version, err = pd.Int16(); err != nil {
		return err
	}
	if r.CorrelationID, err = pd.Int32(); err != nil {
		return err
	}
	r.ClientID, err = pd.String()

	r.Body = allocateBody(key, version)
	if r.Body == nil {
		return ErrUnknown.WithErr(fmt.Errorf("unknown request key: %d", key))
	}
	return r.Body.Decode(pd)
}

func allocateBody(key, version int16) Body {
	switch key {
	case 0:
		return &ProduceRequest{}
	case 1:
		return &FetchRequest{}
	case 2:
		return &OffsetsRequest{}
	case 3:
		return &MetadataRequest{}
	// case 8:
	// 	return &OffsetCommitRequest{Version: version}
	// case 9:
	// 	return &OffsetFetchRequest{}
	// case 10:
	// 	return &ConsumerMetadataRequest{}
	case 11:
		return &JoinGroupRequest{}
	// case 12:
	// 	return &HeartbeatRequest{}
	case 13:
		return &LeaveGroupRequest{}
	case 14:
		return &SyncGroupRequest{}
	case 15:
		return &DescribeGroupsRequest{}
	case 16:
		return &ListGroupsRequest{}
	// case 17:
	// 	return &SaslHandshakeRequest{}
	case 18:
		return &APIVersionsRequest{}
	}
	return nil
}
