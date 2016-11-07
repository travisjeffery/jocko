package protocol

type Body interface {
	Encoder
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
