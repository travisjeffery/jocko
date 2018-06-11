package protocol

import "go.uber.org/zap/zapcore"

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
	if err = pe.PutString(r.ClientID); err != nil {
		return err
	}
	if err = r.Body.Encode(pe); err != nil {
		return err
	}
	pe.Pop()
	return nil
}

func (r *Request) MarshalLogObject(e zapcore.ObjectEncoder) error {
	e.AddObject("body", r.Body.(zapcore.ObjectMarshaler))
	return nil
}
