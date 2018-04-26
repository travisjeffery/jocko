package protocol

import "time"

type Message struct {
	Crc        int32
	MagicByte  int8
	Attributes int8
	Timestamp  time.Time
	Key        []byte
	Value      []byte
}

func (m *Message) Encode(e PacketEncoder) error {
	e.Push(&CRCField{})
	e.PutInt8(m.MagicByte)
	e.PutInt8(m.Attributes)
	if m.MagicByte > 0 {
		e.PutInt64(m.Timestamp.UnixNano() / int64(time.Millisecond))
	}
	if err := e.PutBytes(m.Key); err != nil {
		return err
	}
	if err := e.PutBytes(m.Value); err != nil {
		return err
	}
	e.Pop()
	return nil
}

func (m *Message) Decode(d PacketDecoder) error {
	var err error
	if err = d.Push(&CRCField{}); err != nil {
		return err
	}
	if m.MagicByte, err = d.Int8(); err != nil {
		return err
	}
	if m.Attributes, err = d.Int8(); err != nil {
		return err
	}
	if m.MagicByte > 0 {
		t, err := d.Int64()
		if err != nil {
			return err
		}
		m.Timestamp = time.Unix(t/1000, (t%1000)*int64(time.Millisecond))
	}
	if m.Key, err = d.Bytes(); err != nil {
		return err
	}
	if m.Value, err = d.Bytes(); err != nil {
		return err
	}
	return d.Pop()
}
