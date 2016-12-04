package protocol

type MessageSet struct {
	Offset   int64
	Size     int32
	Messages []*Message
}

func (ms *MessageSet) Encode(e PacketEncoder) error {
	e.PutInt64(ms.Offset)
	e.Push(&SizeField{})
	for _, m := range ms.Messages {
		if err := m.Encode(e); err != nil {
			return err
		}
	}
	e.Pop()
	return nil
}

func (ms *MessageSet) Decode(d PacketDecoder) error {
	var err error
	if ms.Offset, err = d.Int64(); err != nil {
		return err
	}
	if ms.Size, err = d.Int32(); err != nil {
		return err
	}
	for d.remaining() > 0 {
		m := new(Message)
		if err = m.Decode(d); err != nil {
			return err
		}
		ms.Messages = append(ms.Messages, m)
	}
	return nil
}
