package protocol

type MessageSet struct {
	Offset                  int64
	Size                    int32
	Messages                []*Message
	PartialTrailingMessages bool
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
		err = m.Decode(d)
		switch err {
		case nil:
			ms.Messages = append(ms.Messages, m)
		case ErrInsufficientData:
			ms.PartialTrailingMessages = true
			return nil
		default:
			return err
		}
	}
	return nil
}
