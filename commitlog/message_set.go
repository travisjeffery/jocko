package commitlog

const (
	offsetPos       = 0
	sizePos         = 8
	msgSetHeaderLen = 12
)

type MessageSet []byte

func NewMessageSet(msgs []Message, offset uint64) MessageSet {
	ms := make([]byte, msgSetHeaderLen)
	var n uint32
	big.PutUint64(ms[offsetPos:offsetOffset+8], offset)
	for _, m := range msgs {
		ms = append(ms, m...)
		n += uint32(len(m))
	}
	big.PutUint32(ms[sizePos:sizePos+4], n)
	return ms
}

func (ms MessageSet) Offset() int64 {
	return int64(big.Uint64(ms[offsetPos : offsetPos+8]))
}

func (ms MessageSet) Size() int32 {
	return int32(big.Uint32(ms[sizePos:sizePos+4]) + msgSetHeaderLen)
}

func (ms MessageSet) Payload() []byte {
	return ms[msgSetHeaderLen:]
}

func (ms MessageSet) Messages() (msgs []Message) {
	p := ms.Payload()
	for {
		header := Message(p[:msgHeaderLen])
		msg := Message(p[:header.Size()])
		msgs = append(msgs, msg)
		p = p[msg.Size():]
		if len(p) == 0 {
			break
		}
	}
	return msgs
}
