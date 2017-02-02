package commitlog

const (
	offsetPos       = 0
	sizePos         = 8
	msgSetHeaderLen = 12
)

type MessageSet []byte

func NewMessageSet(offset uint64, msgs ...Message) MessageSet {
	ms := make([]byte, msgSetHeaderLen)
	var n uint32
	Encoding.PutUint64(ms[offsetPos:offsetPos+8], offset)
	for _, m := range msgs {
		ms = append(ms, m...)
		n += uint32(len(m))
	}
	Encoding.PutUint32(ms[sizePos:sizePos+4], n)
	return ms
}

func (ms MessageSet) Offset() int64 {
	return int64(Encoding.Uint64(ms[offsetPos : offsetPos+8]))
}

func (ms MessageSet) PutOffset(offset int64) {
	Encoding.PutUint64(ms[offsetPos:offsetPos+8], uint64(offset))
}

func (ms MessageSet) Size() int32 {
	return int32(Encoding.Uint32(ms[sizePos:sizePos+4]) + msgSetHeaderLen)
}

func (ms MessageSet) Payload() []byte {
	return ms[msgSetHeaderLen:]
}
