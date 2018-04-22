package commitlog

type Message []byte

func NewMessage(p []byte) Message {
	return Message(p)
}

func (m Message) Crc() int32 {
	return int32(Encoding.Uint32(m))
}

func (m Message) MagicByte() int8 {
	return int8(m[4])
}

func (m Message) Attributes() int8 {
	return int8(m[5])
}

func (m Message) Timestamp() int64 {
	if m.MagicByte() < 1 {
		panic("magic byte < 1")
	}
	return int64(Encoding.Uint64(m[6:]))
}

func (m Message) Key() []byte {
	start, end, size := m.keyOffsets()
	if size == -1 {
		return nil
	}
	return m[start+4 : end]
}

func (m Message) Value() []byte {
	start, end, size := m.valueOffsets()
	if size == -1 {
		return nil
	}
	return m[start+4 : end]
}

func (m Message) keyOffsets() (start, end, size int32) {
	if m.MagicByte() == 0 {
		start = 6
	} else {
		start = 14
	}
	size = int32(Encoding.Uint32(m[start:]))
	end = start + 4 + size
	return
}

func (m Message) valueOffsets() (start, end, size int32) {
	keyStart, keyEnd, keySize := m.keyOffsets()
	if keySize == -1 {
		start = keyStart + 4
	} else {
		start = keyEnd + 1
	}
	size = int32(Encoding.Uint32(m[start:]))
	end = start + size
	return
}
