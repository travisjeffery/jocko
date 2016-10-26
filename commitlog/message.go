package commitlog

import (
	"encoding/binary"
	"hash/crc32"
)

const (
	compverPos   = 0
	crc32Pos     = 1
	pLenPos      = 5
	payloadPos   = 9
	msgHeaderLen = payloadPos
)

var (
	big = binary.BigEndian
)

type Message []byte

func NewMessage(p []byte) Message {
	b := make([]byte, len(p)+msgHeaderLen)
	big.PutUint32(b[crc32Pos:crc32Pos+4], crc32.ChecksumIEEE(p))
	big.PutUint32(b[pLenPos:pLenPos+4], uint32(len(p)))
	copy(b[payloadPos:], p)
	return Message(b)
}

func (m Message) Size() int32 {
	return int32(m.PLen() + msgHeaderLen)
}

func (m Message) PLen() int32 {
	return int32(big.Uint32(m[pLenPos : pLenPos+4]))
}

func (m Message) Payload() []byte {
	return m[payloadPos:]
}
