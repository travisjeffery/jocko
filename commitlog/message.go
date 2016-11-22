package commitlog

import "encoding/binary"

var (
	big = binary.BigEndian
)

type Message []byte

func NewMessage(p []byte) Message {
	return Message(p)
}
