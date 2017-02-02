package commitlog

type Message []byte

func NewMessage(p []byte) Message {
	return Message(p)
}
