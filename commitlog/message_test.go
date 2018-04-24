package commitlog

import (
	"testing"

	"github.com/stretchr/testify/require"
)

var (
	emptyMessage = []byte{
		167, 236, 104, 3, // CRC
		0x00,                   // magic version byte
		0x00,                   // attribute flags
		0xFF, 0xFF, 0xFF, 0xFF, // key
		0xFF, 0xFF, 0xFF, 0xFF} // value

	emptyV1Message = []byte{
		204, 47, 121, 217, // CRC
		0x01,                                           // magic version byte
		0x00,                                           // attribute flags
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // timestamp
		0xFF, 0xFF, 0xFF, 0xFF, // key
		0xFF, 0xFF, 0xFF, 0xFF} // value

	// TODO: v2

)

func TestMessage(t *testing.T) {
	req := require.New(t)

	m := Message(emptyMessage)
	req.Equal(int8(0), m.MagicByte())
	req.Nil(m.Key())
	req.Nil(m.Value())
	req.Equal(int32(14), m.Size())

	m = Message(emptyV1Message)
	req.Equal(int8(1), m.MagicByte())
	req.Nil(m.Key())
	req.Nil(m.Value())
	req.Equal(int32(22), m.Size())
}
