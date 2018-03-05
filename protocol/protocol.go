package protocol

import (
	"encoding/binary"
	"fmt"
	"io"
)

var Encoding = binary.BigEndian

func Read(r io.Reader, data interface{}) error {
	return binary.Read(r, Encoding, data)
}

func Write(w io.Writer, data interface{}) error {
	return binary.Write(w, Encoding, data)
}

func Size(v interface{}) int {
	return binary.Size(v)
}

func MakeInt16(b []byte) int16 {
	return int16(Encoding.Uint16(b))
}

func MakeInt32(b []byte) int32 {
	return int32(Encoding.Uint32(b))
}

func MakeInt64(b []byte) int64 {
	return int64(Encoding.Uint64(b))
}

func ExpectZeroSize(sz int, err error) error {
	if err == nil && sz != 0 {
		err = fmt.Errorf("reading a response left %d unread bytes", sz)
	}
	return err
}
