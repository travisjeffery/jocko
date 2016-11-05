package encoding

import (
	"encoding/binary"
	"io"
)

var Enc = binary.BigEndian

func Read(r io.Reader, data interface{}) error {
	return binary.Read(r, Enc, data)
}

func Write(w io.Writer, data interface{}) error {
	return binary.Write(w, Enc, data)
}

func Size(v interface{}) int {
	return binary.Size(v)
}
