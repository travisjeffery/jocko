package protocol

import (
	"encoding/binary"
	"io"

	"github.com/travisjeffery/jocko/commitlog"
)

var Encoding = commitlog.Encoding

func Read(r io.Reader, data interface{}) error {
	return binary.Read(r, Encoding, data)
}

func Write(w io.Writer, data interface{}) error {
	return binary.Write(w, Encoding, data)
}

func Size(v interface{}) int {
	return binary.Size(v)
}
