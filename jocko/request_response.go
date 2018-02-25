package jocko

import (
	"io"

	"github.com/travisjeffery/jocko/protocol"
)

// Request represents an API request.
type Request struct {
	Conn    io.ReadWriter
	Header  *protocol.RequestHeader
	Request interface{}
}

// Request represents an API request.
type Response struct {
	Conn     io.ReadWriter
	Header   *protocol.RequestHeader
	Response interface{}
}
