package jocko

import (
	"context"
	"io"

	"github.com/travisjeffery/jocko/protocol"
)

// Request represents an API request.
type Request struct {
	Ctx     context.Context
	Conn    io.ReadWriter
	Header  *protocol.RequestHeader
	Request interface{}
}

// Request represents an API request.
type Response struct {
	Ctx      context.Context
	Conn     io.ReadWriter
	Header   *protocol.RequestHeader
	Response interface{}
}
