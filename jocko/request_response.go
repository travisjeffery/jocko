package jocko

import (
	"context"
	"io"
	"sync"
	"time"

	"github.com/travisjeffery/jocko/protocol"
)

type Context struct {
	sync.Mutex
	Conn     io.ReadWriter
	Header   *protocol.RequestHeader
	Request  interface{}
	Response interface{}
	Parent   context.Context
	values   map[interface{}]interface{}
}

func (ctx *Context) Deadline() (deadline time.Time, ok bool) {
	return time.Time{}, false
}

func (ctx *Context) Done() <-chan struct{} {
	return nil
}

func (ctx *Context) Err() error {
	return nil
}

func (ctx *Context) Value(key interface{}) interface{} {
	ctx.Lock()
	if ctx.values == nil {
		ctx.values = make(map[interface{}]interface{})
	}
	val := ctx.values[key]
	if val == nil {
		val = ctx.Parent.Value(key)
	}
	ctx.Unlock()
	return val
}
