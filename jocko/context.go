package jocko

import (
	"context"
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/travisjeffery/jocko/protocol"
)

type Context struct {
	mu     sync.Mutex
	Conn   *net.TCPConn
	err    error
	header *protocol.RequestHeader
	parent context.Context
	req    interface{}
	res    interface{}
	vals   map[interface{}]interface{}
}

func (ctx *Context) Request() interface{} {
	return ctx.req
}

func (ctx *Context) Response() interface{} {
	return ctx.res
}

func (c *Context) Header() *protocol.RequestHeader {
	return c.header
}

func (ctx *Context) Deadline() (deadline time.Time, ok bool) {
	return time.Time{}, false
}

func (ctx *Context) Done() <-chan struct{} {
	return nil
}

func (ctx *Context) Err() error {
	return ctx.err
}

func (ctx *Context) String() string {
	return fmt.Sprintf("ctx: %s", ctx.header)
}

func (ctx *Context) Value(key interface{}) interface{} {
	ctx.mu.Lock()
	if ctx.vals == nil {
		ctx.vals = make(map[interface{}]interface{})
	}
	val := ctx.vals[key]
	if val == nil {
		val = ctx.parent.Value(key)
	}
	ctx.mu.Unlock()
	return val
}
