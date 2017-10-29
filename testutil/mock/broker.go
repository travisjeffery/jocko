package mock

import (
	"context"

	"github.com/travisjeffery/jocko"
	"github.com/travisjeffery/jocko/protocol"
)

type Broker struct {
	RunFn           func(context.Context, <-chan jocko.Request, chan<- jocko.Response)
	RunInvoked      bool
	JoinFn          func(addr ...string) protocol.Error
	JoinInvoked     bool
	ShutdownFn      func() error
	ShutdownInvoked bool
}

func (b *Broker) Run(ctx context.Context, requests <-chan jocko.Request, responses chan<- jocko.Response) {
	b.RunInvoked = true
	b.RunFn(ctx, requests, responses)
}

func (b *Broker) Join(addr ...string) protocol.Error {
	b.JoinInvoked = true
	return b.JoinFn(addr...)
}

func (b *Broker) Shutdown() error {
	b.ShutdownInvoked = true
	return b.Shutdown()
}
