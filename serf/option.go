package serf

import "github.com/travisjeffery/jocko"

type OptionFn func(b *Serf)

func Logger(logger jocko.Logger) OptionFn {
	return func(b *Serf) {
		b.logger = logger
	}
}

func Addr(serfAddr string) OptionFn {
	return func(b *Serf) {
		b.addr = serfAddr
	}
}

func InitMembers(serfMembers []string) OptionFn {
	return func(b *Serf) {
		b.initMembers = serfMembers
	}
}
