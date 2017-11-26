package raft

import (
	"github.com/hashicorp/raft"
	"github.com/travisjeffery/jocko"
)

type OptionFn func(b *Raft)

func Logger(logger jocko.Logger) OptionFn {
	return func(b *Raft) {
		b.logger = logger
	}
}

func DataDir(dataDir string) OptionFn {
	return func(b *Raft) {
		b.dataDir = dataDir
	}
}

func Addr(addr string) OptionFn {
	return func(b *Raft) {
		b.addr = addr
	}
}

func Config(raft *raft.Config) OptionFn {
	return func(b *Raft) {
		b.config = raft
	}
}
