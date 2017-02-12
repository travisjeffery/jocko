package raft

import (
	"github.com/hashicorp/raft"
	"github.com/travisjeffery/simplelog"
)

type OptionFn func(b *Raft)

func Logger(logger *simplelog.Logger) OptionFn {
	return func(b *Raft) {
		b.logger = logger
	}
}

func DataDir(dataDir string) OptionFn {
	return func(b *Raft) {
		b.dataDir = dataDir
	}
}

func Addr(raftAddr string) OptionFn {
	return func(b *Raft) {
		b.addr = raftAddr
	}
}

func Config(raft *raft.Config) OptionFn {
	return func(b *Raft) {
		b.config = raft
	}
}
