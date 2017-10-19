package mock

import "github.com/travisjeffery/jocko"

type Raft struct {
	BootstrapFn      func(serf jocko.Serf, serfEventCh <-chan *jocko.ClusterMember, commandCh chan<- jocko.RaftCommand) error
	BootstrapInvoked bool
	ApplyFn          func(cmd jocko.RaftCommand) error
	ApplyInvoked     bool
	IsLeaderFn       func() bool
	IsLeaderInvoked  bool
	LeaderIDFn       func() string
	LeaderIDInvoked  bool
	ShutdownFn       func() error
	ShutdownInvoked  bool
	AddrFn           func() string
	AddrInvoked      bool
}

func (r *Raft) Bootstrap(serf jocko.Serf, serfEventCh <-chan *jocko.ClusterMember, commandCh chan<- jocko.RaftCommand) error {
	r.BootstrapInvoked = true
	return r.BootstrapFn(serf, serfEventCh, commandCh)
}

func (r *Raft) Apply(cmd jocko.RaftCommand) error {
	r.ApplyInvoked = true
	return r.ApplyFn(cmd)
}

func (r *Raft) IsLeader() bool {
	r.IsLeaderInvoked = true
	return r.IsLeaderFn()
}

func (r *Raft) LeaderID() string {
	r.LeaderIDInvoked = true
	return r.LeaderIDFn()
}

func (r *Raft) Shutdown() error {
	r.ShutdownInvoked = true
	return r.ShutdownFn()
}

func (r *Raft) Addr() string {
	r.AddrInvoked = true
	return r.AddrFn()
}
