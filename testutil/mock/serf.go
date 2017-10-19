package mock

import "github.com/travisjeffery/jocko"

type Serf struct {
	BootstrapFn      func(node *jocko.ClusterMember, reconcileCh chan<- *jocko.ClusterMember) error
	BootstrapInvoked bool
	ClusterFn        func() []*jocko.ClusterMember
	ClusterInvoked   bool
	MemberFn         func(memberID int32) *jocko.ClusterMember
	MemberInvoked    bool
	JoinFn           func(addrs ...string) (int, error)
	JoinInvoked      bool
	ShutdownFn       func() error
	ShutdownInvoked  bool
	IDFn             func() int32
	IDInvoked        bool
}

func (s *Serf) Bootstrap(node *jocko.ClusterMember, reconcileCh chan<- *jocko.ClusterMember) error {
	s.BootstrapInvoked = true
	return s.BootstrapFn(node, reconcileCh)
}

func (s *Serf) Cluster() []*jocko.ClusterMember {
	s.ClusterInvoked = true
	return s.ClusterFn()
}

func (s *Serf) Member(memberID int32) *jocko.ClusterMember {
	s.MemberInvoked = true
	return s.MemberFn(memberID)
}

func (s *Serf) Join(addrs ...string) (int, error) {
	s.JoinInvoked = true
	return s.JoinFn(addrs...)
}

func (s *Serf) Shutdown() error {
	s.ShutdownInvoked = true
	return s.ShutdownFn()
}

func (s *Serf) ID() int32 {
	s.IDInvoked = true
	return s.IDFn()
}
