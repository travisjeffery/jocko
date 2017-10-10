package testutil

import (
	"testing"

	hashiserf "github.com/hashicorp/serf/serf"

	"github.com/stretchr/testify/assert"
	"github.com/travisjeffery/jocko"
	"github.com/travisjeffery/jocko/serf"
)

// NewTestSerf creates a new (non-bootstrapped) Serf instance.
func NewTestSerf(t *testing.T, opts ...serf.OptionFn) *serf.Serf {
	opts = append(opts,
		serf.Logger(NewTestLogger()),
		serf.Addr(NewTestAddr(t)),
	)

	s, err := serf.New(opts...)
	assert.NoError(t, err)
	return s
}

// NewTestSerfConfig creates a new jocko.ClusterMember and
// hashicorp.serf.Config to expose the internals of Serf initialization.
func NewTestSerfConfig(t *testing.T, s *serf.Serf) (*jocko.ClusterMember, *hashiserf.Config) {
	// TODO: do these need unique IDs?
	m := NewTestClusterMember(t, 0)

	c, err := s.Configure(m)
	assert.NoError(t, err)

	return m, c
}

// NewTestClusterMember creates a new jocko.ClusterMember with free ports.
func NewTestClusterMember(t *testing.T, id int32) *jocko.ClusterMember {
	return &jocko.ClusterMember{
		ID:       id,
		Port:     NewTestPort(t),
		RaftPort: NewTestPort(t),
	}
}

// NewBootstrappedTestSerf creates a new (bootstrapped) Serf instance.
func NewBootstrappedTestSerf(t *testing.T, id int32) *serf.Serf {
	s := NewTestSerf(t)
	m := NewTestClusterMember(t, id)
	ch := make(chan *jocko.ClusterMember, 32)

	err := s.Bootstrap(m, ch)
	assert.NoError(t, err)

	return s
}
