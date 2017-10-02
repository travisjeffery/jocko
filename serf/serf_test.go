package serf_test

import (
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/travisjeffery/jocko"
	"github.com/travisjeffery/jocko/serf"
	"github.com/travisjeffery/jocko/testutil"
)

func TestNew(t *testing.T) {
	t.Run("returns serf instance", func(t *testing.T) {
		assert.IsType(t, &serf.Serf{}, testutil.NewTestSerf(t))
	})

	t.Run("yields instance to OptionFns", func(t *testing.T) {
		opt1 := func(s *serf.Serf) {
			assert.IsType(t, &serf.Serf{}, s)
		}

		opt2 := func(s *serf.Serf) {
			assert.IsType(t, &serf.Serf{}, s)
		}

		testutil.NewTestSerf(t, opt1, opt2)
	})
}

func TestConfigure(t *testing.T) {
	t.Run("adds Broker metadata", func(t *testing.T) {
		s := testutil.NewTestSerf(t)
		m, c := testutil.NewTestSerfConfig(t, s)

		assert.Equal(t, c.Tags["id"], strconv.Itoa(int(m.ID)))
		assert.Equal(t, c.Tags["port"], strconv.Itoa(int(m.Port)))
		assert.Equal(t, c.Tags["raft_port"], strconv.Itoa(int(m.RaftPort)))
	})

	t.Run("assigns serf port and address", func(t *testing.T) {
		s := testutil.NewTestSerf(t)
		_, c := testutil.NewTestSerfConfig(t, s)

		addr, port, err := jocko.SplitHostPort(s.GetAddr())
		assert.NoError(t, err)

		assert.Equal(t, c.MemberlistConfig.BindAddr, addr)
		assert.Equal(t, c.MemberlistConfig.BindPort, port)
	})
}

func TestBootstrap(t *testing.T) {
	t.Run("Handles Serf events", func(t *testing.T) {
		t.Skip()
	})
}

func TestMembership(t *testing.T) {
	s0 := testutil.NewBootstrappedTestSerf(t, 0)
	s1 := testutil.NewBootstrappedTestSerf(t, 1)

	t.Run("Join Peer", func(t *testing.T) {
		testJoin(t, s0, s1)

		testutil.WaitForResult(func() (bool, error) {
			if len(s1.Cluster()) != 2 {
				return false, nil
			}
			if len(s0.Cluster()) != 2 {
				return false, nil
			}
			return true, nil
		}, func(err error) {
			t.Fatalf("err: %v", err)
		})
	})

	t.Run("Remove Peer", func(t *testing.T) {
		assert.NoError(t, s1.Shutdown())

		testutil.WaitForResult(func() (bool, error) {
			if len(s0.Cluster()) != 1 {
				return false, nil
			}
			return true, nil
		}, func(err error) {
			t.Fatalf("err: %v", err)
		})
	})

	assert.NoError(t, s0.Shutdown())
}

func testJoin(t *testing.T, s0 *serf.Serf, other ...*serf.Serf) {
	for ind, s1 := range other {
		num, err := s1.Join(s0.Addr())
		assert.NoError(t, err)
		assert.Equal(t, ind+1, num)
	}
}
