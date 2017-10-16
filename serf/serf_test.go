package serf_test

import (
	"fmt"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/travisjeffery/jocko"
	"github.com/travisjeffery/jocko/serf"
	"github.com/travisjeffery/jocko/testutil"
	"github.com/travisjeffery/simplelog"
)

var (
	logger   *simplelog.Logger
	serfPort int
)

func init() {
	logger = simplelog.New(os.Stdout, simplelog.INFO, "jocko/serf_test")
	serfPort = 7946
}

func Test_Membership(t *testing.T) {
	s0, err := getSerf(0)
	require.NoError(t, err)
	s1, err := getSerf(1)
	require.NoError(t, err)

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
		require.NoError(t, s1.Shutdown())

		testutil.WaitForResult(func() (bool, error) {
			if len(s0.Cluster()) != 1 {
				return false, nil
			}
			return true, nil
		}, func(err error) {
			t.Fatalf("err: %v", err)
		})
	})

	require.NoError(t, s0.Shutdown())
}

func getSerf(id int32) (*serf.Serf, error) {
	s, err := serf.New(
		serf.Logger(logger),
		serf.Addr(getSerfAddr()),
	)
	if err != nil {
		return nil, err
	}
	member := &jocko.ClusterMember{
		ID: id,
	}
	if err := s.Bootstrap(member, make(chan *jocko.ClusterMember, 32)); err != nil {
		return nil, err
	}
	return s, nil
}

func getSerfAddr() string {
	serfPort++
	return fmt.Sprintf("0.0.0.0:%d", serfPort)
}

func testJoin(t *testing.T, s0 *serf.Serf, other ...*serf.Serf) {
	for ind, s1 := range other {
		num, err := s1.Join(s0.Addr())
		require.NoError(t, err)
		require.Equal(t, ind+1, num)
	}
}
