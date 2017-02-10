package broker

import (
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/hashicorp/nomad/testutil"
	"github.com/hashicorp/raft"
	"github.com/stretchr/testify/assert"
	jockoraft "github.com/travisjeffery/jocko/raft"
	"github.com/travisjeffery/jocko/serf"
	"github.com/travisjeffery/simplelog"
)

var (
	logger   *simplelog.Logger
	serfPort int
	raftPort int
	port     int
	dataDir  string
)

func init() {
	logger = simplelog.New(os.Stdout, simplelog.INFO, "jocko/broker_test")
	serfPort = 7946
	raftPort = 5000
	port = 8000
	dataDir, _ = ioutil.TempDir("", "broker_test")
}

func TestBroker_JoinPeer(t *testing.T) {
	defer os.RemoveAll(dataDir)

	s0 := testServer(t, 0)
	defer s0.Shutdown()
	s1 := testServer(t, 1)
	defer s1.Shutdown()

	testJoin(t, s0, s1)

	testutil.WaitForResult(func() (bool, error) {
		if len(s1.Cluster()) != 2 {
			return false, fmt.Errorf("bad: %#v", s1.Cluster())
		}
		if len(s0.Cluster()) != 2 {
			return false, fmt.Errorf("bad: %#v", s0.Cluster())
		}
		return true, nil
	}, func(err error) {
		t.Fatalf("err: %v", err)
	})
}

func TestBroker_RemovePeer(t *testing.T) {
	defer os.RemoveAll(dataDir)

	s0 := testServer(t, 0)
	defer s0.Shutdown()

	s1 := testServer(t, 1)
	defer s1.Shutdown()

	testJoin(t, s0, s1)

	testutil.WaitForResult(func() (bool, error) {
		if len(s1.Cluster()) != 2 {
			return false, fmt.Errorf("bad: %#v", s1.Cluster())
		}
		if len(s0.Cluster()) != 2 {
			return false, fmt.Errorf("bad: %#v", s0.Cluster())
		}
		return true, nil
	}, func(err error) {
		t.Fatalf("err: %v", err)
	})

	s1.Shutdown()

	testutil.WaitForResult(func() (bool, error) {
		if len(s1.Cluster()) != 1 {
			return false, fmt.Errorf("bad: %#v", s1.Cluster())
		}
		if len(s0.Cluster()) != 1 {
			return false, fmt.Errorf("bad: %#v", s0.Cluster())
		}
		return true, nil
	}, func(err error) {
		t.Fatalf("err: %v", err)
	})
}

func testServer(t *testing.T, id int, opts ...BrokerFn) *Broker {
	idStr := strconv.Itoa(id)

	raftConf := raft.DefaultConfig()
	raftConf.LeaderLeaseTimeout = 50 * time.Millisecond
	raftConf.HeartbeatTimeout = 50 * time.Millisecond
	raftConf.ElectionTimeout = 50 * time.Millisecond

	serf, err := serf.New(
		serf.Logger(logger),
		serf.Addr(getSerfAddr()),
	)

	raft, err := jockoraft.New(
		jockoraft.Logger(logger),
		jockoraft.DataDir(filepath.Join(dataDir, idStr)),
		jockoraft.Addr(getRaftAddr()),
		jockoraft.Config(raftConf),
	)

	opts = append(opts, []BrokerFn{
		LogDir(filepath.Join(dataDir, idStr)),
		Addr(getBrokerAddr()),
		Serf(serf),
		Raft(raft),
		Logger(logger),
	}...)

	broker, err := New(
		int32(id),
		opts...,
	)
	assert.NoError(t, err)
	return broker
}

func getBrokerAddr() string {
	port++
	return fmt.Sprintf("0.0.0.0:%d", port)
}

func getRaftAddr() string {
	raftPort++
	return fmt.Sprintf("127.0.0.1:%d", raftPort)
}

func getSerfAddr() string {
	serfPort++
	return fmt.Sprintf("0.0.0.0:%d", serfPort)
}

func testJoin(t *testing.T, s0 *Broker, other ...*Broker) {
	for _, s1 := range other {
		num, err := s1.Join(s0.serf.Addr())
		assert.NoError(t, err)
		assert.Equal(t, 1, num)
	}
}
