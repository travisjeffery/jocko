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
		if len(s1.members()) != 2 {
			return false, fmt.Errorf("bad: %#v", s1.members())
		}
		if len(s0.members()) != 2 {
			return false, fmt.Errorf("bad: %#v", s0.members())
		}
		return true, nil
	}, func(err error) {
		t.Fatalf("err: %v", err)
	})

	testutil.WaitForResult(func() (bool, error) {
		if len(s1.peers) != 2 {
			return false, fmt.Errorf("bad: %#v", s1.peers)
		}
		if len(s0.peers) != 2 {
			return false, fmt.Errorf("bad: %#v", s0.peers)
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
		if len(s1.members()) != 2 {
			return false, fmt.Errorf("bad: %#v", s1.members())
		}
		if len(s0.members()) != 2 {
			return false, fmt.Errorf("bad: %#v", s0.members())
		}
		return true, nil
	}, func(err error) {
		t.Fatalf("err: %v", err)
	})

	testutil.WaitForResult(func() (bool, error) {
		if len(s1.peers) != 2 {
			return false, fmt.Errorf("bad: %#v", s1.peers)
		}
		if len(s0.peers) != 2 {
			return false, fmt.Errorf("bad: %#v", s0.peers)
		}
		return true, nil
	}, func(err error) {
		t.Fatalf("err: %v", err)
	})

	s1.Leave()
	s1.Shutdown()

	testutil.WaitForResult(func() (bool, error) {
		if len(s1.peers) != 1 {
			return false, fmt.Errorf("bad: %#v", s1.peers)
		}
		if len(s0.peers) != 1 {
			return false, fmt.Errorf("bad: %#v", s0.peers)
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

	opts = append(opts, []BrokerFn{
		DataDir(filepath.Join(dataDir, idStr)),
		LogDir(filepath.Join(dataDir, idStr)),
		BindAddr("127.0.0.1"),
		Port(getPort()),
		SerfPort(getSerfPort()),
		RaftPort(getRaftPort()),
		Logger(logger),
		RaftConfig(raftConf),
	}...)

	broker, err := New(
		int32(id),
		opts...,
	)
	assert.NoError(t, err)
	return broker
}

func getRaftPort() int {
	raftPort++
	return raftPort
}

func getSerfPort() int {
	serfPort++
	return serfPort
}

func getPort() int {
	port++
	return port
}

func testJoin(t *testing.T, s0 *Broker, other ...*Broker) {
	addr := fmt.Sprintf("127.0.0.1:%d", s0.serfPort)
	for _, s1 := range other {
		num, err := s1.Join(addr)
		assert.NoError(t, err)
		assert.Equal(t, 1, num)
	}
}
