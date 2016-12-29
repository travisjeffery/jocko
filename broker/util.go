package broker

import (
	"strconv"
	"time"

	"github.com/hashicorp/serf/serf"
	"github.com/travisjeffery/jocko/jocko"
)

func (s *Broker) WaitForLeader(timeout time.Duration) (string, error) {
	tick := time.NewTicker(waitDelay)
	defer tick.Stop()

	timer := time.NewTimer(timeout)
	defer timer.Stop()

	for {
		select {
		case <-tick.C:
			l := s.raft.Leader()
			if l != "" {
				return l, nil
			}
		case <-timer.C:
		}
	}
}

func (s *Broker) WaitForAppliedIndex(idx uint64, timeout time.Duration) error {
	tick := time.NewTicker(waitDelay)
	defer tick.Stop()
	timer := time.NewTimer(timeout)
	defer timer.Stop()

	for {
		select {
		case <-tick.C:
			if s.raft.AppliedIndex() >= idx {
				return nil
			}
		case <-timer.C:
		}
	}
}

func brokerConn(m serf.Member) (*jocko.BrokerConn, error) {
	portStr := m.Tags["port"]
	port, err := strconv.Atoi(portStr)
	if err != nil {
		return nil, err
	}

	idStr := m.Tags["id"]
	id, err := strconv.Atoi(idStr)
	if err != nil {
		return nil, err
	}

	raftPortStr := m.Tags["raft_port"]
	raftPort, err := strconv.Atoi(raftPortStr)
	if err != nil {
		return nil, err
	}

	conn := &jocko.BrokerConn{
		IP:       m.Addr.String(),
		ID:       int32(id),
		RaftPort: raftPort,
		Port:     port,
	}

	return conn, nil
}
