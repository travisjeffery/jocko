package broker

import (
	"encoding/json"
	"net"
	"strconv"
	"time"

	"github.com/pkg/errors"
)

const (
	waitDelay = 100 * time.Millisecond
)

func (s *Broker) WaitForLeader(timeout time.Duration) (string, error) {
	tick := time.NewTicker(waitDelay)
	defer tick.Stop()

	timer := time.NewTimer(timeout)
	defer timer.Stop()

	for {
		select {
		case <-tick.C:
			l := s.raft.LeaderID()
			if l != "" {
				return l, nil
			}
		case <-timer.C:
		}
	}
}

/*func (s *Broker) WaitForAppliedIndex(idx uint64, timeout time.Duration) error {
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
}*/

func getPortFromAddr(addr string) (int, error) {
	_, strPort, err := net.SplitHostPort(addr)
	if err != nil {
		return 0, err
	}

	port, err := strconv.Atoi(strPort)
	if err != nil {
		return 0, err
	}
	return port, nil
}

func unmarshalData(data *json.RawMessage, p interface{}) error {
	b, err := data.MarshalJSON()
	if err != nil {
		return errors.Wrap(err, "json marshal failed")
	}
	if err := json.Unmarshal(b, p); err != nil {
		return errors.Wrap(err, "json unmarshal failed")
	}
	return nil
}
