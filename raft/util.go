package raft

import "time"

const (
	waitDelay = 100 * time.Millisecond
)

func (r *Raft) WaitForLeader(timeout time.Duration) (string, error) {
	tick := time.NewTicker(waitDelay)
	defer tick.Stop()

	timer := time.NewTimer(timeout)
	defer timer.Stop()

	for {
		select {
		case <-tick.C:
			l := r.LeaderID()
			if l != "" {
				return l, nil
			}
		case <-timer.C:
		}
	}
}

func (r *Raft) WaitForAppliedIndex(idx uint64, timeout time.Duration) error {
	tick := time.NewTicker(waitDelay)
	defer tick.Stop()
	timer := time.NewTimer(timeout)
	defer timer.Stop()

	for {
		select {
		case <-tick.C:
			if r.raft.AppliedIndex() >= idx {
				return nil
			}
		case <-timer.C:
		}
	}
}
