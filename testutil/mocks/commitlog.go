package mocks

import (
	"io"
	"sync"
)

type CommitLog struct {
	mu  sync.RWMutex
	log [][]byte
}

func NewCommitLog() *CommitLog {
	return &CommitLog{}
}

func (c *CommitLog) Log() [][]byte {
	log := [][]byte{}
	c.mu.RLock()
	log = append(log, c.log...)
	c.mu.RUnlock()
	return log
}

func (c *CommitLog) Append(b []byte) (int64, error) {
	c.mu.Lock()
	c.log = append(c.log, b)
	c.mu.Unlock()
	return 0, nil
}

func (c *CommitLog) DeleteAll() error {
	return nil
}

func (c *CommitLog) NewReader(offset int64, maxBytes int32) (io.Reader, error) {
	return nil, nil
}

func (c *CommitLog) TruncateTo(int64) error {
	return nil
}

func (c *CommitLog) NewestOffset() int64 {
	return 0
}

func (c *CommitLog) OldestOffset() int64 {
	return 0
}
