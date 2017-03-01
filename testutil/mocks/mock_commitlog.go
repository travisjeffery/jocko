package mocks

import (
	"io"
	"sync"
)

type MockCommitLog struct {
	mu  sync.RWMutex
	log [][]byte
}

func NewCommitLog() *MockCommitLog {
	return &MockCommitLog{}
}

func (c *MockCommitLog) Log() [][]byte {
	log := [][]byte{}
	c.mu.RLock()
	log = append(log, c.log...)
	c.mu.RUnlock()
	return log
}

func (c *MockCommitLog) Append(b []byte) (int64, error) {
	c.mu.Lock()
	c.log = append(c.log, b)
	c.mu.Unlock()
	return 0, nil
}

func (c *MockCommitLog) DeleteAll() error {
	return nil
}

func (c *MockCommitLog) NewReader(offset int64, maxBytes int32) (io.Reader, error) {
	return nil, nil
}

func (c *MockCommitLog) TruncateTo(int64) error {
	return nil
}

func (c *MockCommitLog) NewestOffset() int64 {
	return 0
}

func (c *MockCommitLog) OldestOffset() int64 {
	return 0
}
