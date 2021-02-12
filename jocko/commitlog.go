package jocko

import (
	"io"
	"os"
)

type CommitLog interface {
	Delete() error
	NewReader(offset int64, maxBytes int32) (io.Reader, error)
	Truncate(int64) error
	NewestOffset() int64
	OldestOffset() int64
	Append([]byte) (int64, error)
	SendfileParams(offset int64, maxBytes int32) (*os.File, int64, int, error)
}
