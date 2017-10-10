package testutil

import (
	"github.com/travisjeffery/simplelog"
	"os"
)

// NewTestLogger creates a standard logger for test use.
func NewTestLogger() *simplelog.Logger {
	return simplelog.New(
		os.Stdout,
		simplelog.INFO,
		"jocko/test",
	)
}
