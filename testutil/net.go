package testutil

import (
	"net"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/travisjeffery/jocko"
)

// NewTestAddr finds a free port on localhost from the OS and returns
// an address string.
func NewTestAddr(t *testing.T) string {
	tcp, err := net.Listen("tcp", "localhost:0")
	assert.NoError(t, err)
	defer tcp.Close()

	return tcp.Addr().String()
}

// NewTestPort finds and returns a free port on localhost from the OS.
func NewTestPort(t *testing.T) int {
	_, port, err := jocko.SplitHostPort(NewTestAddr(t))
	assert.NoError(t, err)

	return port
}
