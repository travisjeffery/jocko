package jocko

import (
	"context"
	"crypto/tls"
	"encoding/binary"
	"io"
	"math"
	"net"
	"time"
)

const (
	defaultRTT = 1 * time.Second
	maxTimeout = time.Duration(math.MaxInt32) * time.Millisecond
	minTimeout = time.Duration(math.MinInt32) * time.Millisecond
)

// Dialer is like the net.Dialer API but for opening connections to Jocko brokers.
type Dialer struct {
	// Unique ID for client connections established by this Dialer.
	ClientID string
	// Timeout is the max duration a dial will wait for a connect to complete.
	Timeout time.Duration
	// Deadline is the absolute time after which the dial will fail. Zero is no deadline.
	Deadline time.Time
	// LocalAddr is the local address to dial.
	LocalAddr net.Addr
	// RemoteAddr is the remote address to dial.
	RemoteAddr net.Addr
	// KeepAlive is the keep-alive period for a network connection.
	KeepAlive time.Duration
	// FallbackDelay is the duration to wait before spawning a fallback connection. If 0, default duration is 300ms.
	FallbackDelay time.Duration
	// Resolver species an alternative resolver to use.
	Resolver Resolver
	// TLS enables the Dialer to secure connections. If nil, standard net.Conn is used.
	TLS *tls.Config
	// DualStack enables RFC 6555-compliant "happy eyeballs" dialing.
	DualStack bool
	// SASL enables SASL plain authentication.
	SASL *SASL
}

var (
	defaultDialer = NewDialer("jocko")
)

// NewDialer creates a new dialer.
func NewDialer(clientID string) *Dialer {
	return &Dialer{
		Timeout:   10 * time.Second,
		DualStack: true,
		ClientID:  clientID,
	}
}

// Dial creates a connection to the broker on the given network and address on the default dialer.
func Dial(network, address string) (*Conn, error) {
	return defaultDialer.Dial(network, address)
}

func DialContext(ctx context.Context, network, address string) (*Conn, error) {
	return defaultDialer.DialContext(ctx, network, address)
}

// Dial creates a connection to the broker on the given network and address.
func (d *Dialer) Dial(network, address string) (*Conn, error) {
	return d.DialContext(context.Background(), network, address)
}

func (d *Dialer) DialContext(ctx context.Context, network, address string) (*Conn, error) {
	if d.Timeout != 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, d.Timeout)
		defer cancel()
	}

	if !d.Deadline.IsZero() {
		var cancel context.CancelFunc
		ctx, cancel = context.WithDeadline(ctx, d.Deadline)
		defer cancel()
	}

	c, err := d.dialContext(ctx, network, address)
	if err != nil {
		return nil, err
	}
	return NewConn(c, d.ClientID)
}

func (d *Dialer) dialContext(ctx context.Context, network, address string) (conn net.Conn, err error) {
	if r := d.Resolver; r != nil {
		host, port := splitHostPort(address)
		addrs, err := r.LookupHost(ctx, host)
		if err != nil {
			return nil, err
		}
		if len(addrs) != 0 {
			address = addrs[0]
		}
		if len(port) != 0 {
			address, _ = splitHostPort(address)
			address = net.JoinHostPort(address, port)
		}
	}
	conn, err = (&net.Dialer{
		LocalAddr:     d.LocalAddr,
		FallbackDelay: d.FallbackDelay,
		KeepAlive:     d.KeepAlive,
	}).DialContext(ctx, network, address)
	if err != nil {
		return
	}

	if d.TLS != nil {
		conn, err = d.connectTLS(ctx, conn)
		if err != nil {
			return
		}
	}

	if d.SASL != nil {
		if err = d.connectSASLPlain(ctx, conn); err != nil {
			conn.Close()
			return
		}
	}

	return conn, nil
}

// TODO: add unit tests
func (d *Dialer) connectTLS(ctx context.Context, conn net.Conn) (tlsConn *tls.Conn, err error) {
	tlsConn = tls.Client(conn, d.TLS)
	errc := make(chan error)
	go func() {
		defer close(errc)
		errc <- tlsConn.Handshake()
	}()
	select {
	case <-ctx.Done():
		conn.Close()
		tlsConn.Close()
		<-errc
		err = ctx.Err()
	case err = <-errc:
	}
	return
}

func (d *Dialer) connectSASLPlain(ctx context.Context, conn net.Conn) error {
	length := 1 + len(d.SASL.User) + 1 + len(d.SASL.Pass)
	msg := make([]byte, length+4) //4 byte length header + auth data
	binary.BigEndian.PutUint32(msg, uint32(length))
	copy(msg[4:], []byte("\x00"+d.SASL.User+"\x00"+d.SASL.Pass))
	_, err := conn.Write(msg)
	if err != nil {
		return err
	}
	header := make([]byte, 4)
	_, err = io.ReadFull(conn, header)
	if err != nil {
		return err
	}
	return nil
}

func splitHostPort(s string) (string, string) {
	host, port, _ := net.SplitHostPort(s)
	if len(host) == 0 && len(port) == 0 {
		host = s
	}
	return host, port
}

// Resolver provides service discovery of the hosts of a kafka cluster.
type Resolver interface {
	// LookupHost looks up the given host using the local resolver.
	LookupHost(ctx context.Context, host string) ([]string, error)
}

type SASL struct {
	User, Pass string
}
