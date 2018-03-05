package jocko

import (
	"bufio"
	"context"
	"crypto/tls"
	"math"
	"net"
	"runtime"
	"sync"
	"time"

	"github.com/travisjeffery/jocko/protocol"
)

const (
	defaultRTT = 1 * time.Second
	maxTimeout = time.Duration(math.MaxInt32) * time.Millisecond
	minTimeout = time.Duration(math.MinInt32) * time.Millisecond
)

// Resolver provides service discovery of the hosts of a kafka cluster.
type Resolver interface {
	// LookupHost looks up the given host using the local resolver.
	LookupHost(ctx context.Context, host string) ([]string, error)
}

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
}

var (
	defaultDialer = &Dialer{
		Timeout:   10 * time.Second,
		DualStack: true,
		ClientID:  "jocko",
	}
)

func Dial(network, address string) (*Conn, error) {
	return defaultDialer.Dial(network, address)
}

func DialContext(ctx context.Context, network, address string) (*Conn, error) {
	return defaultDialer.DialContext(ctx, network, address)
}

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

func (d *Dialer) dialContext(ctx context.Context, network, address string) (net.Conn, error) {
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
	return (&net.Dialer{
		LocalAddr:     d.LocalAddr,
		FallbackDelay: d.FallbackDelay,
		KeepAlive:     d.KeepAlive,
	}).DialContext(ctx, network, address)
}

func splitHostPort(s string) (string, string) {
	host, port, _ := net.SplitHostPort(s)
	if len(host) == 0 && len(port) == 0 {
		host = s
	}
	return host, port
}

type Conn struct {
	conn          net.Conn
	mutex         sync.Mutex
	rlock         sync.Mutex
	rbuf          bufio.Reader
	wlock         sync.Mutex
	wbuf          bufio.Writer
	wdeadline     connDeadline
	rdeadline     connDeadline
	clientID      string
	correlationID int32
}

func NewConn(conn net.Conn, clientID string) (*Conn, error) {
	return &Conn{
		conn:     conn,
		clientID: clientID,
		rbuf:     *bufio.NewReader(conn),
		wbuf:     *bufio.NewWriter(conn),
	}, nil
}

func (c *Conn) LocalAddr() net.Addr { return c.conn.LocalAddr() }

func (c *Conn) RemoteAddr() net.Addr { return c.conn.RemoteAddr() }

func (c *Conn) SetDeadline(t time.Time) error {
	c.rdeadline.setDeadline(t)
	c.wdeadline.setDeadline(t)
	return nil
}

func (c *Conn) SetReadDeadline(t time.Time) error {
	c.rdeadline.setDeadline(t)
	return nil
}

func (c *Conn) SetWriteDeadline(t time.Time) error {
	c.wdeadline.setDeadline(t)
	return nil
}

// Read satisfies net.Conn interface. Don't use it.
func (c *Conn) Read(b []byte) (int, error) {
	return 0, nil
}

func (c *Conn) Write(b []byte) (int, error) {
	return 0, nil
}

func (c *Conn) Close() error { return c.conn.Close() }

func (c *Conn) LeaderAndISR(req *protocol.LeaderAndISRRequest) (*protocol.LeaderAndISRResponse, error) {
	var resp protocol.LeaderAndISRResponse
	err := c.writeOperation(func(deadline time.Time, id int32) error {
		return c.writeRequest(req)
	}, func(deadline time.Time, size int) error {
		return c.readResponse(&resp, size)
	})
	if err != nil {
		return nil, err
	}
	return &resp, nil
}

func (c *Conn) CreateTopics(req *protocol.CreateTopicRequests) (*protocol.CreateTopicsResponse, error) {
	var resp protocol.CreateTopicsResponse
	err := c.writeOperation(func(deadline time.Time, id int32) error {
		return c.writeRequest(req)
	}, func(deadline time.Time, size int) error {
		return c.readResponse(&resp, size)
	})
	if err != nil {
		return nil, err
	}
	return &resp, nil
}

func (c *Conn) Fetch(req *protocol.FetchRequest) (*protocol.FetchResponses, error) {
	var resp protocol.FetchResponses
	err := c.readOperation(func(deadline time.Time, id int32) error {
		return c.writeRequest(req)
	}, func(deadline time.Time, size int) error {
		return c.readResponse(&resp, size)
	})
	if err != nil {
		return nil, err
	}
	return &resp, nil
}

func (c *Conn) readResponse(resp protocol.Decoder, size int) error {
	b, err := c.rbuf.Peek(size)
	if err != nil {
		return err
	}
	err = protocol.Decode(b, resp)
	c.rbuf.Discard(size)
	return err
}

func (c *Conn) writeRequest(body protocol.Body) error {
	req := &protocol.Request{
		CorrelationID: c.correlationID,
		ClientID:      c.clientID,
		Body:          body,
	}
	b, err := protocol.Encode(req)
	if err != nil {
		return err
	}
	_, err = c.wbuf.Write(b)
	if err != nil {
		return err
	}
	return c.wbuf.Flush()
}

type wop func(deadline time.Time, id int32) error
type rop func(deadline time.Time, size int) error

func (c *Conn) readOperation(write wop, read rop) error {
	return c.do(&c.rdeadline, write, read)
}

func (c *Conn) writeOperation(write wop, read rop) error {
	return c.do(&c.wdeadline, write, read)
}

func (c *Conn) do(d *connDeadline, write wop, read rop) error {
	id, err := c.doRequest(d, write)
	if err != nil {
		return err
	}
	deadline, size, lock, err := c.waitResponse(d, id)
	if err != nil {
		return err
	}

	if err = read(deadline, size); err != nil {
		switch err.(type) {
		case protocol.Error:
		default:
			c.conn.Close()
		}
	}

	d.unsetConnReadDeadline()
	lock.Unlock()
	return err
}

func (c *Conn) doRequest(d *connDeadline, write wop) (int32, error) {
	c.wlock.Lock()
	c.correlationID++
	id := c.correlationID
	err := write(d.setConnWriteDeadline(c.conn), id)
	d.unsetConnWriteDeadline()
	if err != nil {
		c.conn.Close()
	}
	c.wlock.Unlock()
	return c.correlationID, nil
}

func (c *Conn) waitResponse(d *connDeadline, id int32) (deadline time.Time, size int, lock *sync.Mutex, err error) {
	for {
		var rsz int32
		var rid int32

		c.rlock.Lock()
		deadline = d.setConnReadDeadline(c.conn)

		if rsz, rid, err = c.peekResponseSizeAndID(); err != nil {
			d.unsetConnReadDeadline()
			c.conn.Close()
			c.rlock.Unlock()
			return
		}

		if id == rid {
			c.skipResponseSizeAndID()
			size, lock = int(rsz-4), &c.rlock
			return
		}

		c.rlock.Unlock()
		runtime.Gosched()
	}
}

func (c *Conn) readDeadline() time.Time {
	return c.rdeadline.deadline()
}

func (c *Conn) writeDeadline() time.Time {
	return c.wdeadline.deadline()
}

func (c *Conn) peekResponseSizeAndID() (int32, int32, error) {
	b, err := c.rbuf.Peek(8)
	if err != nil {
		return 0, 0, nil
	}
	size, id := protocol.MakeInt32(b[:4]), protocol.MakeInt32(b[4:])
	return size, id, nil
}

func (c *Conn) skipResponseSizeAndID() {
	c.rbuf.Discard(8)
}

type connDeadline struct {
	mutex sync.Mutex
	value time.Time
	rconn net.Conn
	wconn net.Conn
}

func (d *connDeadline) deadline() time.Time {
	d.mutex.Lock()
	t := d.value
	d.mutex.Unlock()
	return t
}

func (d *connDeadline) setDeadline(t time.Time) {
	d.mutex.Lock()
	d.value = t

	if d.rconn != nil {
		d.rconn.SetReadDeadline(t)
	}

	if d.wconn != nil {
		d.wconn.SetWriteDeadline(t)
	}

	d.mutex.Unlock()
}

func (d *connDeadline) setConnReadDeadline(conn net.Conn) time.Time {
	d.mutex.Lock()
	deadline := d.value
	d.rconn = conn
	d.rconn.SetReadDeadline(deadline)
	d.mutex.Unlock()
	return deadline
}

func (d *connDeadline) setConnWriteDeadline(conn net.Conn) time.Time {
	d.mutex.Lock()
	deadline := d.value
	d.wconn = conn
	d.wconn.SetWriteDeadline(deadline)
	d.mutex.Unlock()
	return deadline
}

func (d *connDeadline) unsetConnReadDeadline() {
	d.mutex.Lock()
	d.rconn = nil
	d.mutex.Unlock()
}

func (d *connDeadline) unsetConnWriteDeadline() {
	d.mutex.Lock()
	d.wconn = nil
	d.mutex.Unlock()
}
