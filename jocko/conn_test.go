package jocko

import (
	"context"
	"io"
	"net"
	"os"
	"testing"
	"time"

	"github.com/travisjeffery/jocko/jocko/config"
	"github.com/travisjeffery/jocko/protocol"
)

const (
	tcp = "tcp"
)

type connPipe struct {
	rconn *Conn
	wconn *Conn
}

type timeout struct{}

func (*timeout) Error() string   { return "timeout" }
func (*timeout) Temporary() bool { return true }
func (*timeout) Timeout() bool   { return true }

func (c *connPipe) Close() error {
	b := [1]byte{}
	c.wconn.SetWriteDeadline(time.Time{})
	c.wconn.Write(b[:])
	c.wconn.Close()
	c.rconn.Close()
	return nil
}

func (c *connPipe) Read(b []byte) (int, error) {
	time.Sleep(time.Millisecond)
	if t := c.rconn.readDeadline(); !t.IsZero() && t.Sub(time.Now()) <= (10*time.Millisecond) {
		return 0, &timeout{}
	}
	n, err := c.rconn.Read(b)
	if n == 1 && b[0] == 0 {
		c.rconn.Close()
		n, err = 0, io.EOF
	}
	return n, err
}

func (c *connPipe) Write(b []byte) (int, error) {
	time.Sleep(time.Millisecond)
	if t := c.wconn.writeDeadline(); !t.IsZero() && t.Sub(time.Now()) <= (10*time.Millisecond) {
		return 0, &timeout{}
	}
	return c.wconn.Write(b)
}

func (c *connPipe) LocalAddr() net.Addr {
	return c.rconn.LocalAddr()
}

func (c *connPipe) RemoteAddr() net.Addr {
	return c.wconn.RemoteAddr()
}

func (c *connPipe) SetDeadline(t time.Time) error {
	c.rconn.SetDeadline(t)
	c.wconn.SetDeadline(t)
	return nil
}

func (c *connPipe) SetReadDeadline(t time.Time) error {
	c.rconn.SetReadDeadline(t)
	return nil
}

func (c *connPipe) SetWriteDeadline(t time.Time) error {
	c.wconn.SetWriteDeadline(t)
	return nil
}

func TestConn(t *testing.T) {
	s, dir := NewTestServer(t, func(cfg *config.Config) {
		cfg.Bootstrap = true
		cfg.BootstrapExpect = 1
		cfg.StartAsLeader = true
	}, nil)
	defer os.RemoveAll(dir)
	err := s.Start(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer s.Shutdown()

	tests := []struct {
		name string
		fn   func(*testing.T, *Conn)
	}{
		{
			name: "close immediately",
			fn:   testConnClose,
		},
		{
			name: "create topice",
			fn:   testConnCreateTopic,
		},
		{
			name: "leader and isr",
			fn:   testConnLeaderAndISR,
		},
		{
			name: "fetch",
			fn:   testConnFetch,
		},
		{
			name: "alter configs",
			fn:   testConnAlterConfigs,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			conn, err := (&Dialer{
				Resolver: &net.Resolver{},
			}).DialContext(ctx, tcp, s.Addr().String())
			if err != nil {
				t.Fatal(err)
			}
			defer conn.Close()
			test.fn(t, conn)
		})
	}
}

func testConnClose(t *testing.T, conn *Conn) {
	if err := conn.Close(); err != nil {
		t.Error(err)
	}
}

func testConnCreateTopic(t *testing.T, conn *Conn) {
	if _, err := conn.CreateTopics(&protocol.CreateTopicRequests{
		Requests: []*protocol.CreateTopicRequest{{
			Topic:             "test_topic",
			NumPartitions:     4,
			ReplicationFactor: 1,
		}},
	}); err != nil {
		t.Error(err)
	}
}

func testConnLeaderAndISR(t *testing.T, conn *Conn) {
	if _, err := conn.LeaderAndISR(&protocol.LeaderAndISRRequest{
		ControllerID: 1,
		PartitionStates: []*protocol.PartitionState{{
			Topic:     "test_topic",
			Partition: 1,
			Leader:    1,
			ISR:       []int32{1},
			Replicas:  []int32{1},
		}},
	}); err != nil {
		t.Error(err)
	}
}

func testConnFetch(t *testing.T, conn *Conn) {
	if _, err := conn.Fetch(&protocol.FetchRequest{
		ReplicaID: 1,
		Topics: []*protocol.FetchTopic{{
			Topic: "test_topic",
			Partitions: []*protocol.FetchPartition{{
				Partition:   1,
				FetchOffset: 0,
			}},
		}},
	}); err != nil {
		t.Error(err)
	}
}

func testConnAlterConfigs(t *testing.T, conn *Conn) {
	t.Skip()

	val := "max"
	if _, err := conn.AlterConfigs(&protocol.AlterConfigsRequest{
		Resources: []protocol.AlterConfigsResource{
			{
				Type: 1,
				Name: "system",
				Entries: []protocol.AlterConfigsEntry{{
					Name:  "memory",
					Value: &val,
				}},
			},
		},
	}); err != nil {
		t.Error(err)
	}
}

func testConnDescribeConfigs(t *testing.T, conn *Conn) {
	t.Skip()

	if _, err := conn.DescribeConfigs(&protocol.DescribeConfigsRequest{
		Resources: []protocol.DescribeConfigsResource{
			{
				Type: 1,
				Name: "system",
			},
		},
	}); err != nil {
		t.Error(err)
	}
}
