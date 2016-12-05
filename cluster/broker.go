package cluster

import (
	"fmt"
	"net"
)

type Broker struct {
	ID       int    `json:"id"`
	Host     string `json:"host"`
	Port     string `json:"port"`
	RaftAddr string `json:"raft_addr"`

	conn net.Conn
}

func (b *Broker) Addr() string {
	return fmt.Sprintf("%s:%s", b.Host, b.Port)
}

func (b *Broker) Write(p []byte) (int, error) {
	if b.conn == nil {
		if err := b.connect(); err != nil {
			return 0, err
		}
	}
	return b.conn.Write(p)
}

func (b *Broker) Read(p []byte) (int, error) {
	if b.conn == nil {
		if err := b.connect(); err != nil {
			return 0, err
		}
	}
	return b.conn.Read(p)
}

func (b *Broker) connect() error {
	tcpAddr, err := net.ResolveTCPAddr("tcp", b.Addr())
	if err != nil {
		return err
	}
	conn, err := net.DialTCP("tcp", nil, tcpAddr)
	if err != nil {
		return err
	}
	b.conn = conn
	return nil
}
