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

}

func (b *Broker) Addr() string {
	return fmt.Sprintf("%s:%s", b.Host, b.Port)
}
