package cluster

import "fmt"

type Broker struct {
	ID   int    `json:"id"`
	Host string `json:"host"`
	Port string `json:"port"`
}

func (b *Broker) Addr() string {
	return fmt.Sprintf("%s:%s", b.Host, b.Port)
}
