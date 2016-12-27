package broker

import (
	"net"
	"strconv"

	"github.com/hashicorp/serf/serf"
	"github.com/travisjeffery/jocko/jocko"
)

func brokerConn(m serf.Member) (*jocko.BrokerConn, error) {
	portStr := m.Tags["port"]
	port, err := strconv.Atoi(portStr)
	if err != nil {
		return nil, err
	}

	idStr := m.Tags["id"]
	id, err := strconv.Atoi(idStr)
	if err != nil {
		return nil, err
	}

	addr := &net.TCPAddr{IP: m.Addr, Port: port}
	conn := &jocko.BrokerConn{
		Addr: addr,
		ID:   int32(id),
		Port: portStr,
	}

	return conn, nil
}
