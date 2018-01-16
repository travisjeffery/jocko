package metadata

import (
	"net"
	"strconv"

	"github.com/hashicorp/serf/serf"
)

type Broker struct {
	Name        string
	ID          int32
	Bootstrap   bool
	Expect      int
	NonVoter    bool
	Status      serf.MemberStatus
	RaftAddr    string
	SerfLANAddr string
	BrokerAddr  string
	conn        net.Conn
}

// TODO: probably a better way of doing this

// Write is used to write the member.
func (b *Broker) Write(p []byte) (int, error) {
	if b.conn == nil {
		if err := b.connect(); err != nil {
			return 0, err
		}
	}
	return b.conn.Write(p)
}

// Read is used to read from the member.
func (b *Broker) Read(p []byte) (int, error) {
	if b.conn == nil {
		if err := b.connect(); err != nil {
			return 0, err
		}
	}
	return b.conn.Read(p)
}

// connect opens a tcp connection to the cluster member.
func (b *Broker) connect() error {
	host, portStr, err := net.SplitHostPort(b.BrokerAddr)
	if err != nil {
		return err
	}
	port, err := strconv.Atoi(portStr)
	if err != nil {
		return err
	}
	addr := &net.TCPAddr{IP: net.ParseIP(host), Port: port}
	conn, err := net.DialTCP("tcp", nil, addr)
	if err != nil {
		return err
	}
	b.conn = conn
	return nil
}

func IsBroker(m serf.Member) (*Broker, bool) {
	if m.Tags["role"] != "jocko" {
		return nil, false
	}

	expect := 0
	expectStr, ok := m.Tags["expect"]
	var err error
	if ok {
		expect, err = strconv.Atoi(expectStr)
		if err != nil {
			return nil, false
		}
	}

	_, bootstrap := m.Tags["bootstrap"]
	_, nonVoter := m.Tags["non_voter"]

	idStr := m.Tags["id"]
	id, err := strconv.Atoi(idStr)
	if err != nil {
		return nil, false
	}

	return &Broker{
		ID:          int32(id),
		Name:        m.Tags["name"],
		Bootstrap:   bootstrap,
		Expect:      expect,
		NonVoter:    nonVoter,
		Status:      m.Status,
		RaftAddr:    m.Tags["raft_addr"],
		SerfLANAddr: m.Tags["serf_lan_addr"],
		BrokerAddr:  m.Tags["broker_addr"],
	}, true
}
