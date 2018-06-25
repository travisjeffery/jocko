package metadata

import (
	"fmt"
	"net"
	"strconv"

	"github.com/hashicorp/serf/serf"
)

type NodeID int32

func (n NodeID) Int32() int32 {
	return int32(n)
}

func (n NodeID) String() string {
	return fmt.Sprintf("%d", n)
}

type Broker struct {
	ID          NodeID
	Name        string
	Bootstrap   bool
	Expect      int
	NonVoter    bool
	Status      serf.MemberStatus
	RaftAddr    string
	SerfLANAddr string
	BrokerAddr  string
}

func (b Broker) Host() string {
	host, _, err := net.SplitHostPort(b.BrokerAddr)
	if err != nil {
		panic(err)
	}
	return host
}

func (b Broker) Port() int32 {
	_, portStr, err := net.SplitHostPort(b.BrokerAddr)
	if err != nil {
		panic(err)
	}
	port, err := strconv.Atoi(portStr)
	if err != nil {
		panic(err)
	}
	return int32(port)
}

func (b Broker) String() string {
	return fmt.Sprintf("broker: %d", b.ID)
}

// IsBroker checks if the given serf.Member is a broker, building and returning Broker instance from the Member's tags if so.
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
		ID:          NodeID(id),
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
