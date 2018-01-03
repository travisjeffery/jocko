package metadata

import (
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
