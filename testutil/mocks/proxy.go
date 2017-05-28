package mocks

import (
	"strconv"

	"github.com/travisjeffery/jocko/protocol"
)

// Proxy for testing
type Proxy struct {
	msgCount     int
	msgs         [][]byte
}

// NewProxy is a proxy that fetches given number of msgs
func NewProxy(msgCount int) *Proxy {
	return &Proxy{
		msgCount: msgCount,
	}
}

func (p *Proxy) Messages() [][]byte {
	return p.msgs
}

func (p *Proxy) FetchMessages(clientID string, fetchRequest *protocol.FetchRequest) (*protocol.FetchResponses, error) {
	if len(p.msgs) >= p.msgCount {
		return &protocol.FetchResponses{}, nil
	}
	msgs := [][]byte{
		[]byte("msg " + strconv.Itoa(len(p.msgs))),
	}
	response := &protocol.FetchResponses{
		Responses: []*protocol.FetchResponse{{
				Topic: fetchRequest.Topics[0].Topic,
				PartitionResponses: []*protocol.FetchPartitionResponse{{
						RecordSet: msgs[0],
					},
				},
			},
		},
	}
	p.msgs = append(p.msgs, msgs...)
	return response, nil
}

func (p *Proxy) CreateTopic(clientID string, createRequest *protocol.CreateTopicRequest) (*protocol.CreateTopicsResponse, error) {
	return nil, nil
}
