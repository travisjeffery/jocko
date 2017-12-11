package mock

import (
	"strconv"

	"github.com/travisjeffery/jocko/protocol"
)

// Client for testing
type Client struct {
	msgCount int
	msgs     [][]byte
}

// NewClient is a client that fetches given number of msgs
func NewClient(msgCount int) *Client {
	return &Client{
		msgCount: msgCount,
	}
}

func (p *Client) Messages() [][]byte {
	return p.msgs
}

func (p *Client) FetchMessages(clientID string, fetchRequest *protocol.FetchRequest) (*protocol.FetchResponses, error) {
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

func (p *Client) CreateTopics(clientID string, createRequests *protocol.CreateTopicRequests) (*protocol.CreateTopicsResponse, error) {
	return nil, nil
}
