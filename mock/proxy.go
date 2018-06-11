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

func (p *Client) Fetch(fetchRequest *protocol.FetchRequest) (*protocol.FetchResponse, error) {
	if len(p.msgs) >= p.msgCount {
		return &protocol.FetchResponse{}, nil
	}
	msgs := [][]byte{
		[]byte("msg " + strconv.Itoa(len(p.msgs))),
	}
	response := &protocol.FetchResponse{
		Responses: protocol.FetchTopicResponses{{
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

func (p *Client) CreateTopics(createRequest *protocol.CreateTopicRequests) (*protocol.CreateTopicsResponse, error) {
	return nil, nil
}

func (p *Client) LeaderAndISR(request *protocol.LeaderAndISRRequest) (*protocol.LeaderAndISRResponse, error) {
	return nil, nil
}
