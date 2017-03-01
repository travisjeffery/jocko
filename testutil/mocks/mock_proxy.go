package mocks

import (
	"strconv"

	"github.com/travisjeffery/jocko/protocol"
)

// MockProxy for testing
type MockProxy struct {
	msgID          int
	totalMessages  int
	mockedMessages [][]byte
}

// NewProxyForFetchMessages that fetches numMessagesToFetch messages in batches of 2
func NewProxyForFetchMessages(numMessagesToFetch int) *MockProxy {
	return &MockProxy{
		msgID:         0,
		totalMessages: numMessagesToFetch,
	}
}

func (p *MockProxy) MockedMessages() [][]byte {
	return p.mockedMessages
}

func (p *MockProxy) FetchMessages(clientID string, fetchRequest *protocol.FetchRequest) (*protocol.FetchResponses, error) {
	if p.msgID >= p.totalMessages {
		return &protocol.FetchResponses{}, nil
	}
	mockMessages := [][]byte{
		[]byte("msg " + strconv.Itoa(p.msgID)),
		[]byte("msg " + strconv.Itoa(p.msgID+1)),
	}
	response := &protocol.FetchResponses{
		Responses: []*protocol.FetchResponse{
			&protocol.FetchResponse{
				Topic: fetchRequest.Topics[0].Topic,
				PartitionResponses: []*protocol.FetchPartitionResponse{
					&protocol.FetchPartitionResponse{
						RecordSet: mockMessages[0],
					},
					&protocol.FetchPartitionResponse{
						RecordSet: mockMessages[1],
					},
				},
			},
		},
	}
	p.mockedMessages = append(p.mockedMessages, mockMessages...)
	p.msgID += 2
	return response, nil
}

func (p *MockProxy) CreateTopic(clientID string, createRequest *protocol.CreateTopicRequest) (*protocol.CreateTopicsResponse, error) {
	return nil, nil
}
