package broker

import (
	"bytes"
	"context"
	"io"
	"os"
	"reflect"
	"testing"

	"github.com/davecgh/go-spew/spew"
	"github.com/pkg/errors"

	"github.com/travisjeffery/jocko"
	"github.com/travisjeffery/jocko/log"
	"github.com/travisjeffery/jocko/mock"
	"github.com/travisjeffery/jocko/protocol"
)

func TestNew(t *testing.T) {
	tests := []struct {
		name      string
		fields    fields
		setFields func(f *fields)
		wantErr   bool
	}{
		{
			name: "broker ok",
		},
		{
			name:    "no logger error",
			wantErr: true,
			setFields: func(f *fields) {
				f.logger = nil
			},
		},
		{
			name:    "no broker addr error",
			wantErr: true,
			setFields: func(f *fields) {
				f.brokerAddr = ""
			},
		},
		{
			name:    "no raft addr error",
			wantErr: true,
			setFields: func(f *fields) {
				f.raft = &mock.Raft{
					AddrFunc: func() string {
						return ""
					},
				}
			},
		},
		{
			name:    "serf bootstrap error",
			wantErr: true,
			setFields: func(f *fields) {
				f.serf = &mock.Serf{
					BootstrapFunc: func(n *jocko.ClusterMember, rCh chan<- *jocko.ClusterMember) error {
						return errors.New("mock serf bootstrap error")
					},
				}
			},
		},
		{
			name:    "raft bootstrap error",
			wantErr: true,
			setFields: func(f *fields) {
				f.raft = &mock.Raft{
					AddrFunc: f.raft.AddrFunc,
					BootstrapFunc: func(s jocko.Serf, sCh <-chan *jocko.ClusterMember, cCh chan<- jocko.RaftCommand, controllerCh chan<- struct{}, removedCh chan<- int32) error {
						return errors.New("mock raft bootstrap error")
					},
				}
			},
		},
	}
	for _, tt := range tests {
		os.RemoveAll("/tmp/jocko")

		t.Run(tt.name, func(t *testing.T) {
			tt.fields = newFields()
			if tt.setFields != nil {
				tt.setFields(&tt.fields)
			}
			want := &Broker{
				logger:       tt.fields.logger,
				id:           tt.fields.id,
				topicMap:     tt.fields.topicMap,
				replicators:  tt.fields.replicators,
				brokerAddr:   tt.fields.brokerAddr,
				logDir:       tt.fields.logDir,
				raftCommands: tt.fields.raftCommands,
				raft:         tt.fields.raft,
				serf:         tt.fields.serf,
				shutdownCh:   tt.fields.shutdownCh,
				shutdown:     tt.fields.shutdown,
			}

			got, err := New(tt.fields.id, Addr(tt.fields.brokerAddr), Serf(tt.fields.serf), Raft(tt.fields.raft), Logger(tt.fields.logger), RaftCommands(tt.fields.raftCommands), LogDir(tt.fields.logDir))

			if err != nil && tt.wantErr {
				return
			} else if (err != nil) != tt.wantErr {
				t.Errorf("New() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.fields.serf.BootstrapCalled() {
				t.Error("expected serf bootstrap invoked; did not")
			}
			if !tt.fields.raft.BootstrapCalled() {
				t.Error("expected raft bootstrap invoked; did not")
			}
			if got != nil && got.shutdownCh == nil {
				t.Errorf("got.shutdownCh is nil")
			} else if got != nil {
				want.shutdownCh = got.shutdownCh
			}
		})
	}
}

func TestBroker_Run(t *testing.T) {
	mustEncode := func(e protocol.Encoder) []byte {
		var b []byte
		var err error
		if b, err = protocol.Encode(e); err != nil {
			panic(err)
		}
		return b
	}
	type args struct {
		ctx        context.Context
		requestCh  chan jocko.Request
		responseCh chan jocko.Response
		requests   []jocko.Request
		responses  []jocko.Response
	}
	tests := []struct {
		name      string
		fields    fields
		setFields func(f *fields)
		args      args
	}{
		{
			name:   "api versions",
			fields: newFields(),
			args: args{
				requestCh:  make(chan jocko.Request, 2),
				responseCh: make(chan jocko.Response, 2),
				requests: []jocko.Request{{
					Header:  &protocol.RequestHeader{CorrelationID: 1},
					Request: &protocol.APIVersionsRequest{},
				}},
				responses: []jocko.Response{{
					Header:   &protocol.RequestHeader{CorrelationID: 1},
					Response: &protocol.Response{CorrelationID: 1, Body: (&Broker{}).handleAPIVersions(nil, nil)},
				}},
			},
		},
		{
			name:   "create topic ok",
			fields: newFields(),
			args: args{
				requestCh:  make(chan jocko.Request, 2),
				responseCh: make(chan jocko.Response, 2),
				requests: []jocko.Request{{
					Header: &protocol.RequestHeader{CorrelationID: 1},
					Request: &protocol.CreateTopicRequests{Requests: []*protocol.CreateTopicRequest{{
						Topic:             "the-topic",
						NumPartitions:     1,
						ReplicationFactor: 1,
					}}}},
				},
				responses: []jocko.Response{{
					Header: &protocol.RequestHeader{CorrelationID: 1},
					Response: &protocol.Response{CorrelationID: 1, Body: &protocol.CreateTopicsResponse{
						TopicErrorCodes: []*protocol.TopicErrorCode{{Topic: "the-topic", ErrorCode: protocol.ErrNone.Code()}},
					}},
				}},
			},
		},
		{
			name:   "create topic invalid replication factor error",
			fields: newFields(),
			args: args{
				requestCh:  make(chan jocko.Request, 2),
				responseCh: make(chan jocko.Response, 2),
				requests: []jocko.Request{{
					Header: &protocol.RequestHeader{CorrelationID: 1},
					Request: &protocol.CreateTopicRequests{Requests: []*protocol.CreateTopicRequest{{
						Topic:             "the-topic",
						NumPartitions:     1,
						ReplicationFactor: 2,
					}}}},
				},
				responses: []jocko.Response{{
					Header: &protocol.RequestHeader{CorrelationID: 1},
					Response: &protocol.Response{CorrelationID: 1, Body: &protocol.CreateTopicsResponse{
						TopicErrorCodes: []*protocol.TopicErrorCode{{Topic: "the-topic", ErrorCode: protocol.ErrInvalidReplicationFactor.Code()}},
					}},
				}},
			},
		},
		{
			name:   "delete topic",
			fields: newFields(),
			args: args{
				requestCh:  make(chan jocko.Request, 2),
				responseCh: make(chan jocko.Response, 2),
				requests: []jocko.Request{{
					Header: &protocol.RequestHeader{CorrelationID: 1},
					Request: &protocol.CreateTopicRequests{Requests: []*protocol.CreateTopicRequest{{
						Topic:             "the-topic",
						NumPartitions:     1,
						ReplicationFactor: 1,
					}}}}, {
					Header:  &protocol.RequestHeader{CorrelationID: 2},
					Request: &protocol.DeleteTopicsRequest{Topics: []string{"the-topic"}}},
				},
				responses: []jocko.Response{{
					Header: &protocol.RequestHeader{CorrelationID: 1},
					Response: &protocol.Response{CorrelationID: 1, Body: &protocol.CreateTopicsResponse{
						TopicErrorCodes: []*protocol.TopicErrorCode{{Topic: "the-topic", ErrorCode: protocol.ErrNone.Code()}},
					}},
				}, {
					Header: &protocol.RequestHeader{CorrelationID: 2},
					Response: &protocol.Response{CorrelationID: 2, Body: &protocol.DeleteTopicsResponse{
						TopicErrorCodes: []*protocol.TopicErrorCode{{Topic: "the-topic", ErrorCode: protocol.ErrNone.Code()}},
					}}}},
			},
		},
		{
			name:   "offsets",
			fields: newFields(),
			args: args{
				requestCh:  make(chan jocko.Request, 2),
				responseCh: make(chan jocko.Response, 2),
				requests: []jocko.Request{
					{
						Header: &protocol.RequestHeader{CorrelationID: 1},
						Request: &protocol.CreateTopicRequests{Requests: []*protocol.CreateTopicRequest{{
							Topic:             "the-topic",
							NumPartitions:     1,
							ReplicationFactor: 1,
						}}},
					},
					{
						Header: &protocol.RequestHeader{CorrelationID: 2},
						Request: &protocol.ProduceRequest{TopicData: []*protocol.TopicData{{
							Topic: "the-topic",
							Data: []*protocol.Data{{
								RecordSet: mustEncode(&protocol.MessageSet{Offset: 0, Messages: []*protocol.Message{{Value: []byte("The message.")}}})}}}}},
					},
					{
						Header:  &protocol.RequestHeader{CorrelationID: 3},
						Request: &protocol.OffsetsRequest{ReplicaID: 0, Topics: []*protocol.OffsetsTopic{{Topic: "the-topic", Partitions: []*protocol.OffsetsPartition{{Partition: 0, Timestamp: -1}}}}},
					},
					{
						Header:  &protocol.RequestHeader{CorrelationID: 4},
						Request: &protocol.OffsetsRequest{ReplicaID: 0, Topics: []*protocol.OffsetsTopic{{Topic: "the-topic", Partitions: []*protocol.OffsetsPartition{{Partition: 0, Timestamp: -2}}}}},
					},
				},
				responses: []jocko.Response{
					{
						Header: &protocol.RequestHeader{CorrelationID: 1},
						Response: &protocol.Response{CorrelationID: 1, Body: &protocol.CreateTopicsResponse{
							TopicErrorCodes: []*protocol.TopicErrorCode{{Topic: "the-topic", ErrorCode: protocol.ErrNone.Code()}},
						}},
					},
					{
						Header: &protocol.RequestHeader{CorrelationID: 2},
						Response: &protocol.Response{CorrelationID: 2, Body: &protocol.ProduceResponses{
							Responses: []*protocol.ProduceResponse{{
								Topic:              "the-topic",
								PartitionResponses: []*protocol.ProducePartitionResponse{{Partition: 0, BaseOffset: 0, ErrorCode: protocol.ErrNone.Code()}},
							}},
						}},
					},
					{
						Header: &protocol.RequestHeader{CorrelationID: 3},
						Response: &protocol.Response{CorrelationID: 3, Body: &protocol.OffsetsResponse{
							Responses: []*protocol.OffsetResponse{{
								Topic:              "the-topic",
								PartitionResponses: []*protocol.PartitionResponse{{Partition: 0, Offsets: []int64{1}, ErrorCode: protocol.ErrNone.Code()}},
							}},
						}},
					},
					{
						Header: &protocol.RequestHeader{CorrelationID: 4},
						Response: &protocol.Response{CorrelationID: 4, Body: &protocol.OffsetsResponse{
							Responses: []*protocol.OffsetResponse{{
								Topic:              "the-topic",
								PartitionResponses: []*protocol.PartitionResponse{{Partition: 0, Offsets: []int64{0}, ErrorCode: protocol.ErrNone.Code()}},
							}},
						}},
					},
				},
			},
		},
		{
			name:   "fetch",
			fields: newFields(),
			args: args{
				requestCh:  make(chan jocko.Request, 2),
				responseCh: make(chan jocko.Response, 2),
				requests: []jocko.Request{
					{
						Header: &protocol.RequestHeader{CorrelationID: 1},
						Request: &protocol.CreateTopicRequests{Requests: []*protocol.CreateTopicRequest{{
							Topic:             "the-topic",
							NumPartitions:     1,
							ReplicationFactor: 1,
						}}},
					},
					{
						Header: &protocol.RequestHeader{CorrelationID: 2},
						Request: &protocol.ProduceRequest{TopicData: []*protocol.TopicData{{
							Topic: "the-topic",
							Data: []*protocol.Data{{
								RecordSet: mustEncode(&protocol.MessageSet{Offset: 0, Messages: []*protocol.Message{{Value: []byte("The message.")}}})}}}}},
					},
					{
						Header:  &protocol.RequestHeader{CorrelationID: 3},
						Request: &protocol.FetchRequest{ReplicaID: 1, MinBytes: 5, Topics: []*protocol.FetchTopic{{Topic: "the-topic", Partitions: []*protocol.FetchPartition{{Partition: 0, FetchOffset: 0, MaxBytes: 100}}}}},
					},
				},
				responses: []jocko.Response{
					{
						Header: &protocol.RequestHeader{CorrelationID: 1},
						Response: &protocol.Response{CorrelationID: 1, Body: &protocol.CreateTopicsResponse{
							TopicErrorCodes: []*protocol.TopicErrorCode{{Topic: "the-topic", ErrorCode: protocol.ErrNone.Code()}},
						}},
					},
					{
						Header: &protocol.RequestHeader{CorrelationID: 2},
						Response: &protocol.Response{CorrelationID: 2, Body: &protocol.ProduceResponses{
							Responses: []*protocol.ProduceResponse{
								{
									Topic:              "the-topic",
									PartitionResponses: []*protocol.ProducePartitionResponse{{Partition: 0, BaseOffset: 0, ErrorCode: protocol.ErrNone.Code()}},
								},
							},
						}},
					},
					{
						Header: &protocol.RequestHeader{CorrelationID: 3},
						Response: &protocol.Response{CorrelationID: 3, Body: &protocol.FetchResponses{
							Responses: []*protocol.FetchResponse{{
								Topic: "the-topic",
								PartitionResponses: []*protocol.FetchPartitionResponse{{
									Partition:     0,
									ErrorCode:     protocol.ErrNone.Code(),
									HighWatermark: 1,
									RecordSet:     mustEncode(&protocol.MessageSet{Offset: 0, Messages: []*protocol.Message{{Value: []byte("The message.")}}}),
								}},
							}}},
						},
					},
				},
			},
		},
		{
			name:   "metadata",
			fields: newFields(),
			args: args{
				requestCh:  make(chan jocko.Request, 2),
				responseCh: make(chan jocko.Response, 2),
				requests: []jocko.Request{
					{
						Header: &protocol.RequestHeader{CorrelationID: 1},
						Request: &protocol.CreateTopicRequests{Requests: []*protocol.CreateTopicRequest{{
							Topic:             "the-topic",
							NumPartitions:     1,
							ReplicationFactor: 1,
						}}},
					},
					{
						Header: &protocol.RequestHeader{CorrelationID: 2},
						Request: &protocol.ProduceRequest{TopicData: []*protocol.TopicData{{
							Topic: "the-topic",
							Data: []*protocol.Data{{
								RecordSet: mustEncode(&protocol.MessageSet{Offset: 0, Messages: []*protocol.Message{{Value: []byte("The message.")}}})}}}}},
					},
					{
						Header:  &protocol.RequestHeader{CorrelationID: 3},
						Request: &protocol.MetadataRequest{Topics: []string{"the-topic", "unknown-topic"}},
					},
				},
				responses: []jocko.Response{
					{
						Header: &protocol.RequestHeader{CorrelationID: 1},
						Response: &protocol.Response{CorrelationID: 1, Body: &protocol.CreateTopicsResponse{
							TopicErrorCodes: []*protocol.TopicErrorCode{{Topic: "the-topic", ErrorCode: protocol.ErrNone.Code()}},
						}},
					},
					{
						Header: &protocol.RequestHeader{CorrelationID: 2},
						Response: &protocol.Response{CorrelationID: 2, Body: &protocol.ProduceResponses{
							Responses: []*protocol.ProduceResponse{
								{
									Topic:              "the-topic",
									PartitionResponses: []*protocol.ProducePartitionResponse{{Partition: 0, BaseOffset: 0, ErrorCode: protocol.ErrNone.Code()}},
								},
							},
						}},
					},
					{
						Header: &protocol.RequestHeader{CorrelationID: 3},
						Response: &protocol.Response{CorrelationID: 3, Body: &protocol.MetadataResponse{
							Brokers: []*protocol.Broker{{NodeID: 1, Host: "localhost", Port: 9092}},
							TopicMetadata: []*protocol.TopicMetadata{
								{Topic: "the-topic", TopicErrorCode: protocol.ErrNone.Code(), PartitionMetadata: []*protocol.PartitionMetadata{{PartitionErrorCode: protocol.ErrNone.Code(), ParititionID: 0, Leader: 1, Replicas: []int32{1}, ISR: []int32{1}}}},
								{Topic: "unknown-topic", TopicErrorCode: protocol.ErrUnknownTopicOrPartition.Code()},
							},
						}},
					},
				},
			},
		},
		{
			name:   "produce topic/partition doesn't exist error",
			fields: newFields(),
			args: args{
				requestCh:  make(chan jocko.Request, 2),
				responseCh: make(chan jocko.Response, 2),
				requests: []jocko.Request{{
					Header: &protocol.RequestHeader{CorrelationID: 2},
					Request: &protocol.ProduceRequest{TopicData: []*protocol.TopicData{{
						Topic: "another-topic",
						Data: []*protocol.Data{{
							RecordSet: mustEncode(&protocol.MessageSet{Offset: 1, Messages: []*protocol.Message{{Value: []byte("The message.")}}})}}}}}},
				},
				responses: []jocko.Response{{
					Header: &protocol.RequestHeader{CorrelationID: 2},
					Response: &protocol.Response{CorrelationID: 2, Body: &protocol.ProduceResponses{
						Responses: []*protocol.ProduceResponse{{
							Topic:              "another-topic",
							PartitionResponses: []*protocol.ProducePartitionResponse{{Partition: 0, ErrorCode: protocol.ErrUnknownTopicOrPartition.Code()}},
						}},
					}}}},
			},
		},
		{
			name:   "leader and isr leader new partition",
			fields: newFields(),
			args: args{
				requestCh:  make(chan jocko.Request, 2),
				responseCh: make(chan jocko.Response, 2),
				requests: []jocko.Request{{
					Header: &protocol.RequestHeader{CorrelationID: 2},
					Request: &protocol.LeaderAndISRRequest{
						PartitionStates: []*protocol.PartitionState{
							{
								Topic:     "the-topic",
								Partition: 1,
								ISR:       []int32{1},
								Replicas:  []int32{1},
								Leader:    1,
								ZKVersion: 1,
							},
						},
					}},
				},
				responses: []jocko.Response{{
					Header: &protocol.RequestHeader{CorrelationID: 2},
					Response: &protocol.Response{CorrelationID: 2, Body: &protocol.LeaderAndISRResponse{
						Partitions: []*protocol.LeaderAndISRPartition{
							{
								ErrorCode: protocol.ErrNone.Code(),
								Partition: 1,
								Topic:     "the-topic",
							},
						},
					}}}},
			},
		},
		{
			name:   "leader and isr leader become leader",
			fields: newFields(),
			setFields: func(f *fields) {
				f.topicMap = map[string][]*Partition{
					"the-topic": []*Partition{{
						Topic:                   "the-topic",
						ID:                      1,
						Replicas:                nil,
						ISR:                     nil,
						Leader:                  0,
						PreferredLeader:         0,
						LeaderAndISRVersionInZK: 0,
					}},
				}
			},
			args: args{
				requestCh:  make(chan jocko.Request, 2),
				responseCh: make(chan jocko.Response, 2),
				requests: []jocko.Request{{
					Header: &protocol.RequestHeader{CorrelationID: 2},
					Request: &protocol.LeaderAndISRRequest{
						PartitionStates: []*protocol.PartitionState{
							{
								Topic:     "the-topic",
								Partition: 1,
								ISR:       []int32{1},
								Replicas:  []int32{1},
								Leader:    1,
								ZKVersion: 1,
							},
						},
					}},
				},
				responses: []jocko.Response{{
					Header: &protocol.RequestHeader{CorrelationID: 2},
					Response: &protocol.Response{CorrelationID: 2, Body: &protocol.LeaderAndISRResponse{
						Partitions: []*protocol.LeaderAndISRPartition{
							{
								ErrorCode: protocol.ErrNone.Code(),
								Partition: 1,
								Topic:     "the-topic",
							},
						},
					}}}},
			},
		},
		{
			name:   "leader and isr leader become follower",
			fields: newFields(),
			setFields: func(f *fields) {
				f.topicMap = map[string][]*Partition{
					"the-topic": []*Partition{{
						Topic:                   "the-topic",
						ID:                      1,
						Replicas:                nil,
						ISR:                     nil,
						Leader:                  1,
						PreferredLeader:         1,
						LeaderAndISRVersionInZK: 0,
					}},
				}
			},
			args: args{
				requestCh:  make(chan jocko.Request, 2),
				responseCh: make(chan jocko.Response, 2),
				requests: []jocko.Request{{
					Header: &protocol.RequestHeader{CorrelationID: 2},
					Request: &protocol.LeaderAndISRRequest{
						PartitionStates: []*protocol.PartitionState{
							{
								Topic:     "the-topic",
								Partition: 1,
								ISR:       []int32{1},
								Replicas:  []int32{1},
								Leader:    0,
								ZKVersion: 1,
							},
						},
					}},
				},
				responses: []jocko.Response{{
					Header: &protocol.RequestHeader{CorrelationID: 2},
					Response: &protocol.Response{CorrelationID: 2, Body: &protocol.LeaderAndISRResponse{
						Partitions: []*protocol.LeaderAndISRPartition{
							{
								ErrorCode: protocol.ErrNone.Code(),
								Partition: 1,
								Topic:     "the-topic",
							},
						},
					}}}},
			},
		},
	}
	for _, tt := range tests {
		os.RemoveAll("/tmp/jocko")
		t.Run(tt.name, func(t *testing.T) {
			if tt.setFields != nil {
				tt.setFields(&tt.fields)
			}
			b := &Broker{
				logger:       tt.fields.logger,
				id:           tt.fields.id,
				loner:        tt.fields.loner,
				topicMap:     tt.fields.topicMap,
				replicators:  tt.fields.replicators,
				brokerAddr:   tt.fields.brokerAddr,
				logDir:       tt.fields.logDir,
				raft:         tt.fields.raft,
				serf:         tt.fields.serf,
				raftCommands: tt.fields.raftCommands,
				shutdownCh:   tt.fields.shutdownCh,
				shutdown:     tt.fields.shutdown,
			}
			if tt.fields.topicMap != nil {
				for _, ps := range tt.fields.topicMap {
					for _, p := range ps {
						b.startReplica(p)
					}
				}
			}
			ctx, cancel := context.WithCancel(context.Background())
			go b.Run(ctx, tt.args.requestCh, tt.args.responseCh)

			for i := 0; i < len(tt.args.requests); i++ {
				tt.args.requestCh <- tt.args.requests[i]
				response := <-tt.args.responseCh

				switch res := response.Response.(*protocol.Response).Body.(type) {
				// handle timestamp explicitly since we don't know what
				// it'll be set to
				case *protocol.ProduceResponses:
					for _, response := range res.Responses {
						for _, pr := range response.PartitionResponses {
							if pr.ErrorCode != protocol.ErrNone.Code() {
								break
							}
							if pr.Timestamp == 0 {
								t.Error("expected timestamp not to be 0")
							}
							pr.Timestamp = 0
						}
					}
				}

				if !reflect.DeepEqual(response.Response, tt.args.responses[i].Response) {
					t.Errorf("got %s, want: %s", spewstr(response.Response), spewstr(tt.args.responses[i].Response))
				}

			}
			cancel()
		})
	}
}

func spewstr(v interface{}) string {
	var buf bytes.Buffer
	spew.Fdump(&buf, v)
	return buf.String()
}

func TestBroker_Join(t *testing.T) {
	type args struct {
		addrs []string
	}
	err := errors.New("mock serf join error")
	tests := []struct {
		name      string
		fields    fields
		setFields func(f *fields)
		args      args
		want      protocol.Error
	}{
		{
			name:   "ok",
			fields: newFields(),
			args:   args{addrs: []string{"localhost:9082"}},
			want:   protocol.ErrNone,
		},
		{
			name:   "serf errr",
			fields: newFields(),
			setFields: func(f *fields) {
				f.serf.JoinFunc = func(addrs ...string) (int, error) {
					return -1, err
				}
			},
			args: args{addrs: []string{"localhost:9082"}},
			want: protocol.ErrUnknown.WithErr(err),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.setFields != nil {
				tt.setFields(&tt.fields)
			}
			b := &Broker{
				logger:      tt.fields.logger,
				id:          tt.fields.id,
				topicMap:    tt.fields.topicMap,
				replicators: tt.fields.replicators,
				brokerAddr:  tt.fields.brokerAddr,
				logDir:      tt.fields.logDir,
				raft:        tt.fields.raft,
				serf:        tt.fields.serf,
				shutdownCh:  tt.fields.shutdownCh,
				shutdown:    tt.fields.shutdown,
			}
			if got := b.Join(tt.args.addrs...); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Broker.Join() = %v, want %v", got, tt.want)
			}
			if !tt.fields.serf.JoinCalled() {
				t.Error("expected serf join invoked; did not")
			}
		})
	}
}

func TestBroker_clusterMembers(t *testing.T) {
	type fields struct {
		logger      log.Logger
		id          int32
		topicMap    map[string][]*Partition
		replicators map[*Partition]*Replicator
		brokerAddr  string
		logDir      string
		raft        jocko.Raft
		serf        jocko.Serf
		shutdownCh  chan struct{}
		shutdown    bool
	}
	members := []*jocko.ClusterMember{{ID: 1}}
	tests := []struct {
		name   string
		fields fields
		want   []*jocko.ClusterMember
	}{
		{
			name: "found members ok",
			fields: fields{
				serf: &mock.Serf{
					ClusterFunc: func() []*jocko.ClusterMember {
						return members
					},
				},
			},
			want: members,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b := &Broker{
				logger:      tt.fields.logger,
				id:          tt.fields.id,
				topicMap:    tt.fields.topicMap,
				replicators: tt.fields.replicators,
				brokerAddr:  tt.fields.brokerAddr,
				logDir:      tt.fields.logDir,
				raft:        tt.fields.raft,
				serf:        tt.fields.serf,
				shutdownCh:  tt.fields.shutdownCh,
				shutdown:    tt.fields.shutdown,
			}
			if got := b.clusterMembers(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Broker.clusterMembers() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestBroker_isController(t *testing.T) {
	type fields struct {
		logger      log.Logger
		id          int32
		topicMap    map[string][]*Partition
		replicators map[*Partition]*Replicator
		brokerAddr  string
		logDir      string
		raft        jocko.Raft
		serf        jocko.Serf
		shutdownCh  chan struct{}
		shutdown    bool
	}
	tests := []struct {
		name   string
		fields fields
		want   bool
	}{
		{
			name: "is leader",
			fields: fields{
				raft: &mock.Raft{
					IsLeaderFunc: func() bool {
						return true
					},
					LeaderIDFunc: func() string {
						return "localhost:9093"
					},
				},
			},
			want: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b := &Broker{
				logger:      tt.fields.logger,
				id:          tt.fields.id,
				topicMap:    tt.fields.topicMap,
				replicators: tt.fields.replicators,
				brokerAddr:  tt.fields.brokerAddr,
				logDir:      tt.fields.logDir,
				raft:        tt.fields.raft,
				serf:        tt.fields.serf,
				shutdownCh:  tt.fields.shutdownCh,
				shutdown:    tt.fields.shutdown,
			}
			if got := b.isController(); got != tt.want {
				t.Errorf("Broker.isController() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestBroker_topicPartitions(t *testing.T) {
	type fields struct {
		logger      log.Logger
		id          int32
		topicMap    map[string][]*Partition
		replicators map[*Partition]*Replicator
		brokerAddr  string
		logDir      string
		raft        jocko.Raft
		serf        jocko.Serf
		shutdownCh  chan struct{}
		shutdown    bool
	}
	type args struct {
		topic string
	}
	tests := []struct {
		name      string
		fields    fields
		args      args
		wantFound []*Partition
		wantErr   protocol.Error
	}{
		{
			name: "partitions found",
			fields: fields{
				topicMap: map[string][]*Partition{"topic": []*Partition{{ID: 1}}},
			},
			args:      args{topic: "topic"},
			wantFound: []*Partition{{ID: 1}},
			wantErr:   protocol.ErrNone,
		},
		{
			name: "partitions not found",
			fields: fields{
				topicMap: map[string][]*Partition{"topic": []*Partition{{ID: 1}}},
			},
			args:      args{topic: "not_topic"},
			wantFound: nil,
			wantErr:   protocol.ErrUnknownTopicOrPartition,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b := &Broker{
				logger:      tt.fields.logger,
				id:          tt.fields.id,
				topicMap:    tt.fields.topicMap,
				replicators: tt.fields.replicators,
				brokerAddr:  tt.fields.brokerAddr,
				logDir:      tt.fields.logDir,
				raft:        tt.fields.raft,
				serf:        tt.fields.serf,
				shutdownCh:  tt.fields.shutdownCh,
				shutdown:    tt.fields.shutdown,
			}
			gotFound, gotErr := b.topicPartitions(tt.args.topic)
			if !reflect.DeepEqual(gotFound, tt.wantFound) {
				t.Errorf("Broker.topicPartitions() gotFound = %v, want %v", gotFound, tt.wantFound)
			}
			if !reflect.DeepEqual(gotErr, tt.wantErr) {
				t.Errorf("Broker.topicPartitions() gotErr = %v, want %v", gotErr, tt.wantErr)
			}
		})
	}
}

func TestBroker_topics(t *testing.T) {
	type fields struct {
		logger      log.Logger
		id          int32
		topicMap    map[string][]*Partition
		replicators map[*Partition]*Replicator
		brokerAddr  string
		logDir      string
		raft        jocko.Raft
		serf        jocko.Serf
		shutdownCh  chan struct{}
		shutdown    bool
	}
	topicMap := map[string][]*Partition{
		"topic": []*Partition{{ID: 1}},
	}
	tests := []struct {
		name   string
		fields fields
		want   map[string][]*Partition
	}{
		{
			name: "topic map returned",
			fields: fields{
				topicMap: topicMap},
			want: topicMap,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b := &Broker{
				logger:      tt.fields.logger,
				id:          tt.fields.id,
				topicMap:    tt.fields.topicMap,
				replicators: tt.fields.replicators,
				brokerAddr:  tt.fields.brokerAddr,
				logDir:      tt.fields.logDir,
				raft:        tt.fields.raft,
				serf:        tt.fields.serf,
				shutdownCh:  tt.fields.shutdownCh,
				shutdown:    tt.fields.shutdown,
			}
			if got := b.topics(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Broker.topics() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestBroker_partition(t *testing.T) {
	f := newFields()
	f.topicMap = map[string][]*Partition{
		"the-topic":   []*Partition{{ID: 1}},
		"empty-topic": []*Partition{},
	}
	type args struct {
		topic     string
		partition int32
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    *Partition
		wanterr protocol.Error
	}{
		{
			name:   "found partitions",
			fields: f,
			args: args{
				topic:     "the-topic",
				partition: 1,
			},
			want:    f.topicMap["the-topic"][0],
			wanterr: protocol.ErrNone,
		},
		{
			name:   "no partitions",
			fields: f,
			args: args{
				topic:     "not-the-topic",
				partition: 1,
			},
			want:    nil,
			wanterr: protocol.ErrUnknownTopicOrPartition,
		},
		{
			name:   "empty partitions",
			fields: f,
			args: args{
				topic:     "empty-topic",
				partition: 1,
			},
			want:    nil,
			wanterr: protocol.ErrUnknownTopicOrPartition,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b := &Broker{
				logger:      tt.fields.logger,
				id:          tt.fields.id,
				topicMap:    tt.fields.topicMap,
				replicators: tt.fields.replicators,
				brokerAddr:  tt.fields.brokerAddr,
				logDir:      tt.fields.logDir,
				raft:        tt.fields.raft,
				serf:        tt.fields.serf,
				shutdownCh:  tt.fields.shutdownCh,
				shutdown:    tt.fields.shutdown,
			}
			got, goterr := b.partition(tt.args.topic, tt.args.partition)
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Broker.partition() got = %v, want %v", got, tt.want)
			}
			if !reflect.DeepEqual(goterr, tt.wanterr) {
				t.Errorf("Broker.partition() goterr = %v, want %v", goterr, tt.wanterr)
			}
		})
	}
}

func TestBroker_createPartition(t *testing.T) {
	type fields struct {
		logger      log.Logger
		id          int32
		topicMap    map[string][]*Partition
		replicators map[*Partition]*Replicator
		brokerAddr  string
		logDir      string
		raft        jocko.Raft
		serf        jocko.Serf
		shutdownCh  chan struct{}
		shutdown    bool
	}
	type args struct {
		partition *Partition
	}
	raft := &mock.Raft{
		LeaderIDFunc: func() string {
			return "localhost:9093"
		},
		ApplyFunc: func(c jocko.RaftCommand) error {
			if c.Cmd != createPartition {
				t.Errorf("Broker.createPartition() c.Cmd = %v, want %v", c.Cmd, createPartition)
			}
			return nil
		},
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr bool
	}{
		{
			name: "called apply",
			fields: fields{
				raft: raft,
			},
			args:    args{partition: &Partition{ID: 1}},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b := &Broker{
				logger:      tt.fields.logger,
				id:          tt.fields.id,
				topicMap:    tt.fields.topicMap,
				replicators: tt.fields.replicators,
				brokerAddr:  tt.fields.brokerAddr,
				logDir:      tt.fields.logDir,
				raft:        tt.fields.raft,
				serf:        tt.fields.serf,
				shutdownCh:  tt.fields.shutdownCh,
				shutdown:    tt.fields.shutdown,
			}
			if err := b.createPartition(tt.args.partition); (err != nil) != tt.wantErr {
				t.Errorf("Broker.createPartition() error = %v, wantErr %v", err, tt.wantErr)
			}
			if !raft.ApplyCalled() {
				t.Errorf("Broker.createPartition() raft.ApplyCalled() = %v, want %v", raft.ApplyCalled(), true)
			}
		})
	}
}

func TestBroker_clusterMember(t *testing.T) {
	type fields struct {
		logger      log.Logger
		id          int32
		topicMap    map[string][]*Partition
		replicators map[*Partition]*Replicator
		brokerAddr  string
		logDir      string
		raft        jocko.Raft
		serf        jocko.Serf
		shutdownCh  chan struct{}
		shutdown    bool
	}
	member := &jocko.ClusterMember{ID: 1}
	serf := &mock.Serf{
		MemberFunc: func(id int32) *jocko.ClusterMember {
			if id != member.ID {
				t.Errorf("serf.Member() id = %v, want %v", id, member.ID)
			}
			return member
		},
	}
	type args struct {
		id int32
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   *jocko.ClusterMember
	}{
		{
			name: "found member",
			fields: fields{
				serf: serf,
			},
			args: args{id: 1},
			want: member,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b := &Broker{
				logger:      tt.fields.logger,
				id:          tt.fields.id,
				topicMap:    tt.fields.topicMap,
				replicators: tt.fields.replicators,
				brokerAddr:  tt.fields.brokerAddr,
				logDir:      tt.fields.logDir,
				raft:        tt.fields.raft,
				serf:        tt.fields.serf,
				shutdownCh:  tt.fields.shutdownCh,
				shutdown:    tt.fields.shutdown,
			}
			if got := b.clusterMember(tt.args.id); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Broker.clusterMember() = %v, want %v", got, tt.want)
			}
			if !serf.MemberCalled() {
				t.Errorf("serf.MemberCalled() = %v, want %v", serf.MemberCalled(), true)
			}
		})
	}
}

func TestBroker_startReplica(t *testing.T) {
	type args struct {
		partition *Partition
	}
	partition := &Partition{
		Topic:  "the-topic",
		ID:     1,
		Leader: 1,
	}
	tests := []struct {
		name      string
		setFields func(f *fields)
		args      args
		want      protocol.Error
	}{
		{
			name: "started replica as leader",
			args: args{
				partition: partition,
			},
			want: protocol.ErrNone,
		},
		{
			name: "started replica as follower",
			args: args{
				partition: &Partition{
					ID:       1,
					Topic:    "replica-topic",
					Replicas: []int32{1},
					Leader:   2,
				},
			},
			want: protocol.ErrNone,
		},
		{
			name: "started replica with existing topic",
			setFields: func(f *fields) {
				f.topicMap["existing-topic"] = []*Partition{
					{
						ID:    1,
						Topic: "existing-topic",
					},
				}
			},
			args: args{
				partition: &Partition{ID: 2, Topic: "existing-topic"},
			},
			want: protocol.ErrNone,
		},
		// TODO: Possible bug. If a duplicate partition is added,
		//   the partition will be appended to the partitions as a duplicate.
		// {
		// 	name:   "started replica with dupe partition",
		// 	fields: f,
		// 	args: args{
		// 		partition: &Partition{ID: 1, Topic: "existing-topic"},
		// 	},
		// 	want: protocol.ErrNone,
		// },
		{
			name: "started replica with commitlog error",
			setFields: func(f *fields) {
				f.logDir = ""
			},
			args: args{
				partition: &Partition{Leader: 1},
			},
			want: protocol.ErrUnknown.WithErr(errors.New("mkdir failed: mkdir /0: permission denied")),
		},
	}
	for _, tt := range tests {
		fields := newFields()
		if tt.setFields != nil {
			tt.setFields(&fields)
		}
		t.Run(tt.name, func(t *testing.T) {
			b := &Broker{
				logger:      fields.logger,
				id:          fields.id,
				topicMap:    fields.topicMap,
				replicators: fields.replicators,
				brokerAddr:  fields.brokerAddr,
				logDir:      fields.logDir,
				raft:        fields.raft,
				serf:        fields.serf,
				shutdownCh:  fields.shutdownCh,
				shutdown:    fields.shutdown,
			}
			if got := b.startReplica(tt.args.partition); got.Error() != tt.want.Error() {
				t.Errorf("Broker.startReplica() = %v, want %v", got, tt.want)
			}
			got, err := b.partition(tt.args.partition.Topic, tt.args.partition.ID)
			if !reflect.DeepEqual(got, tt.args.partition) {
				t.Errorf("Broker.partition() = %v, want %v", got, partition)
			}
			parts := map[int32]*Partition{}
			for _, p := range b.topicMap[tt.args.partition.Topic] {
				if _, ok := parts[p.ID]; ok {
					t.Errorf("Broker.topicPartition contains dupes, dupe %v", p)
				}
				parts[p.ID] = p
			}
			if err != protocol.ErrNone {
				t.Errorf("Broker.partition() err = %v, want %v", err, protocol.ErrNone)
			}
		})
	}
}

func TestBroker_createTopic(t *testing.T) {
	type fields struct {
		logger      log.Logger
		id          int32
		topicMap    map[string][]*Partition
		replicators map[*Partition]*Replicator
		brokerAddr  string
		logDir      string
		raft        jocko.Raft
		serf        jocko.Serf
		shutdownCh  chan struct{}
		shutdown    bool
	}
	type args struct {
		topic             string
		partitions        int32
		replicationFactor int16
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   protocol.Error
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b := &Broker{
				logger:      tt.fields.logger,
				id:          tt.fields.id,
				topicMap:    tt.fields.topicMap,
				replicators: tt.fields.replicators,
				brokerAddr:  tt.fields.brokerAddr,
				logDir:      tt.fields.logDir,
				raft:        tt.fields.raft,
				serf:        tt.fields.serf,
				shutdownCh:  tt.fields.shutdownCh,
				shutdown:    tt.fields.shutdown,
			}
			if got := b.createTopic(tt.args.topic, tt.args.partitions, tt.args.replicationFactor); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Broker.createTopic() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestBroker_deleteTopic(t *testing.T) {
	type fields struct {
		logger      log.Logger
		id          int32
		topicMap    map[string][]*Partition
		replicators map[*Partition]*Replicator
		brokerAddr  string
		logDir      string
		raft        jocko.Raft
		serf        jocko.Serf
		shutdownCh  chan struct{}
		shutdown    bool
	}
	type args struct {
		topic string
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   protocol.Error
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b := &Broker{
				logger:      tt.fields.logger,
				id:          tt.fields.id,
				topicMap:    tt.fields.topicMap,
				replicators: tt.fields.replicators,
				brokerAddr:  tt.fields.brokerAddr,
				logDir:      tt.fields.logDir,
				raft:        tt.fields.raft,
				serf:        tt.fields.serf,
				shutdownCh:  tt.fields.shutdownCh,
				shutdown:    tt.fields.shutdown,
			}
			if got := b.deleteTopic(tt.args.topic); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Broker.deleteTopic() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestBroker_deletePartitions(t *testing.T) {
	type fields struct {
		logger      log.Logger
		id          int32
		topicMap    map[string][]*Partition
		replicators map[*Partition]*Replicator
		brokerAddr  string
		logDir      string
		raft        jocko.Raft
		serf        jocko.Serf
		shutdownCh  chan struct{}
		shutdown    bool
	}
	type args struct {
		tp *Partition
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b := &Broker{
				logger:      tt.fields.logger,
				id:          tt.fields.id,
				topicMap:    tt.fields.topicMap,
				replicators: tt.fields.replicators,
				brokerAddr:  tt.fields.brokerAddr,
				logDir:      tt.fields.logDir,
				raft:        tt.fields.raft,
				serf:        tt.fields.serf,
				shutdownCh:  tt.fields.shutdownCh,
				shutdown:    tt.fields.shutdown,
			}
			if err := b.deletePartitions(tt.args.tp); (err != nil) != tt.wantErr {
				t.Errorf("Broker.deletePartitions() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestBroker_Shutdown(t *testing.T) {
	tests := []struct {
		name    string
		fields  fields
		wantErr bool
	}{
		{
			name:    "shutdown ok",
			fields:  newFields(),
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b, err := New(tt.fields.id, Addr(tt.fields.brokerAddr), Serf(tt.fields.serf), Raft(tt.fields.raft), Logger(tt.fields.logger), RaftCommands(tt.fields.raftCommands), LogDir(tt.fields.logDir))
			if err != nil {
				t.Errorf("Broker.New() error = %v, wanted nil", err)
			}
			if err := b.Shutdown(); (err != nil) != tt.wantErr {
				t.Errorf("Broker.Shutdown() error = %v, wantErr %v", err, tt.wantErr)
			}
			if tt.fields.raft.ShutdownCalled() != true {
				t.Errorf("did not shutdown raft")
			}
			if tt.fields.serf.ShutdownCalled() != true {
				t.Errorf("did not shutdown raft")
			}
		})
	}
}

func TestBroker_becomeFollower(t *testing.T) {
	type fields struct {
		logger      log.Logger
		id          int32
		topicMap    map[string][]*Partition
		replicators map[*Partition]*Replicator
		brokerAddr  string
		logDir      string
		raft        jocko.Raft
		serf        jocko.Serf
		shutdownCh  chan struct{}
		shutdown    bool
	}
	type args struct {
		topic          string
		partitionID    int32
		partitionState *protocol.PartitionState
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   protocol.Error
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b := &Broker{
				logger:      tt.fields.logger,
				id:          tt.fields.id,
				topicMap:    tt.fields.topicMap,
				replicators: tt.fields.replicators,
				brokerAddr:  tt.fields.brokerAddr,
				logDir:      tt.fields.logDir,
				raft:        tt.fields.raft,
				serf:        tt.fields.serf,
				shutdownCh:  tt.fields.shutdownCh,
				shutdown:    tt.fields.shutdown,
			}
			if got := b.becomeFollower(tt.args.topic, tt.args.partitionID, tt.args.partitionState); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Broker.becomeFollower() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestBroker_becomeLeader(t *testing.T) {
	type fields struct {
		logger      log.Logger
		id          int32
		topicMap    map[string][]*Partition
		replicators map[*Partition]*Replicator
		brokerAddr  string
		logDir      string
		raft        jocko.Raft
		serf        jocko.Serf
		shutdownCh  chan struct{}
		shutdown    bool
	}
	type args struct {
		topic          string
		partitionID    int32
		partitionState *protocol.PartitionState
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   protocol.Error
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b := &Broker{
				logger:      tt.fields.logger,
				id:          tt.fields.id,
				topicMap:    tt.fields.topicMap,
				replicators: tt.fields.replicators,
				brokerAddr:  tt.fields.brokerAddr,
				logDir:      tt.fields.logDir,
				raft:        tt.fields.raft,
				serf:        tt.fields.serf,
				shutdownCh:  tt.fields.shutdownCh,
				shutdown:    tt.fields.shutdown,
			}
			if got := b.becomeLeader(tt.args.topic, tt.args.partitionID, tt.args.partitionState); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Broker.becomeLeader() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_contains(t *testing.T) {
	type args struct {
		rs []int32
		r  int32
	}
	tests := []struct {
		name string
		args args
		want bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := contains(tt.args.rs, tt.args.r); got != tt.want {
				t.Errorf("contains() = %v, want %v", got, tt.want)
			}
		})
	}
}

type fields struct {
	id           int32
	serf         *mock.Serf
	raft         *mock.Raft
	raftCommands chan jocko.RaftCommand
	logger       log.Logger
	topicMap     map[string][]*Partition
	replicators  map[*Partition]*Replicator
	brokerAddr   string
	loner        bool
	logDir       string
	shutdownCh   chan struct{}
	shutdown     bool
}

func newFields() fields {
	serf := &mock.Serf{
		BootstrapFunc: func(n *jocko.ClusterMember, rCh chan<- *jocko.ClusterMember) error {
			if n == nil {
				return errors.New("*jocko.ClusterMember is nil")
			}
			if rCh == nil {
				return errors.New("chan<- *jocko.ClusterMember is nil")
			}
			return nil
		},
		JoinFunc: func(addrs ...string) (int, error) {
			return 1, nil
		},
		ClusterFunc: func() []*jocko.ClusterMember {
			return []*jocko.ClusterMember{{ID: 1, Port: 9092, IP: "localhost"}}
		},
		MemberFunc: func(memberID int32) *jocko.ClusterMember {
			return &jocko.ClusterMember{ID: 1}
		},
		ShutdownFunc: func() error {
			return nil
		},
	}
	raft := &mock.Raft{
		AddrFunc: func() string {
			return "localhost:9093"
		},
		BootstrapFunc: func(s jocko.Serf, sCh <-chan *jocko.ClusterMember, cCh chan<- jocko.RaftCommand, controllerCh chan<- struct{}, removedCh chan<- int32) error {
			if s == nil {
				return errors.New("jocko.Serf is nil")
			}
			if sCh == nil {
				return errors.New("<-chan *jocko.ClusterMember is nil")
			}
			if cCh == nil {
				return errors.New("chan<- jocko.RaftCommand is nil")
			}
			return nil
		},
		IsLeaderFunc: func() bool {
			return true
		},
		LeaderIDFunc: func() string {
			return "localhost:9093"
		},
		ApplyFunc: func(jocko.RaftCommand) error {
			return nil
		},
		ShutdownFunc: func() error {
			return nil
		},
	}
	return fields{
		topicMap:     make(map[string][]*Partition),
		raftCommands: make(chan jocko.RaftCommand),
		replicators:  make(map[*Partition]*Replicator),
		logger:       log.New(),
		logDir:       "/tmp/jocko",
		loner:        true,
		serf:         serf,
		raft:         raft,
		brokerAddr:   "localhost:9092",
		id:           1,
	}
}

type nopReaderWriter struct{}

func (nopReaderWriter) Read(b []byte) (int, error)  { return 0, nil }
func (nopReaderWriter) Write(b []byte) (int, error) { return 0, nil }
func newNopReaderWriter() io.ReadWriter             { return nopReaderWriter{} }
