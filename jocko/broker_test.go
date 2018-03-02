package jocko

import (
	"bytes"
	"context"
	"fmt"
	"reflect"
	"testing"

	"github.com/davecgh/go-spew/spew"
	"github.com/hashicorp/consul/testutil/retry"
	"github.com/hashicorp/raft"
	opentracing "github.com/opentracing/opentracing-go"
	"github.com/stretchr/testify/require"

	"github.com/travisjeffery/jocko/jocko/config"
	"github.com/travisjeffery/jocko/jocko/structs"
	"github.com/travisjeffery/jocko/log"
	"github.com/travisjeffery/jocko/protocol"
)

func TestBroker_Run(t *testing.T) {
	// creating the config up here so we can set the nodeid in the expected test cases
	mustEncode := func(e protocol.Encoder) []byte {
		var b []byte
		var err error
		if b, err = protocol.Encode(e); err != nil {
			panic(err)
		}
		return b
	}
	type fields struct {
		topics map[*structs.Topic][]*structs.Partition
	}
	type args struct {
		requestCh  chan Request
		responseCh chan Response
		requests   []Request
		responses  []Response
	}
	tests := []struct {
		fields fields
		name   string
		args   args
		handle func(*testing.T, *Broker, Request, Response)
	}{
		{
			name: "api versions",
			args: args{
				requestCh:  make(chan Request, 2),
				responseCh: make(chan Response, 2),
				requests: []Request{{
					Header:  &protocol.RequestHeader{CorrelationID: 1},
					Request: &protocol.APIVersionsRequest{},
				}},
				responses: []Response{{
					Header:   &protocol.RequestHeader{CorrelationID: 1},
					Response: &protocol.Response{CorrelationID: 1, Body: APIVersions},
				}},
			},
		},
		{
			name: "create topic ok",
			args: args{
				requestCh:  make(chan Request, 2),
				responseCh: make(chan Response, 2),
				requests: []Request{{
					Header: &protocol.RequestHeader{CorrelationID: 1},
					Request: &protocol.CreateTopicRequests{Requests: []*protocol.CreateTopicRequest{{
						Topic:             "the-topic",
						NumPartitions:     1,
						ReplicationFactor: 1,
					}}}},
				},
				responses: []Response{{
					Header: &protocol.RequestHeader{CorrelationID: 1},
					Response: &protocol.Response{CorrelationID: 1, Body: &protocol.CreateTopicsResponse{
						TopicErrorCodes: []*protocol.TopicErrorCode{{Topic: "the-topic", ErrorCode: protocol.ErrNone.Code()}},
					}},
				}},
			},
		},
		{
			name: "create topic invalid replication factor error",
			args: args{
				requestCh:  make(chan Request, 2),
				responseCh: make(chan Response, 2),
				requests: []Request{{
					Header: &protocol.RequestHeader{CorrelationID: 1},
					Request: &protocol.CreateTopicRequests{Requests: []*protocol.CreateTopicRequest{{
						Topic:             "the-topic",
						NumPartitions:     1,
						ReplicationFactor: 2,
					}}}},
				},
				responses: []Response{{
					Header: &protocol.RequestHeader{CorrelationID: 1},
					Response: &protocol.Response{CorrelationID: 1, Body: &protocol.CreateTopicsResponse{
						TopicErrorCodes: []*protocol.TopicErrorCode{{Topic: "the-topic", ErrorCode: protocol.ErrInvalidReplicationFactor.Code()}},
					}},
				}},
			},
		},
		{
			name: "delete topic",
			args: args{
				requestCh:  make(chan Request, 2),
				responseCh: make(chan Response, 2),
				requests: []Request{{
					Header: &protocol.RequestHeader{CorrelationID: 1},
					Request: &protocol.CreateTopicRequests{Requests: []*protocol.CreateTopicRequest{{
						Topic:             "the-topic",
						NumPartitions:     1,
						ReplicationFactor: 1,
					}}}}, {
					Header:  &protocol.RequestHeader{CorrelationID: 2},
					Request: &protocol.DeleteTopicsRequest{Topics: []string{"the-topic"}}},
				},
				responses: []Response{{
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
			name: "offsets",
			args: args{
				requestCh:  make(chan Request, 2),
				responseCh: make(chan Response, 2),
				requests: []Request{
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
				responses: []Response{
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
			handle: func(t *testing.T, _ *Broker, req Request, res Response) {
				switch res := res.Response.(*protocol.Response).Body.(type) {
				// handle timestamp explicitly since we don't know what
				// it'll be set to
				case *protocol.ProduceResponses:
					handleProduceResponse(t, res)
				}
			},
		},
		{
			name: "fetch",
			args: args{
				requestCh:  make(chan Request, 2),
				responseCh: make(chan Response, 2),
				requests: []Request{
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
				responses: []Response{
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
			handle: func(t *testing.T, _ *Broker, req Request, res Response) {
				switch res := res.Response.(*protocol.Response).Body.(type) {
				// handle timestamp explicitly since we don't know what
				// it'll be set to
				case *protocol.ProduceResponses:
					handleProduceResponse(t, res)
				}
			},
		},
		{
			name: "metadata",
			args: args{
				requestCh:  make(chan Request, 2),
				responseCh: make(chan Response, 2),
				requests: []Request{
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
				responses: []Response{
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
			handle: func(t *testing.T, _ *Broker, req Request, res Response) {
				switch res := res.Response.(*protocol.Response).Body.(type) {
				// handle timestamp explicitly since we don't know what
				// it'll be set to
				case *protocol.ProduceResponses:
					handleProduceResponse(t, res)
				}
			},
		},
		{
			name: "produce topic/partition doesn't exist error",
			args: args{
				requestCh:  make(chan Request, 2),
				responseCh: make(chan Response, 2),
				requests: []Request{{
					Header: &protocol.RequestHeader{CorrelationID: 2},
					Request: &protocol.ProduceRequest{TopicData: []*protocol.TopicData{{
						Topic: "another-topic",
						Data: []*protocol.Data{{
							RecordSet: mustEncode(&protocol.MessageSet{Offset: 1, Messages: []*protocol.Message{{Value: []byte("The message.")}}})}}}}}},
				},
				responses: []Response{{
					Header: &protocol.RequestHeader{CorrelationID: 2},
					Response: &protocol.Response{CorrelationID: 2, Body: &protocol.ProduceResponses{
						Responses: []*protocol.ProduceResponse{{
							Topic:              "another-topic",
							PartitionResponses: []*protocol.ProducePartitionResponse{{Partition: 0, ErrorCode: protocol.ErrUnknownTopicOrPartition.Code()}},
						}},
					}}}},
			},
			handle: func(t *testing.T, _ *Broker, req Request, res Response) {
				switch res := res.Response.(*protocol.Response).Body.(type) {
				// handle timestamp explicitly since we don't know what
				// it'll be set to
				case *protocol.ProduceResponses:
					handleProduceResponse(t, res)
				}
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var dir string
			s, teardown := NewTestServer(t, func(cfg *config.BrokerConfig) {
				cfg.ID = 1
				cfg.Bootstrap = true
				cfg.BootstrapExpect = 1
				cfg.StartAsLeader = true
				cfg.Addr = "localhost:9092"
				dir = cfg.DataDir
			}, nil)
			b := s.broker

			ctx, cancel := context.WithCancel(context.Background())
			span := b.tracer.StartSpan("TestBroker_Run")
			span.SetTag("name", tt.name)
			span.SetTag("test", true)
			defer span.Finish()
			runCtx := opentracing.ContextWithSpan(ctx, span)

			defer func() {
				b.Leave()
				b.Shutdown()
				teardown()
			}()

			retry.Run(t, func(r *retry.R) {
				if len(b.brokerLookup.Brokers()) != 1 {
					r.Fatal("server not added")
				}
			})
			if tt.fields.topics != nil {
				for topic, ps := range tt.fields.topics {
					_, err := b.raftApply(structs.RegisterTopicRequestType, structs.RegisterTopicRequest{
						Topic: *topic,
					})
					if err != nil {
						t.Fatalf("err: %s", err)
					}
					for _, p := range ps {
						_, err = b.raftApply(structs.RegisterPartitionRequestType, structs.RegisterPartitionRequest{
							Partition: *p,
						})
						if err != nil {
							t.Fatalf("err: %s", err)
						}
					}
				}
			}

			go b.Run(ctx, tt.args.requestCh, tt.args.responseCh)

			for i := 0; i < len(tt.args.requests); i++ {
				req := tt.args.requests[i]
				reqSpan := b.tracer.StartSpan("request", opentracing.ChildOf(span.Context()))
				req.Ctx = opentracing.ContextWithSpan(runCtx, reqSpan)
				tt.args.requestCh <- req

				response := <-tt.args.responseCh

				if tt.handle != nil {
					tt.handle(t, b, tt.args.requests[i], response)
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
			var dir string
			s, teardown := NewTestServer(t, func(cfg *config.BrokerConfig) {
				cfg.ID = 1
				cfg.Bootstrap = true
				cfg.BootstrapExpect = 1
				cfg.StartAsLeader = true
				dir = cfg.DataDir
			}, nil)
			defer teardown()
			b := s.broker
			if err := b.Shutdown(); (err != nil) != tt.wantErr {
				t.Errorf("Shutdown() error = %v, wantErr %v", err, tt.wantErr)
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
	id     int32
	logger log.Logger
	logDir string
}

func newFields() fields {
	return fields{
		logger: log.New(),
		logDir: "/tmp/jocko/logs",
		id:     1,
	}
}

func TestBroker_JoinLAN(t *testing.T) {
	s1, t1 := NewTestServer(t, nil, nil)
	b1 := s1.broker
	defer t1()
	defer b1.Shutdown()
	s2, t2 := NewTestServer(t, nil, nil)
	b2 := s2.broker
	defer t2()
	defer b2.Shutdown()

	joinLAN(t, b1, b2)

	retry.Run(t, func(r *retry.R) {
		require.Equal(t, 2, len(b1.LANMembers()))
		require.Equal(t, 2, len(b2.LANMembers()))
	})
}

func TestBroker_RegisterMember(t *testing.T) {
	s1, t1 := NewTestServer(t, func(cfg *config.BrokerConfig) {
		cfg.Bootstrap = true
		cfg.BootstrapExpect = 3
	}, nil)
	b1 := s1.broker
	defer t1()
	defer b1.Shutdown()

	s2, t2 := NewTestServer(t, func(cfg *config.BrokerConfig) {
		cfg.Bootstrap = false
		cfg.BootstrapExpect = 3
	}, nil)
	b2 := s2.broker
	defer t2()
	defer b2.Shutdown()

	joinLAN(t, b2, b1)

	waitForLeader(t, b1, b2)

	state := b1.fsm.State()
	retry.Run(t, func(r *retry.R) {
		_, node, err := state.GetNode(b2.config.RaftAddr)
		if err != nil {
			r.Fatalf("err: %v", err)
		}
		if node == nil {
			r.Fatal("node not registered")
		}
	})
	retry.Run(t, func(r *retry.R) {
		_, node, err := state.GetNode(b1.config.RaftAddr)
		if err != nil {
			r.Fatalf("err: %v", err)
		}
		if node == nil {
			r.Fatal("node not registered")
		}
	})
}

func TestBroker_FailedMember(t *testing.T) {
	s1, t1 := NewTestServer(t, func(cfg *config.BrokerConfig) {
		cfg.Bootstrap = true
		cfg.BootstrapExpect = 2
	}, nil)
	b1 := s1.broker
	defer t1()
	defer b1.Shutdown()

	s2, t2 := NewTestServer(t, func(cfg *config.BrokerConfig) {
		cfg.Bootstrap = false
		cfg.BootstrapExpect = 2
		cfg.NonVoter = true
	}, nil)
	b2 := s2.broker
	defer t2()

	waitForLeader(t, b1, b2)

	joinLAN(t, b2, b1)

	// Fail the member
	b2.Shutdown()

	// Should be registered
	state := b1.fsm.State()
	retry.Run(t, func(r *retry.R) {
		_, node, err := state.GetNode(b2.config.RaftAddr)
		if err != nil {
			r.Fatalf("err: %v", err)
		}
		if node == nil {
			r.Fatal("node not registered")
		}
	})

	// todo: check have failed checks
}

func TestBroker_LeftMember(t *testing.T) {
	s1, t1 := NewTestServer(t, func(cfg *config.BrokerConfig) {
		cfg.Bootstrap = true
		cfg.BootstrapExpect = 2
	}, nil)
	b1 := s1.broker
	defer t1()

	s2, t2 := NewTestServer(t, func(cfg *config.BrokerConfig) {
		cfg.Bootstrap = false
		cfg.BootstrapExpect = 2
		cfg.NonVoter = true
	}, nil)
	b2 := s2.broker
	defer t2()

	waitForLeader(t, b1, b2)

	joinLAN(t, b2, b1)

	// Fail the member
	b2.Leave()
	b2.Shutdown()

	// Should be deregistered
	state := b1.fsm.State()
	retry.Run(t, func(r *retry.R) {
		_, node, err := state.GetNode(b2.config.RaftAddr)
		if err != nil {
			r.Fatalf("err: %v", err)
		}
		if node != nil {
			r.Fatal("node still registered")
		}
	})
}

func TestBroker_LeaveLeader(t *testing.T) {
	s1, t1 := NewTestServer(t, func(cfg *config.BrokerConfig) {
		cfg.Bootstrap = true
		cfg.BootstrapExpect = 3
	}, nil)
	b1 := s1.broker
	defer t1()
	defer b1.Shutdown()

	s2, t2 := NewTestServer(t, func(cfg *config.BrokerConfig) {
		cfg.Bootstrap = false
		cfg.BootstrapExpect = 3
	}, nil)
	b2 := s2.broker
	defer t2()
	defer b2.Shutdown()

	s3, t3 := NewTestServer(t, func(cfg *config.BrokerConfig) {
		cfg.Bootstrap = false
		cfg.BootstrapExpect = 3
	}, nil)
	b3 := s3.broker
	defer t3()
	defer b3.Shutdown()

	brokers := []*Broker{b1, b2, b3}

	joinLAN(t, b2, b1)
	joinLAN(t, b3, b1)

	for _, b := range brokers {
		retry.Run(t, func(r *retry.R) {
			r.Check(wantPeers(b, 3))
		})
	}

	var leader *Broker
	for _, b := range brokers {
		if b.isLeader() {
			leader = b
			break
		}
	}

	if leader == nil {
		t.Fatal("no leader")
	}

	if !leader.isReadyForConsistentReads() {
		t.Fatal("leader should be ready for consistent reads")
	}

	err := leader.Leave()
	require.NoError(t, err)

	if leader.isReadyForConsistentReads() {
		t.Fatal("leader should not be ready for consistent reads")
	}

	leader.Shutdown()

	var remain *Broker
	for _, b := range brokers {
		if b == leader {
			continue
		}
		remain = b
		retry.Run(t, func(r *retry.R) { r.Check(wantPeers(b, 2)) })
	}

	retry.Run(t, func(r *retry.R) {
		for _, b := range brokers {
			if leader == b && b.isLeader() {
				r.Fatal("should have new leader")
			}
		}
	})

	state := remain.fsm.State()
	retry.Run(t, func(r *retry.R) {
		_, node, err := state.GetNode(leader.config.RaftAddr)
		if err != nil {
			r.Fatalf("err: %v", err)
		}
		if node != nil {
			r.Fatal("leader should be deregistered")
		}
	})
}

func waitForLeader(t *testing.T, brokers ...*Broker) {
	retry.Run(t, func(r *retry.R) {
		for _, b := range brokers {
			if raft.Leader == b.raft.State() {
				t.Fatal("no leader")
			}
		}
	})
}

func joinLAN(t *testing.T, b1 *Broker, b2 *Broker) {
	addr := fmt.Sprintf("127.0.0.1:%d", b2.config.SerfLANConfig.MemberlistConfig.BindPort)
	err := b1.JoinLAN(addr)
	require.Equal(t, err, protocol.ErrNone)
}

// wantPeers determines whether the server has the given
// number of voting raft peers.
func wantPeers(s *Broker, peers int) error {
	n, err := s.numPeers()
	if err != nil {
		return err
	}
	if got, want := n, peers; got != want {
		return fmt.Errorf("got %d peers want %d", got, want)
	}
	return nil
}

func handleProduceResponse(t *testing.T, res *protocol.ProduceResponses) {
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
