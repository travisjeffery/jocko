package jocko

import (
	"bytes"
	"context"
	"fmt"
	"reflect"
	"testing"
	"time"

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
		requestCh  chan *Context
		responseCh chan *Context
		requests   []*Context
		responses  []*Context
	}
	tests := []struct {
		fields fields
		name   string
		args   args
		handle func(*testing.T, *Broker, *Context)
	}{
		{
			name: "api versions",
			args: args{
				requestCh:  make(chan *Context, 2),
				responseCh: make(chan *Context, 2),
				requests: []*Context{{
					header:  &protocol.RequestHeader{CorrelationID: 1},
					req: &protocol.APIVersionsRequest{},
				}},
				responses: []*Context{{
					header:   &protocol.RequestHeader{CorrelationID: 1},
					res: &protocol.Response{CorrelationID: 1, Body: apiVersions},
				}},
			},
		},
		{
			name: "create topic ok",
			args: args{
				requestCh:  make(chan *Context, 2),
				responseCh: make(chan *Context, 2),
				requests: []*Context{{
					header: &protocol.RequestHeader{CorrelationID: 1},
					req: &protocol.CreateTopicRequests{Requests: []*protocol.CreateTopicRequest{{
						Topic:             "the-topic",
						NumPartitions:     1,
						ReplicationFactor: 1,
					}}}},
				},
				responses: []*Context{{
					header: &protocol.RequestHeader{CorrelationID: 1},
					res: &protocol.Response{CorrelationID: 1, Body: &protocol.CreateTopicsResponse{
						TopicErrorCodes: []*protocol.TopicErrorCode{{Topic: "the-topic", ErrorCode: protocol.ErrNone.Code()}},
					}},
				}},
			},
		},
		{
			name: "create topic invalid replication factor error",
			args: args{
				requestCh:  make(chan *Context, 2),
				responseCh: make(chan *Context, 2),
				requests: []*Context{{
					header: &protocol.RequestHeader{CorrelationID: 1},
					req: &protocol.CreateTopicRequests{Requests: []*protocol.CreateTopicRequest{{
						Topic:             "the-topic",
						NumPartitions:     1,
						ReplicationFactor: 2,
					}}}},
				},
				responses: []*Context{{
					header: &protocol.RequestHeader{CorrelationID: 1},
					res: &protocol.Response{CorrelationID: 1, Body: &protocol.CreateTopicsResponse{
						TopicErrorCodes: []*protocol.TopicErrorCode{{Topic: "the-topic", ErrorCode: protocol.ErrInvalidReplicationFactor.Code()}},
					}},
				}},
			},
		},
		{
			name: "delete topic",
			args: args{
				requestCh:  make(chan *Context, 2),
				responseCh: make(chan *Context, 2),
				requests: []*Context{{
					header: &protocol.RequestHeader{CorrelationID: 1},
					req: &protocol.CreateTopicRequests{Requests: []*protocol.CreateTopicRequest{{
						Topic:             "the-topic",
						NumPartitions:     1,
						ReplicationFactor: 1,
					}}}}, {
					header:  &protocol.RequestHeader{CorrelationID: 2},
					req: &protocol.DeleteTopicsRequest{Topics: []string{"the-topic"}}},
				},
				responses: []*Context{{
					header: &protocol.RequestHeader{CorrelationID: 1},
					res: &protocol.Response{CorrelationID: 1, Body: &protocol.CreateTopicsResponse{
						TopicErrorCodes: []*protocol.TopicErrorCode{{Topic: "the-topic", ErrorCode: protocol.ErrNone.Code()}},
					}},
				}, {
					header: &protocol.RequestHeader{CorrelationID: 2},
					res: &protocol.Response{CorrelationID: 2, Body: &protocol.DeleteTopicsResponse{
						TopicErrorCodes: []*protocol.TopicErrorCode{{Topic: "the-topic", ErrorCode: protocol.ErrNone.Code()}},
					}}}},
			},
		},
		{
			name: "offsets",
			args: args{
				requestCh:  make(chan *Context, 2),
				responseCh: make(chan *Context, 2),
				requests: []*Context{
					{
						header: &protocol.RequestHeader{CorrelationID: 1},
						req: &protocol.CreateTopicRequests{Requests: []*protocol.CreateTopicRequest{{
							Topic:             "the-topic",
							NumPartitions:     1,
							ReplicationFactor: 1,
						}}},
					},
					{
						header: &protocol.RequestHeader{CorrelationID: 2},
						req: &protocol.ProduceRequest{TopicData: []*protocol.TopicData{{
							Topic: "the-topic",
							Data: []*protocol.Data{{
								RecordSet: mustEncode(&protocol.MessageSet{Offset: 0, Messages: []*protocol.Message{{Value: []byte("The message.")}}})}}}}},
					},
					{
						header:  &protocol.RequestHeader{CorrelationID: 3},
						req: &protocol.OffsetsRequest{ReplicaID: 0, Topics: []*protocol.OffsetsTopic{{Topic: "the-topic", Partitions: []*protocol.OffsetsPartition{{Partition: 0, Timestamp: -1}}}}},
					},
					{
						header:  &protocol.RequestHeader{CorrelationID: 4},
						req: &protocol.OffsetsRequest{ReplicaID: 0, Topics: []*protocol.OffsetsTopic{{Topic: "the-topic", Partitions: []*protocol.OffsetsPartition{{Partition: 0, Timestamp: -2}}}}},
					},
				},
				responses: []*Context{
					{
						header: &protocol.RequestHeader{CorrelationID: 1},
						res: &protocol.Response{CorrelationID: 1, Body: &protocol.CreateTopicsResponse{
							TopicErrorCodes: []*protocol.TopicErrorCode{{Topic: "the-topic", ErrorCode: protocol.ErrNone.Code()}},
						}},
					},
					{
						header: &protocol.RequestHeader{CorrelationID: 2},
						res: &protocol.Response{CorrelationID: 2, Body: &protocol.ProduceResponses{
							Responses: []*protocol.ProduceResponse{{
								Topic:              "the-topic",
								PartitionResponses: []*protocol.ProducePartitionResponse{{Partition: 0, BaseOffset: 0, ErrorCode: protocol.ErrNone.Code()}},
							}},
						}},
					},
					{
						header: &protocol.RequestHeader{CorrelationID: 3},
						res: &protocol.Response{CorrelationID: 3, Body: &protocol.OffsetsResponse{
							Responses: []*protocol.OffsetResponse{{
								Topic:              "the-topic",
								PartitionResponses: []*protocol.PartitionResponse{{Partition: 0, Offsets: []int64{1}, ErrorCode: protocol.ErrNone.Code()}},
							}},
						}},
					},
					{
						header: &protocol.RequestHeader{CorrelationID: 4},
						res: &protocol.Response{CorrelationID: 4, Body: &protocol.OffsetsResponse{
							Responses: []*protocol.OffsetResponse{{
								Topic:              "the-topic",
								PartitionResponses: []*protocol.PartitionResponse{{Partition: 0, Offsets: []int64{0}, ErrorCode: protocol.ErrNone.Code()}},
							}},
						}},
					},
				},
			},
			handle: func(t *testing.T, _ *Broker, ctx *Context) {
				switch res := ctx.res.(*protocol.Response).Body.(type) {
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
				requestCh:  make(chan *Context, 2),
				responseCh: make(chan *Context, 2),
				requests: []*Context{
					{
						header: &protocol.RequestHeader{CorrelationID: 1},
						req: &protocol.CreateTopicRequests{Requests: []*protocol.CreateTopicRequest{{
							Topic:             "the-topic",
							NumPartitions:     1,
							ReplicationFactor: 1,
						}}},
					},
					{
						header: &protocol.RequestHeader{CorrelationID: 2},
						req: &protocol.ProduceRequest{TopicData: []*protocol.TopicData{{
							Topic: "the-topic",
							Data: []*protocol.Data{{
								RecordSet: mustEncode(&protocol.MessageSet{Offset: 0, Messages: []*protocol.Message{{Value: []byte("The message.")}}})}}}}},
					},
					{
						header:  &protocol.RequestHeader{CorrelationID: 3},
						req: &protocol.FetchRequest{ReplicaID: 1, MinBytes: 5, Topics: []*protocol.FetchTopic{{Topic: "the-topic", Partitions: []*protocol.FetchPartition{{Partition: 0, FetchOffset: 0, MaxBytes: 100}}}}},
					},
				},
				responses: []*Context{
					{
						header: &protocol.RequestHeader{CorrelationID: 1},
						res: &protocol.Response{CorrelationID: 1, Body: &protocol.CreateTopicsResponse{
							TopicErrorCodes: []*protocol.TopicErrorCode{{Topic: "the-topic", ErrorCode: protocol.ErrNone.Code()}},
						}},
					},
					{
						header: &protocol.RequestHeader{CorrelationID: 2},
						res: &protocol.Response{CorrelationID: 2, Body: &protocol.ProduceResponses{
							Responses: []*protocol.ProduceResponse{
								{
									Topic:              "the-topic",
									PartitionResponses: []*protocol.ProducePartitionResponse{{Partition: 0, BaseOffset: 0, ErrorCode: protocol.ErrNone.Code()}},
								},
							},
						}},
					},
					{
						header: &protocol.RequestHeader{CorrelationID: 3},
						res: &protocol.Response{CorrelationID: 3, Body: &protocol.FetchResponses{
							Responses: []*protocol.FetchResponse{{
								Topic: "the-topic",
								PartitionResponses: []*protocol.FetchPartitionResponse{{
									Partition:     0,
									ErrorCode:     protocol.ErrNone.Code(),
									HighWatermark: 0,
									RecordSet:     mustEncode(&protocol.MessageSet{Offset: 0, Messages: []*protocol.Message{{Value: []byte("The message.")}}}),
								}},
							}}},
						},
					},
				},
			},
			handle: func(t *testing.T, _ *Broker, ctx *Context) {
				switch res := ctx.res.(*protocol.Response).Body.(type) {
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
				requestCh:  make(chan *Context, 2),
				responseCh: make(chan *Context, 2),
				requests: []*Context{
					{
						header: &protocol.RequestHeader{CorrelationID: 1},
						req: &protocol.CreateTopicRequests{Requests: []*protocol.CreateTopicRequest{{
							Topic:             "the-topic",
							NumPartitions:     1,
							ReplicationFactor: 1,
						}}},
					},
					{
						header: &protocol.RequestHeader{CorrelationID: 2},
						req: &protocol.ProduceRequest{TopicData: []*protocol.TopicData{{
							Topic: "the-topic",
							Data: []*protocol.Data{{
								RecordSet: mustEncode(&protocol.MessageSet{Offset: 0, Messages: []*protocol.Message{{Value: []byte("The message.")}}})}}}}},
					},
					{
						header:  &protocol.RequestHeader{CorrelationID: 3},
						req: &protocol.MetadataRequest{Topics: []string{"the-topic", "unknown-topic"}},
					},
				},
				responses: []*Context{
					{
						header: &protocol.RequestHeader{CorrelationID: 1},
						res: &protocol.Response{CorrelationID: 1, Body: &protocol.CreateTopicsResponse{
							TopicErrorCodes: []*protocol.TopicErrorCode{{Topic: "the-topic", ErrorCode: protocol.ErrNone.Code()}},
						}},
					},
					{
						header: &protocol.RequestHeader{CorrelationID: 2},
						res: &protocol.Response{CorrelationID: 2, Body: &protocol.ProduceResponses{
							Responses: []*protocol.ProduceResponse{
								{
									Topic:              "the-topic",
									PartitionResponses: []*protocol.ProducePartitionResponse{{Partition: 0, BaseOffset: 0, ErrorCode: protocol.ErrNone.Code()}},
								},
							},
						}},
					},
					{
						header: &protocol.RequestHeader{CorrelationID: 3},
						res: &protocol.Response{CorrelationID: 3, Body: &protocol.MetadataResponse{
							Brokers: []*protocol.Broker{{NodeID: 1, Host: "localhost", Port: 9092}},
							TopicMetadata: []*protocol.TopicMetadata{
								{Topic: "the-topic", TopicErrorCode: protocol.ErrNone.Code(), PartitionMetadata: []*protocol.PartitionMetadata{{PartitionErrorCode: protocol.ErrNone.Code(), ParititionID: 0, Leader: 1, Replicas: []int32{1}, ISR: []int32{1}}}},
								{Topic: "unknown-topic", TopicErrorCode: protocol.ErrUnknownTopicOrPartition.Code()},
							},
						}},
					},
				},
			},
			handle: func(t *testing.T, _ *Broker, ctx *Context) {
				switch res := ctx.res.(*protocol.Response).Body.(type) {
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
				requestCh:  make(chan *Context, 2),
				responseCh: make(chan *Context, 2),
				requests: []*Context{{
					header: &protocol.RequestHeader{CorrelationID: 2},
					req: &protocol.ProduceRequest{TopicData: []*protocol.TopicData{{
						Topic: "another-topic",
						Data: []*protocol.Data{{
							RecordSet: mustEncode(&protocol.MessageSet{Offset: 1, Messages: []*protocol.Message{{Value: []byte("The message.")}}})}}}}}},
				},
				responses: []*Context{{
					header: &protocol.RequestHeader{CorrelationID: 2},
					res: &protocol.Response{CorrelationID: 2, Body: &protocol.ProduceResponses{
						Responses: []*protocol.ProduceResponse{{
							Topic:              "another-topic",
							PartitionResponses: []*protocol.ProducePartitionResponse{{Partition: 0, ErrorCode: protocol.ErrUnknownTopicOrPartition.Code()}},
						}},
					}}}},
			},
			handle: func(t *testing.T, _ *Broker, ctx *Context) {
				switch res := ctx.res.(*protocol.Response).Body.(type) {
				// handle timestamp explicitly since we don't know what
				// it'll be set to
				case *protocol.ProduceResponses:
					handleProduceResponse(t, res)
				}
			},
		},
		// {
		// 	name: "find coordinator",
		// 	args: args{
		// 		requestCh:  make(chan *Context, 2),
		// 		responseCh: make(chan *Context, 2),
		// 		requests: []*Context{{
		// 			header: &protocol.RequestHeader{CorrelationID: 3},
		// 			req: &protocol.FindCoordinatorRequest{
		// 				CoordinatorKey: "test-group",
		// 			},
		// 		}},
		// 		responses: []*Context{{
		// 			header: &protocol.RequestHeader{CorrelationID: 3},
		// 			res: &protocol.res{CorrelationID: 3, Body: &protocol.FindCoordinatorResponse{
		// 				Coordinator: protocol.Coordinator{
		// 					NodeID: 1,
		// 					Host:   "localhost",
		// 					Port:   9092,
		// 				},
		// 			}},
		// 		}},
		// 	},
		// },
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s, teardown := NewTestServer(t, func(cfg *config.Config) {
				cfg.ID = 1
				cfg.Bootstrap = true
				cfg.BootstrapExpect = 1
				cfg.StartAsLeader = true
				cfg.Addr = "localhost:9092"
			}, nil)
			b := s.broker()

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
				request := tt.args.requests[i]
				reqSpan := b.tracer.StartSpan("request", opentracing.ChildOf(span.Context()))

				ctx := &Context{header: request.header, req: request.req, parent: opentracing.ContextWithSpan(runCtx, reqSpan)}

				tt.args.requestCh <- ctx

				ctx = <-tt.args.responseCh

				if tt.handle != nil {
					tt.handle(t, b, ctx)
				}

				if !reflect.DeepEqual(ctx.res, tt.args.responses[i].res) {
					t.Errorf("got %s, want: %s", spewstr(ctx.res), spewstr(tt.args.responses[i].res))
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
			s, teardown := NewTestServer(t, func(cfg *config.Config) {
				cfg.ID = 1
				cfg.Bootstrap = true
				cfg.BootstrapExpect = 1
				cfg.StartAsLeader = true
			}, nil)
			defer teardown()
			b := s.broker()
			if err := b.Shutdown(); (err != nil) != tt.wantErr {
				t.Errorf("Shutdown() error = %v, wantErr %v", err, tt.wantErr)
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
	b1 := s1.broker()
	defer t1()
	defer b1.Shutdown()
	s2, t2 := NewTestServer(t, nil, nil)
	b2 := s2.broker()
	defer t2()
	defer b2.Shutdown()

	joinLAN(t, b1, b2)

	retry.Run(t, func(r *retry.R) {
		require.Equal(t, 2, len(b1.LANMembers()))
		require.Equal(t, 2, len(b2.LANMembers()))
	})
}

func TestBroker_RegisterMember(t *testing.T) {
	s1, t1 := NewTestServer(t, func(cfg *config.Config) {
		cfg.Bootstrap = true
		cfg.BootstrapExpect = 3
	}, nil)
	b1 := s1.broker()
	defer t1()
	defer b1.Shutdown()

	s2, t2 := NewTestServer(t, func(cfg *config.Config) {
		cfg.Bootstrap = false
		cfg.BootstrapExpect = 3
	}, nil)
	b2 := s2.broker()
	defer t2()
	defer b2.Shutdown()

	joinLAN(t, b2, b1)

	waitForLeader(t, b1, b2)

	state := b1.fsm.State()
	retry.Run(t, func(r *retry.R) {
		_, node, err := state.GetNode(b2.config.ID)
		if err != nil {
			r.Fatalf("err: %v", err)
		}
		if node == nil {
			r.Fatal("node not registered")
		}
	})
	retry.Run(t, func(r *retry.R) {
		_, node, err := state.GetNode(b1.config.ID)
		if err != nil {
			r.Fatalf("err: %v", err)
		}
		if node == nil {
			r.Fatal("node not registered")
		}
	})
}

func TestBroker_FailedMember(t *testing.T) {
	s1, t1 := NewTestServer(t, func(cfg *config.Config) {
		cfg.Bootstrap = true
		cfg.BootstrapExpect = 1
		cfg.StartAsLeader = true
	}, nil)
	defer t1()
	defer s1.Shutdown()

	s2, t2 := NewTestServer(t, func(cfg *config.Config) {
		cfg.Bootstrap = false
		cfg.NonVoter = true
	}, nil)
	defer t2()

	TestJoin(t, s2, s1)

	// Fail the member
	s2.Shutdown()

	state := s1.broker().fsm.State()

	// Should be registered
	retry.Run(t, func(r *retry.R) {
		_, node, err := state.GetNode(s2.broker().config.ID)
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
	s1, t1 := NewTestServer(t, func(cfg *config.Config) {
		cfg.Bootstrap = true
		cfg.BootstrapExpect = 1
		cfg.StartAsLeader = true
	}, nil)
	defer t1()

	s2, t2 := NewTestServer(t, func(cfg *config.Config) {
		cfg.Bootstrap = false
		cfg.NonVoter = true
	}, nil)
	defer t2()

	TestJoin(t, s2, s1)

	state := s1.broker().fsm.State()

	// should be registered
	retry.Run(t, func(r *retry.R) {
		_, node, err := state.GetNode(s2.broker().config.ID)
		if err != nil {
			r.Fatalf("err: %v", err)
		}
		if node == nil {
			r.Fatal("node isn't registered")
		}
	})

	s2.broker().Leave()

	// Should be deregistered
	retry.Run(t, func(r *retry.R) {
		_, node, err := state.GetNode(s2.broker().config.ID)
		if err != nil {
			r.Fatalf("err: %v", err)
		}
		if node != nil {
			r.Fatal("node still registered")
		}
	})
}

func TestBroker_LeaveLeader(t *testing.T) {
	s1, t1 := NewTestServer(t, func(cfg *config.Config) {
		cfg.Bootstrap = true
		cfg.BootstrapExpect = 3
	}, nil)
	b1 := s1.broker()
	defer t1()
	defer b1.Shutdown()

	s2, t2 := NewTestServer(t, func(cfg *config.Config) {
		cfg.Bootstrap = false
		cfg.BootstrapExpect = 3
	}, nil)
	b2 := s2.broker()
	defer t2()
	defer b2.Shutdown()

	s3, t3 := NewTestServer(t, func(cfg *config.Config) {
		cfg.Bootstrap = false
		cfg.BootstrapExpect = 3
	}, nil)
	b3 := s3.broker()
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
		_, node, err := state.GetNode(leader.config.ID)
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
		var leader *Broker
		for _, b := range brokers {
			if raft.Leader == b.raft.State() {
				leader = b
			}
		}
		if leader == nil {
			r.Fatal("no leader")
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
			if pr.LogAppendTime.IsZero() {
				t.Error("expected timestamp not to be 0")
			}
			pr.LogAppendTime = time.Time{}
		}
	}
}

func (s *Server) broker() *Broker {
	return s.handler.(*Broker)
}
