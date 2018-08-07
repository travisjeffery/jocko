// +build !race

package jocko

import (
	"bytes"
	"context"
	"fmt"
	"net"
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/davecgh/go-spew/spew"
	"github.com/hashicorp/consul/testutil/retry"
	"github.com/hashicorp/raft"
	"github.com/hashicorp/serf/serf"
	opentracing "github.com/opentracing/opentracing-go"
	"github.com/stretchr/testify/require"

	"github.com/travisjeffery/jocko/jocko/config"
	"github.com/travisjeffery/jocko/jocko/structs"
	"github.com/travisjeffery/jocko/log"
	"github.com/travisjeffery/jocko/protocol"
)

func TestBroker_Run(t *testing.T) {
	log.SetPrefix("broker_test: ")

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
					header: &protocol.RequestHeader{CorrelationID: 1},
					req:    &protocol.APIVersionsRequest{},
				}},
				responses: []*Context{{
					header: &protocol.RequestHeader{CorrelationID: 1},
					res:    &protocol.Response{CorrelationID: 1, Body: apiVersions},
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
						Topic:             "test-topic",
						NumPartitions:     1,
						ReplicationFactor: 1,
					}}}},
				},
				responses: []*Context{{
					header: &protocol.RequestHeader{CorrelationID: 1},
					res: &protocol.Response{CorrelationID: 1, Body: &protocol.CreateTopicsResponse{
						TopicErrorCodes: []*protocol.TopicErrorCode{{Topic: "test-topic", ErrorCode: protocol.ErrNone.Code()}},
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
						Topic:             "test-topic",
						NumPartitions:     1,
						ReplicationFactor: 2,
					}}}},
				},
				responses: []*Context{{
					header: &protocol.RequestHeader{CorrelationID: 1},
					res: &protocol.Response{CorrelationID: 1, Body: &protocol.CreateTopicsResponse{
						TopicErrorCodes: []*protocol.TopicErrorCode{{Topic: "test-topic", ErrorCode: protocol.ErrInvalidReplicationFactor.Code()}},
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
						Topic:             "test-topic",
						NumPartitions:     1,
						ReplicationFactor: 1,
					}}}}, {
					header: &protocol.RequestHeader{CorrelationID: 2},
					req:    &protocol.DeleteTopicsRequest{Topics: []string{"test-topic"}}},
				},
				responses: []*Context{{
					header: &protocol.RequestHeader{CorrelationID: 1},
					res: &protocol.Response{CorrelationID: 1, Body: &protocol.CreateTopicsResponse{
						TopicErrorCodes: []*protocol.TopicErrorCode{{Topic: "test-topic", ErrorCode: protocol.ErrNone.Code()}},
					}},
				}, {
					header: &protocol.RequestHeader{CorrelationID: 2},
					res: &protocol.Response{CorrelationID: 2, Body: &protocol.DeleteTopicsResponse{
						TopicErrorCodes: []*protocol.TopicErrorCode{{Topic: "test-topic", ErrorCode: protocol.ErrNone.Code()}},
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
						req: &protocol.CreateTopicRequests{
							Timeout: 100 * time.Millisecond,
							Requests: []*protocol.CreateTopicRequest{{
								Topic:             "test-topic",
								NumPartitions:     1,
								ReplicationFactor: 1,
							}}},
					},
					{
						header: &protocol.RequestHeader{CorrelationID: 2},
						req: &protocol.ProduceRequest{
							Timeout: 100 * time.Millisecond,
							TopicData: []*protocol.TopicData{{
								Topic: "test-topic",
								Data: []*protocol.Data{{
									RecordSet: mustEncode(&protocol.MessageSet{Offset: 0, Messages: []*protocol.Message{{Value: []byte("The message.")}}})}}}}},
					},
					{
						header: &protocol.RequestHeader{CorrelationID: 3},
						req:    &protocol.OffsetsRequest{ReplicaID: 0, Topics: []*protocol.OffsetsTopic{{Topic: "test-topic", Partitions: []*protocol.OffsetsPartition{{Partition: 0, Timestamp: -1}}}}},
					},
					{
						header: &protocol.RequestHeader{CorrelationID: 4},
						req:    &protocol.OffsetsRequest{ReplicaID: 0, Topics: []*protocol.OffsetsTopic{{Topic: "test-topic", Partitions: []*protocol.OffsetsPartition{{Partition: 0, Timestamp: -2}}}}},
					},
				},
				responses: []*Context{
					{
						header: &protocol.RequestHeader{CorrelationID: 1},
						res: &protocol.Response{CorrelationID: 1, Body: &protocol.CreateTopicsResponse{
							TopicErrorCodes: []*protocol.TopicErrorCode{{Topic: "test-topic", ErrorCode: protocol.ErrNone.Code()}},
						}},
					},
					{
						header: &protocol.RequestHeader{CorrelationID: 2},
						res: &protocol.Response{CorrelationID: 2, Body: &protocol.ProduceResponse{
							Responses: []*protocol.ProduceTopicResponse{{
								Topic:              "test-topic",
								PartitionResponses: []*protocol.ProducePartitionResponse{{Partition: 0, BaseOffset: 0, ErrorCode: protocol.ErrNone.Code()}},
							}},
						}},
					},
					{
						header: &protocol.RequestHeader{CorrelationID: 3},
						res: &protocol.Response{CorrelationID: 3, Body: &protocol.OffsetsResponse{
							Responses: []*protocol.OffsetResponse{{
								Topic:              "test-topic",
								PartitionResponses: []*protocol.PartitionResponse{{Partition: 0, Offsets: []int64{1}, ErrorCode: protocol.ErrNone.Code()}},
							}},
						}},
					},
					{
						header: &protocol.RequestHeader{CorrelationID: 4},
						res: &protocol.Response{CorrelationID: 4, Body: &protocol.OffsetsResponse{
							Responses: []*protocol.OffsetResponse{{
								Topic:              "test-topic",
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
				case *protocol.ProduceResponse:
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
						req: &protocol.CreateTopicRequests{
							Timeout: 100 * time.Millisecond,
							Requests: []*protocol.CreateTopicRequest{{
								Topic:             "test-topic",
								NumPartitions:     1,
								ReplicationFactor: 1,
							}}},
					},
					{
						header: &protocol.RequestHeader{CorrelationID: 2},
						req: &protocol.ProduceRequest{
							Timeout: 100 * time.Millisecond,
							TopicData: []*protocol.TopicData{{
								Topic: "test-topic",
								Data: []*protocol.Data{{
									RecordSet: mustEncode(&protocol.MessageSet{Offset: 0, Messages: []*protocol.Message{{Value: []byte("The message.")}}})}}},
							}},
					},
					{
						header: &protocol.RequestHeader{CorrelationID: 3},
						req: &protocol.FetchRequest{
							MaxWaitTime: 100 * time.Millisecond,
							ReplicaID:   1,
							MinBytes:    5,
							Topics: []*protocol.FetchTopic{
								{
									Topic: "test-topic",
									Partitions: []*protocol.FetchPartition{{Partition: 0,
										FetchOffset: 0,
										MaxBytes:    100,
									}},
								},
							}},
					},
				},
				responses: []*Context{
					{
						header: &protocol.RequestHeader{CorrelationID: 1},
						res: &protocol.Response{CorrelationID: 1, Body: &protocol.CreateTopicsResponse{
							TopicErrorCodes: []*protocol.TopicErrorCode{{Topic: "test-topic", ErrorCode: protocol.ErrNone.Code()}},
						}},
					},
					{
						header: &protocol.RequestHeader{CorrelationID: 2},
						res: &protocol.Response{CorrelationID: 2, Body: &protocol.ProduceResponse{
							Responses: []*protocol.ProduceTopicResponse{
								{
									Topic:              "test-topic",
									PartitionResponses: []*protocol.ProducePartitionResponse{{Partition: 0, BaseOffset: 0, ErrorCode: protocol.ErrNone.Code()}},
								},
							},
						}},
					},
					{
						header: &protocol.RequestHeader{CorrelationID: 3},
						res: &protocol.Response{CorrelationID: 3, Body: &protocol.FetchResponse{
							Responses: protocol.FetchTopicResponses{{
								Topic: "test-topic",
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
				case *protocol.ProduceResponse:
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
						req: &protocol.CreateTopicRequests{
							Timeout: 100 * time.Millisecond,
							Requests: []*protocol.CreateTopicRequest{{
								Topic:             "test-topic",
								NumPartitions:     1,
								ReplicationFactor: 1,
							}}},
					},
					{
						header: &protocol.RequestHeader{CorrelationID: 2},
						req: &protocol.ProduceRequest{
							Timeout: 100 * time.Millisecond,
							TopicData: []*protocol.TopicData{{
								Topic: "test-topic",
								Data: []*protocol.Data{{
									RecordSet: mustEncode(&protocol.MessageSet{Offset: 0, Messages: []*protocol.Message{{Value: []byte("The message.")}}})}}}}},
					},
					{
						header: &protocol.RequestHeader{CorrelationID: 3},
						req:    &protocol.MetadataRequest{Topics: []string{"test-topic", "unknown-topic"}},
					},
				},
				responses: []*Context{
					{
						header: &protocol.RequestHeader{CorrelationID: 1},
						res: &protocol.Response{CorrelationID: 1, Body: &protocol.CreateTopicsResponse{
							TopicErrorCodes: []*protocol.TopicErrorCode{{Topic: "test-topic", ErrorCode: protocol.ErrNone.Code()}},
						}},
					},
					{
						header: &protocol.RequestHeader{CorrelationID: 2},
						res: &protocol.Response{CorrelationID: 2, Body: &protocol.ProduceResponse{
							Responses: []*protocol.ProduceTopicResponse{
								{
									Topic:              "test-topic",
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
								{Topic: "test-topic", TopicErrorCode: protocol.ErrNone.Code(), PartitionMetadata: []*protocol.PartitionMetadata{{PartitionErrorCode: protocol.ErrNone.Code(), PartitionID: 0, Leader: 1, Replicas: []int32{1}, ISR: []int32{1}}}},
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
				case *protocol.ProduceResponse:
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
					req: &protocol.ProduceRequest{
						Timeout: 100 * time.Millisecond,
						TopicData: []*protocol.TopicData{{
							Topic: "another-topic",
							Data: []*protocol.Data{{
								RecordSet: mustEncode(&protocol.MessageSet{Offset: 1, Messages: []*protocol.Message{{Value: []byte("The message.")}}})}}}}}},
				},
				responses: []*Context{{
					header: &protocol.RequestHeader{CorrelationID: 2},
					res: &protocol.Response{CorrelationID: 2, Body: &protocol.ProduceResponse{
						Responses: []*protocol.ProduceTopicResponse{{
							Topic:              "another-topic",
							PartitionResponses: []*protocol.ProducePartitionResponse{{Partition: 0, ErrorCode: protocol.ErrUnknownTopicOrPartition.Code()}},
						}},
					}}}},
			},
			handle: func(t *testing.T, _ *Broker, ctx *Context) {
				switch res := ctx.res.(*protocol.Response).Body.(type) {
				// handle timestamp explicitly since we don't know what
				// it'll be set to
				case *protocol.ProduceResponse:
					handleProduceResponse(t, res)
				}
			},
		},
		{
			name: "find coordinator",
			args: args{
				requestCh:  make(chan *Context, 2),
				responseCh: make(chan *Context, 2),
				requests: []*Context{{
					header: &protocol.RequestHeader{CorrelationID: 1},
					req: &protocol.CreateTopicRequests{
						Timeout: 100 * time.Millisecond,
						Requests: []*protocol.CreateTopicRequest{{
							Topic:             "test-topic",
							NumPartitions:     1,
							ReplicationFactor: 1,
						}}},
				}, {
					header: &protocol.RequestHeader{CorrelationID: 3},
					req: &protocol.FindCoordinatorRequest{
						CoordinatorKey:  "test-group",
						CoordinatorType: protocol.CoordinatorGroup,
					},
				}},
				responses: []*Context{{
					header: &protocol.RequestHeader{CorrelationID: 1},
					res: &protocol.Response{CorrelationID: 1, Body: &protocol.CreateTopicsResponse{
						TopicErrorCodes: []*protocol.TopicErrorCode{{Topic: "test-topic", ErrorCode: protocol.ErrNone.Code()}},
					}},
				}, {
					header: &protocol.RequestHeader{CorrelationID: 3},
					res: &protocol.Response{CorrelationID: 3, Body: &protocol.FindCoordinatorResponse{
						Coordinator: protocol.Coordinator{
							NodeID: 1,
							Host:   "localhost",
							Port:   9092,
						},
					}},
				}},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s, dir := NewTestServer(t, func(cfg *config.Config) {
				cfg.ID = 1
				cfg.Bootstrap = true
				cfg.BootstrapExpect = 1
				cfg.StartAsLeader = true
				cfg.Addr = "localhost:9092"
				cfg.OffsetsTopicReplicationFactor = 1
			}, nil)
			b := s.broker()

			ctx, cancel := context.WithCancel(context.Background())
			span := b.tracer.StartSpan("TestBroker_Run")
			span.SetTag("name", tt.name)
			span.SetTag("test", true)
			defer span.Finish()
			runCtx := opentracing.ContextWithSpan(ctx, span)

			defer func() {
				os.RemoveAll(dir)
				s.Shutdown()
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

// setupTest sets up a server/broker to send requests to get responses back via the returned
// channels. Call teardown when your test is finished.
func setupTest(t *testing.T) (
	ctx context.Context,
	srv *Server,
	reqCh chan *Context,
	resCh chan *Context,
	teardown func(),
) {
	s, dir := NewTestServer(t, func(cfg *config.Config) {
		cfg.ID = 1
		cfg.Bootstrap = true
		cfg.BootstrapExpect = 1
		cfg.StartAsLeader = true
		cfg.Addr = "localhost:9092"
		cfg.OffsetsTopicReplicationFactor = 1
	}, nil)
	b := s.broker()

	ctx, cancel := context.WithCancel(context.Background())

	span := b.tracer.StartSpan(t.Name())
	span.SetTag("name", t.Name())
	span.SetTag("test", true)

	retry.Run(t, func(r *retry.R) {
		if len(b.brokerLookup.Brokers()) != 1 {
			r.Fatal("server not added")
		}
	})

	reqCh = make(chan *Context, 2)
	resCh = make(chan *Context, 2)

	go b.Run(ctx, reqCh, resCh)

	teardown = func() {
		close(reqCh)
		close(resCh)
		span.Finish()
		cancel()
		os.RemoveAll(dir)
		s.Shutdown()
	}

	ctx = opentracing.ContextWithSpan(ctx, span)

	return ctx, s, reqCh, resCh, teardown
}

func TestBroker_Run_JoinSyncGroup(t *testing.T) {
	t.Skip()

	ctx, _, reqCh, resCh, teardown := setupTest(t)
	defer teardown()

	correlationID := int32(1)

	// create topic
	req := &Context{
		header: &protocol.RequestHeader{
			CorrelationID: correlationID,
			ClientID:      "join-and-sync",
		},
		req: &protocol.CreateTopicRequests{
			Timeout: 100 * time.Millisecond,
			Requests: []*protocol.CreateTopicRequest{{
				Topic:             "test-topic",
				NumPartitions:     1,
				ReplicationFactor: 1,
			}}},
		parent: ctx,
	}
	reqCh <- req

	act := <-resCh
	exp := &Context{
		header: &protocol.RequestHeader{CorrelationID: correlationID},
		res: &protocol.Response{CorrelationID: correlationID, Body: &protocol.CreateTopicsResponse{
			TopicErrorCodes: []*protocol.TopicErrorCode{{Topic: "test-topic", ErrorCode: protocol.ErrNone.Code()}},
		}},
	}
	if !reflect.DeepEqual(act.res, exp.res) {
		t.Errorf("got %s, want: %s", spewstr(act.res), spewstr(exp.res))
	}

	correlationID++

	// join group
	req = &Context{
		header: &protocol.RequestHeader{
			CorrelationID: correlationID,
			ClientID:      "join-and-sync",
		},
		req: &protocol.JoinGroupRequest{
			GroupID:      "test-group",
			ProtocolType: "consumer",
			GroupProtocols: []*protocol.GroupProtocol{{
				ProtocolName:     "protocolname",
				ProtocolMetadata: []byte("protocolmetadata"),
			}},
		},
		parent: ctx,
	}
	reqCh <- req
	act = <-resCh

	memberID := act.res.(*protocol.Response).Body.(*protocol.JoinGroupResponse).MemberID
	require.NotZero(t, memberID)
	require.Equal(t, memberID, act.res.(*protocol.Response).Body.(*protocol.JoinGroupResponse).LeaderID)
	require.Equal(t, memberID, act.res.(*protocol.Response).Body.(*protocol.JoinGroupResponse).Members[0].MemberID)

	correlationID++

	// sync group
	req = &Context{
		header: &protocol.RequestHeader{
			CorrelationID: correlationID,
			ClientID:      "join-and-sync",
		},
		req: &protocol.SyncGroupRequest{
			GroupID:      "test-group",
			GenerationID: 1,
			MemberID:     memberID,
		},
		parent: ctx,
	}
	reqCh <- req

	act = <-resCh
	exp = &Context{
		header: &protocol.RequestHeader{
			CorrelationID: correlationID,
		},
		res: &protocol.Response{
			CorrelationID: correlationID,
			Body:          &protocol.SyncGroupResponse{},
		},
	}

	if !reflect.DeepEqual(act.res, exp.res) {
		t.Errorf("got %s, want: %s", spewstr(act.res), spewstr(exp.res))
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
			s, dir := NewTestServer(t, func(cfg *config.Config) {
				cfg.Bootstrap = true
			}, nil)
			defer os.RemoveAll(dir)
			if err := s.Start(context.Background()); err != nil {
				t.Fatal(err)
			}
			if err := s.Shutdown(); (err != nil) != tt.wantErr {
				t.Fatalf("Shutdown() error = %v, wantErr = %v", err, tt.wantErr)
			}
		})
	}
}

func spewstr(v interface{}) string {
	var buf bytes.Buffer
	spew.Fdump(&buf, v)
	return buf.String()
}

type fields struct {
	id     int32
	logDir string
}

func newFields() fields {
	return fields{
		logDir: "/tmp/jocko/logs",
		id:     1,
	}
}

func TestBroker_JoinLAN(t *testing.T) {
	s1, dir1 := NewTestServer(t, nil, nil)

	defer os.RemoveAll(dir1)
	defer s1.Shutdown()
	s2, dir2 := NewTestServer(t, nil, nil)

	defer os.RemoveAll(dir2)
	defer s2.Shutdown()

	joinLAN(t, s1, s2)

	retry.Run(t, func(r *retry.R) {
		require.Equal(t, 2, len(s1.broker().LANMembers()))
		require.Equal(t, 2, len(s2.broker().LANMembers()))
	})
}

func TestBroker_RegisterMember(t *testing.T) {
	s1, dir1 := NewTestServer(t, func(cfg *config.Config) {
		cfg.Bootstrap = true
		cfg.BootstrapExpect = 3
	}, nil)

	defer os.RemoveAll(dir1)
	defer s1.Shutdown()

	s2, dir2 := NewTestServer(t, func(cfg *config.Config) {
		cfg.Bootstrap = false
		cfg.BootstrapExpect = 3
	}, nil)

	defer os.RemoveAll(dir2)
	defer s2.Shutdown()

	joinLAN(t, s2, s1)

	waitForLeader(t, s1, s2)

	state := s1.broker().fsm.State()
	retry.Run(t, func(r *retry.R) {
		_, node, err := state.GetNode(s2.config.ID)
		if err != nil {
			r.Fatalf("err: %v", err)
		}
		if node == nil {
			r.Fatal("node not registered")
		}
	})
	retry.Run(t, func(r *retry.R) {
		_, node, err := state.GetNode(s1.config.ID)
		if err != nil {
			r.Fatalf("err: %v", err)
		}
		if node == nil {
			r.Fatal("node not registered")
		}
	})
}

func TestBroker_FailedMember(t *testing.T) {
	s1, dir1 := NewTestServer(t, func(cfg *config.Config) {
		cfg.Bootstrap = true
		cfg.BootstrapExpect = 1
		cfg.StartAsLeader = true
	}, nil)
	defer os.RemoveAll(dir1)
	defer s1.Shutdown()

	s2, dir2 := NewTestServer(t, func(cfg *config.Config) {
		cfg.Bootstrap = false
		cfg.NonVoter = true
	}, nil)
	defer os.RemoveAll(dir2)

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
	s1, dir1 := NewTestServer(t, func(cfg *config.Config) {
		cfg.Bootstrap = true
		cfg.BootstrapExpect = 1
		cfg.StartAsLeader = true
	}, nil)

	defer os.RemoveAll(dir1)
	defer s1.Shutdown()

	s2, dir2 := NewTestServer(t, func(cfg *config.Config) {
		cfg.Bootstrap = false
		cfg.NonVoter = true
	}, nil)

	defer os.RemoveAll(dir2)
	defer s2.Shutdown()

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

func TestBroker_ReapLeader(t *testing.T) {
	s1, dir1 := NewTestServer(t, func(cfg *config.Config) {
		cfg.Bootstrap = true
	}, nil)
	defer os.RemoveAll(dir1)
	defer s1.Shutdown()

	s2, dir2 := NewTestServer(t, func(cfg *config.Config) {
		cfg.Bootstrap = false
	}, nil)
	defer os.RemoveAll(dir2)
	defer s2.Shutdown()

	s3, dir3 := NewTestServer(t, func(cfg *config.Config) {
		cfg.Bootstrap = false
	}, nil)
	defer os.RemoveAll(dir3)
	defer s3.Shutdown()

	joinLAN(t, s1, s2)
	joinLAN(t, s1, s3)

	state := s1.broker().fsm.State()

	retry.Run(t, func(r *retry.R) {
		_, node, err := state.GetNode(s3.config.ID)
		if err != nil {
			r.Fatalf("err: %v", err)
		}
		if node == nil {
			r.Fatal("server not registered")
		}
	})

	knownMembers := make(map[int32]struct{})
	knownMembers[s1.config.ID] = struct{}{}
	knownMembers[s2.config.ID] = struct{}{}
	err := s1.broker().reconcileReaped(knownMembers)
	if err != nil {
		t.Fatal(err)
	}
	retry.Run(t, func(t *retry.R) {
		_, node, err := state.GetNode(s3.config.ID)
		if err != nil {
			t.Fatal(err)
		}
		if node != nil {
			t.Fatalf("server with id %v should be deregistered", s3.config.ID)
		}
	})
}

func TestBroker_ReapMember(t *testing.T) {
	s1, dir1 := NewTestServer(t, func(cfg *config.Config) {
		cfg.Bootstrap = true
	}, nil)
	defer os.RemoveAll(dir1)
	defer s1.Shutdown()

	s2, dir2 := NewTestServer(t, func(cfg *config.Config) {
		cfg.Bootstrap = false
	}, nil)
	defer os.RemoveAll(dir2)
	defer s2.Shutdown()

	joinLAN(t, s1, s2)

	state := s1.broker().fsm.State()

	retry.Run(t, func(r *retry.R) {
		_, node, err := state.GetNode(s2.config.ID)
		if err != nil {
			r.Fatalf("err: %v", err)
		}
		if node == nil {
			r.Fatal("server not registered")
		}
	})

	mems := s1.broker().LANMembers()
	var b2mem serf.Member
	for _, m := range mems {
		if m.Name == s2.config.NodeName {
			b2mem = m
			b2mem.Status = StatusReap
			break
		}
	}
	s1.broker().reconcileCh <- b2mem

	reaped := false
	for start := time.Now(); time.Since(start) < 5*time.Second; {
		_, node, err := state.GetNode(s2.config.ID)
		if err != nil {
			t.Fatalf("err: %v", err)
		}
		if node == nil {
			reaped = true
			break
		}
	}
	if !reaped {
		t.Fatalf("server should not be registered")
	}
}

func TestBroker_LeftLeader(t *testing.T) {
	s1, dir1 := NewTestServer(t, func(cfg *config.Config) {
		cfg.Bootstrap = true
		cfg.BootstrapExpect = 3
	}, nil)

	defer os.RemoveAll(dir1)
	defer s1.Shutdown()

	s2, dir2 := NewTestServer(t, func(cfg *config.Config) {
		cfg.Bootstrap = false
		cfg.BootstrapExpect = 3
	}, nil)
	defer os.RemoveAll(dir2)
	defer s2.Shutdown()

	s3, dir3 := NewTestServer(t, func(cfg *config.Config) {
		cfg.Bootstrap = false
		cfg.BootstrapExpect = 3
	}, nil)
	defer os.RemoveAll(dir3)
	defer s3.Shutdown()

	brokers := []*Broker{s1.broker(), s2.broker(), s3.broker()}

	joinLAN(t, s2, s1)
	joinLAN(t, s3, s1)

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

func waitForLeader(t *testing.T, servers ...*Server) {
	retry.Run(t, func(r *retry.R) {
		var leader *Server
		for _, s := range servers {
			if raft.Leader == s.broker().raft.State() {
				leader = s
			}
		}
		if leader == nil {
			r.Fatal("no leader")
		}
	})
}

func joinLAN(t *testing.T, leader *Server, member *Server) {
	if leader == nil || member == nil {
		panic("no server")
	}
	leaderAddr := fmt.Sprintf("127.0.0.1:%d", leader.config.SerfLANConfig.MemberlistConfig.BindPort)
	memberAddr := fmt.Sprintf("127.0.0.1:%d", member.config.SerfLANConfig.MemberlistConfig.BindPort)
	if err := member.broker().JoinLAN(leaderAddr); err != protocol.ErrNone {
		t.Fatal(err)
	}
	retry.Run(t, func(r *retry.R) {
		if !seeEachOther(leader.broker().LANMembers(), member.broker().LANMembers(), leaderAddr, memberAddr) {
			r.Fatalf("leader and member cannot see each other")
		}
	})
	if !seeEachOther(leader.broker().LANMembers(), member.broker().LANMembers(), leaderAddr, memberAddr) {
		t.Fatalf("leader and member cannot see each other")
	}
}

func seeEachOther(a, b []serf.Member, addra, addrb string) bool {
	return serfMembersContains(a, addrb) && serfMembersContains(b, addra)
}

func serfMembersContains(members []serf.Member, addr string) bool {
	_, want, err := net.SplitHostPort(addr)
	if err != nil {
		panic(err)
	}
	for _, m := range members {
		if got := fmt.Sprintf("%d", m.Port); got == want {
			return true
		}
	}
	return false
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

func handleProduceResponse(t *testing.T, res *protocol.ProduceResponse) {
	for _, response := range res.Responses {
		for _, pr := range response.PartitionResponses {
			if pr.ErrorCode != protocol.ErrNone.Code() {
				break
			}
			if pr.LogAppendTime.IsZero() {
				continue
			}
			pr.LogAppendTime = time.Time{}
		}
	}
}

func (s *Server) broker() *Broker {
	return s.handler.(*Broker)
}
