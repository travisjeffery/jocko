package fsm

import (
	"testing"

	"github.com/hashicorp/raft"
	stdopentracing "github.com/opentracing/opentracing-go"
	"github.com/travisjeffery/jocko/jocko/structs"
)

func TestRegisterNode(t *testing.T) {
	fsm, err := New(stdopentracing.GlobalTracer())
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	req := structs.RegisterNodeRequest{
		Node: structs.Node{Node: 1},
	}
	buf, err := structs.Encode(structs.RegisterNodeRequestType, req)
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	resp := fsm.Apply(makeLog(buf))
	if resp != nil {
		t.Fatalf("resp: %v", resp)
	}

	_, node, err := fsm.state.GetNode(1)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if node == nil {
		t.Fatalf("node not found")
	}
	if node.ModifyIndex != 1 {
		t.Fatalf("bad index: %d", node.ModifyIndex)
	}
}

func TestRegisterTopic(t *testing.T) {
	fsm, err := New(stdopentracing.GlobalTracer())
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	req := structs.RegisterTopicRequest{
		Topic: structs.Topic{Topic: "topic1"},
	}
	buf, err := structs.Encode(structs.RegisterTopicRequestType, req)
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	resp := fsm.Apply(makeLog(buf))
	if resp != nil {
		t.Fatalf("resp: %v", resp)
	}

	_, topic, err := fsm.state.GetTopic("topic1")
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if topic == nil {
		t.Fatalf("topic not found")
	}
	if topic.ModifyIndex != 1 {
		t.Fatalf("bad index: %d", topic.ModifyIndex)
	}
}

func TestRegisterPartition(t *testing.T) {
	fsm, err := New(stdopentracing.GlobalTracer())
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	req := structs.RegisterPartitionRequest{
		Partition: structs.Partition{Partition: 1, Topic: "test-topic"},
	}
	buf, err := structs.Encode(structs.RegisterPartitionRequestType, req)
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	resp := fsm.Apply(makeLog(buf))
	if resp != nil {
		t.Fatalf("resp: %v", resp)
	}

	_, partition, err := fsm.state.GetPartition("test-topic", 1)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if partition == nil {
		t.Fatalf("partition not found")
	}
	if partition.ModifyIndex != 1 {
		t.Fatalf("bad index: %d", partition.ModifyIndex)
	}
}

func TestRegisterGroup(t *testing.T) {
	fsm, err := New(stdopentracing.GlobalTracer())
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	req := structs.RegisterGroupRequest{
		Group: structs.Group{Group: "group-id", Members: map[string]structs.Member{}},
	}
	buf, err := structs.Encode(structs.RegisterGroupRequestType, req)
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	resp := fsm.Apply(makeLog(buf))
	if resp != nil {
		t.Fatalf("resp: %v", resp)
	}

	_, group, err := fsm.state.GetGroup("group-id")
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if group == nil {
		t.Fatalf("group not found")
	}
	if group.ModifyIndex != 1 {
		t.Fatalf("bad index: %d", group.ModifyIndex)
	}
}

func makeLog(buf []byte) *raft.Log {
	return &raft.Log{
		Index: 1,
		Term:  1,
		Type:  raft.LogCommand,
		Data:  buf,
	}
}
