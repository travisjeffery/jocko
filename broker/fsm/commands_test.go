package fsm

import (
	"testing"

	"github.com/hashicorp/raft"
	"github.com/travisjeffery/jocko/broker/structs"
	"github.com/travisjeffery/jocko/log"
)

func TestRegisterNode(t *testing.T) {
	fsm, err := New(log.New())
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	req := structs.RegisterNodeRequest{
		Node: structs.Node{Node: "node1"},
	}
	buf, err := structs.Encode(structs.RegisterNodeRequestType, req)
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	resp := fsm.Apply(makeLog(buf))
	if resp != nil {
		t.Fatalf("resp: %v", resp)
	}

	_, node, err := fsm.state.GetNode("node1")
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
	fsm, err := New(log.New())
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
	fsm, err := New(log.New())
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

func makeLog(buf []byte) *raft.Log {
	return &raft.Log{
		Index: 1,
		Term:  1,
		Type:  raft.LogCommand,
		Data:  buf,
	}
}
