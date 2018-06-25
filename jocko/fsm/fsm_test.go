package fsm

import (
	"reflect"
	"testing"

	stdopentracing "github.com/opentracing/opentracing-go"
	"github.com/travisjeffery/jocko/jocko/structs"
)

func testStore(t *testing.T) *Store {
	s, err := NewStore(stdopentracing.GlobalTracer())
	if err != nil {
		t.Fatalf("err: %s", err)
	}
	if s == nil {
		t.Fatalf("missing store")
	}
	return s
}

func testRegisterNode(t *testing.T, s *Store, idx uint64, nodeID int32) {
	testRegisterNodeWithMeta(t, s, idx, nodeID, nil)
}

func testRegisterNodeWithMeta(t *testing.T, s *Store, idx uint64, nodeID int32, meta map[string]string) {
	node := &structs.Node{Node: nodeID, Meta: meta}
	if err := s.EnsureNode(idx, node); err != nil {
		t.Fatalf("err: %s", err)
	}
	tx := s.db.Txn(false)
	defer tx.Abort()
	n, err := tx.First("nodes", "id", nodeID)
	if err != nil {
		t.Fatalf("err: %s", err)
	}
	if result, ok := n.(*structs.Node); !ok || result.Node != nodeID {
		t.Fatalf("bad node: %#v", result)
	}
}

func TestStore_maxIndex(t *testing.T) {
	s := testStore(t)
	testRegisterNode(t, s, 0, 1)
	testRegisterNode(t, s, 1, 2)

	if max := s.maxIndex("nodes", "services"); max != 1 {
		t.Fatalf("bad max: %d", max)
	}
}

func TestStore_Abandon(t *testing.T) {
	s := testStore(t)
	abandonCh := s.AbandonCh()
	s.Abandon()
	select {
	case <-abandonCh:
	default:
		t.Fatalf("bad")
	}
}

func TestStore_DeleteNode(t *testing.T) {
	s := testStore(t)

	// add the node
	testRegisterNode(t, s, 0, 1)

	if idx, ns, err := s.GetNodes(); err != nil || len(ns) != 1 || idx != 0 {
		t.Fatalf("bad: %#v %d (err: %#v)", ns, idx, err)
	}

	// delete the node
	if err := s.DeleteNode(1, 1); err != nil {
		t.Fatalf("err: %s", err)
	}

	// check it's gone
	if idx, n, err := s.GetNode(1); err != nil || n != nil || idx != 1 {
		t.Fatalf("bad: %#v %d (err: %#v)", n, idx, err)
	}

	if idx, ns, err := s.GetNodes(); err != nil || len(ns) != 0 || idx != 1 {
		t.Fatalf("bad: %#v %d (err: %#v)", ns, idx, err)
	}

	// index is updated
	if idx := s.maxIndex("nodes"); idx != 1 {
		t.Fatalf("bad index: %d", idx)
	}

	// deleting should be idempotent
	if err := s.DeleteNode(4, 1); err != nil {
		t.Fatalf("err: %s", err)
	}
	if idx := s.maxIndex("nodes"); idx != 1 {
		t.Fatalf("bad index: %d", idx)
	}
}

func TestStore_RegisterTopic(t *testing.T) {
	s := testStore(t)

	if _, topic, err := s.GetTopic("unknown-topic"); topic != nil && err != nil {
		t.Fatalf("err: %s, topic: %v", err, topic)
	}

	testRegisterTopic(t, s, 0, "topic1")

	if idx, topics, err := s.GetTopics(); err != nil || idx != 0 || !reflect.DeepEqual(topics, []*structs.Topic{{Topic: "topic1", Config: structs.NewTopicConfig()}}) {
		t.Fatalf("err: %s", err)
	}

	// delete the topic
	if err := s.DeleteTopic(1, "topic1"); err != nil {
		t.Fatalf("err: %s", err)
	}

	// check it's gone
	if idx, top, err := s.GetTopic("topic1"); err != nil || top != nil || idx != 1 {
		t.Fatalf("bad: %#v %d (err: %s)", top, idx, err)
	}

	// check index is updated
	if idx := s.maxIndex("topics"); idx != 1 {
		t.Fatalf("err: %v", idx)
	}

	// deleting should be idempotent
	if err := s.DeleteTopic(2, "topic1"); err != nil {
		t.Fatalf("err: %s", err)
	}
	if idx := s.maxIndex("topics"); idx != 1 {
		t.Fatalf("err: %v", idx)
	}
}

func testRegisterTopic(t *testing.T, s *Store, idx uint64, id string) {
	if err := s.EnsureTopic(idx, &structs.Topic{Topic: id}); err != nil {
		t.Fatalf("err: %s", err)
	}
	tx := s.db.Txn(false)
	defer tx.Abort()
	top, err := tx.First("topics", "id", id)
	if err != nil {
		t.Fatalf("err: %s", err)
	}
	if result, ok := top.(*structs.Topic); !ok || result.Topic != id {
		t.Fatalf("bad topic: %#v", result)
	}
}

func TestStore_RegisterPartition(t *testing.T) {
	s := testStore(t)

	testRegisterPartition(t, s, 0, 1, "test-topic")

	if _, p, err := s.GetPartition("test-topic", 1); err != nil || p == nil {
		t.Fatalf("err: %s, partition: %v", err, p)
	}

	if _, p, err := s.PartitionsByLeader(partitionLeader); err != nil || p == nil || len(p) != 1 {
		t.Fatalf("err: %s, partition: %v", err, p)
	}

	// delete the partition
	if err := s.DeletePartition(1, "test-topic", 1); err != nil {
		t.Fatalf("err: %s", err)
	}

	// check it's gone
	if idx, top, err := s.GetPartition("test-topic", 1); err != nil || top != nil || idx != 1 {
		t.Fatalf("bad: %#v %d (err: %s)", top, idx, err)
	}

	// check index is updated
	if idx := s.maxIndex("partitions"); idx != 1 {
		t.Fatalf("err: %d", idx)
	}

	// deleting should be idempotent
	if err := s.DeletePartition(2, "test-topic", 1); err != nil {
		t.Fatalf("err: %d", err)
	}
	if idx := s.maxIndex("partitions"); idx != 1 {
		t.Fatalf("err: %d", idx)
	}
}

const (
	partitionLeader = 1
)

func testRegisterPartition(t *testing.T, s *Store, idx uint64, id int32, topic string) {
	if err := s.EnsurePartition(idx, &structs.Partition{Partition: id, Topic: topic, Leader: partitionLeader}); err != nil {
		t.Fatalf("err: %s", err)
	}
	tx := s.db.Txn(false)
	defer tx.Abort()
	top, err := tx.First("partitions", "id", topic, id)
	if err != nil {
		t.Fatalf("err: %s", err)
	}
	if result, ok := top.(*structs.Partition); !ok || result.Partition != id {
		t.Fatalf("bad partition: %#v", result)
	}
}

func TestStore_RegisterGroup(t *testing.T) {
	s := testStore(t)

	testRegisterGroup(t, s, 0, "test-group")

	if _, p, err := s.GetGroup("test-group"); err != nil || p == nil {
		t.Fatalf("err: %s, group: %v", err, p)
	}

	if err := s.EnsureGroup(1, &structs.Group{Group: "test-group", Coordinator: coordinator, Members: map[string]structs.Member{"member": structs.Member{ID: "member"}}}); err != nil {
		t.Fatalf("err: %s", err)
	}

	if _, p, err := s.GetGroup("test-group"); err != nil || p == nil || len(p.Members) != 1 || p.Members["member"].ID != "member" {
		t.Fatalf("err: %s, group: %v", err, p)
	}

	if _, p, err := s.GetGroupsByCoordinator(coordinator); err != nil || p == nil || len(p) != 1 || p[0].Members["member"].ID != "member" {
		t.Fatalf("err: %s, group: %v", err, p)
	}

	if err := s.EnsureGroup(1, &structs.Group{Group: "test-group", Coordinator: coordinator, LeaderID: "leader"}); err != nil {
		t.Fatalf("err: %s", err)
	}

	if _, p, err := s.GetGroup("test-group"); err != nil || p == nil || p.LeaderID != "leader" {
		t.Fatalf("err: %s, group: %v", err, p)
	}

	// delete the group
	if err := s.DeleteGroup(2, "test-group"); err != nil {
		t.Fatalf("err: %s", err)
	}

	// check it's gone
	if idx, top, err := s.GetGroup("test-group"); err != nil || top != nil || idx != 2 {
		t.Fatalf("bad: %#v %d (err: %s)", top, idx, err)
	}

	// check index is updated
	if idx := s.maxIndex("groups"); idx != 2 {
		t.Fatalf("err: %d", idx)
	}

	// deleting should be idempotent
	if err := s.DeleteGroup(2, "test-group"); err != nil {
		t.Fatalf("err: %d", err)
	}
	if idx := s.maxIndex("groups"); idx != 2 {
		t.Fatalf("err: %d", idx)
	}
}

const (
	coordinator = int32(1)
)

func testRegisterGroup(t *testing.T, s *Store, idx uint64, id string) {
	if err := s.EnsureGroup(idx, &structs.Group{Group: id, Coordinator: coordinator}); err != nil {
		t.Fatalf("err: %s", err)
	}
	tx := s.db.Txn(false)
	defer tx.Abort()
	top, err := tx.First("groups", "id", id)
	if err != nil {
		t.Fatalf("err: %s", err)
	}
	if result, ok := top.(*structs.Group); !ok || result.Coordinator != coordinator {
		t.Fatalf("bad group: %#v", result)
	}
}
