package fsm

import (
	"testing"

	"github.com/travisjeffery/jocko/broker/structs"
	"github.com/travisjeffery/jocko/log"
)

func testStore(t *testing.T) *Store {
	s, err := NewStore(log.New())
	if err != nil {
		t.Fatalf("err: %s", err)
	}
	if s == nil {
		t.Fatalf("missing store")
	}
	return s
}

func testRegisterNode(t *testing.T, s *Store, idx uint64, nodeID string) {
	testRegisterNodeWithMeta(t, s, idx, nodeID, nil)
}

func testRegisterNodeWithMeta(t *testing.T, s *Store, idx uint64, nodeID string, meta map[string]string) {
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
	testRegisterNode(t, s, 0, "foo")
	testRegisterNode(t, s, 1, "bar")

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
	testRegisterNode(t, s, 0, "node1")

	// delete the node
	if err := s.DeleteNode(1, "node1"); err != nil {
		t.Fatalf("err: %s", err)
	}

	// check it's gone
	if idx, n, err := s.GetNode("node1"); err != nil || n != nil || idx != 1 {
		t.Fatalf("bad: %#v %d (err: %#v)", n, idx, err)
	}

	// index is updated
	if idx := s.maxIndex("nodes"); idx != 1 {
		t.Fatalf("bad index: %d", idx)
	}

	// deleting should be idempotent
	if err := s.DeleteNode(4, "node1"); err != nil {
		t.Fatalf("err: %s", err)
	}
	if idx := s.maxIndex("nodes"); idx != 1 {
		t.Fatalf("bad index: %d", idx)
	}
}

func TestStore_RegisterTopic(t *testing.T) {
	s := testStore(t)

	testRegisterTopic(t, s, 0, "topic1")

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
		t.Fatalf("err: %s", idx)
	}

	// deleting should be idempotent
	if err := s.DeleteTopic(2, "topic1"); err != nil {
		t.Fatalf("err: %d", err)
	}
	if idx := s.maxIndex("topics"); idx != 1 {
		t.Fatalf("err: %s", idx)
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

	testRegisterPartition(t, s, 0, 1)

	// delete the partition
	if err := s.DeletePartition(1, 1); err != nil {
		t.Fatalf("err: %s", err)
	}

	// check it's gone
	if idx, top, err := s.GetPartition(1); err != nil || top != nil || idx != 1 {
		t.Fatalf("bad: %#v %d (err: %s)", top, idx, err)
	}

	// check index is updated
	if idx := s.maxIndex("partitions"); idx != 1 {
		t.Fatalf("err: %s", idx)
	}

	// deleting should be idempotent
	if err := s.DeletePartition(2, 1); err != nil {
		t.Fatalf("err: %d", err)
	}
	if idx := s.maxIndex("partitions"); idx != 1 {
		t.Fatalf("err: %s", idx)
	}
}

func testRegisterPartition(t *testing.T, s *Store, idx uint64, id int32) {
	if err := s.EnsurePartition(idx, &structs.Partition{Partition: id}); err != nil {
		t.Fatalf("err: %s", err)
	}
	tx := s.db.Txn(false)
	defer tx.Abort()
	top, err := tx.First("partitions", "id", id)
	if err != nil {
		t.Fatalf("err: %s", err)
	}
	if result, ok := top.(*structs.Partition); !ok || result.Partition != id {
		t.Fatalf("bad partition: %#v", result)
	}
}
