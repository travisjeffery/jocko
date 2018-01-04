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
		t.Fatalf("err: %v", err)
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
		t.Fatalf("err: %v", err)
	}
	if idx := s.maxIndex("nodes"); idx != 1 {
		t.Fatalf("bad index: %d", idx)
	}
}
