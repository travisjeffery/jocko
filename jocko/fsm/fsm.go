package fsm

import (
	"fmt"
	"io"
	"os"
	"strings"
	"sync"

	memdb "github.com/hashicorp/go-memdb"
	"github.com/hashicorp/raft"
	opentracing "github.com/opentracing/opentracing-go"
	"github.com/travisjeffery/jocko/jocko/structs"
	"github.com/travisjeffery/jocko/jocko/util"
	"github.com/travisjeffery/jocko/log"
	"github.com/ugorji/go/codec"
)

var (
	fsmVerboseLogs bool
)

type command func(buf []byte, index uint64) interface{}

// unboundCommand is a command method on the FSM, not yet bound to an FSM
// instance.
type unboundCommand func(c *FSM, buf []byte, index uint64) interface{}

// commands is a map from message type to unbound command.
var commands map[structs.MessageType]unboundCommand

func registerCommand(msg structs.MessageType, fn unboundCommand) {
	if commands == nil {
		commands = make(map[structs.MessageType]unboundCommand)
	}
	if commands[msg] != nil {
		panic(fmt.Errorf("Message %d is already registered", msg))
	}
	commands[msg] = fn
}

type NodeID int32
type Tracer opentracing.Tracer

// FSM implements a finite state machine used with Raft to provide strong consistency.
type FSM struct {
	apply     map[structs.MessageType]command
	stateLock sync.RWMutex
	state     *Store
	tracer    opentracing.Tracer
	nodeID    NodeID
}

// New returns a new FSM instance.
func New(args ...interface{}) (*FSM, error) {
	var nodeID NodeID
	var tracer Tracer
	for _, arg := range args {
		switch a := arg.(type) {
		case NodeID:
			nodeID = a
		case Tracer:
			tracer = a
		}
	}
	store, err := NewStore(tracer, nodeID)
	if err != nil {
		return nil, err
	}
	fsm := &FSM{
		apply:  make(map[structs.MessageType]command),
		state:  store,
		tracer: tracer,
		nodeID: nodeID,
	}
	for msg, fn := range commands {
		thisFn := fn
		fsm.apply[msg] = func(buf []byte, index uint64) interface{} {
			return thisFn(fsm, buf, index)
		}
	}
	return fsm, nil
}

// State is used to return a handle to the current state
func (c *FSM) State() *Store {
	c.stateLock.RLock()
	defer c.stateLock.RUnlock()
	return c.state
}

func (c *FSM) Apply(l *raft.Log) interface{} {
	buf := l.Data
	msgType := structs.MessageType(buf[0])
	if fn := c.apply[msgType]; fn != nil {
		return fn(buf[1:], l.Index)
	}
	return nil
}

func (c *FSM) Restore(old io.ReadCloser) error {
	defer old.Close()

	newState, err := NewStore(c.tracer)
	if err != nil {
		return err
	}

	restore := newState.Restore()
	defer restore.Abort()

	dec := codec.NewDecoder(old, msgpackHandle)

	var header snapshotHeader
	if err := dec.Decode(&header); err != nil {
		return err
	}

	msgType := make([]byte, 1)
	for {
		_, err := old.Read(msgType)
		if err == io.EOF {
			break
		} else if err != nil {
			return err
		}

		msg := structs.MessageType(msgType[0])
		if fn := restorers[msg]; fn != nil {
			if err := fn(&header, restore, dec); err != nil {
				return err
			}
		} else {
			return fmt.Errorf("unrecognized msg type %d", msg)
		}
	}
	restore.Commit()

	c.stateLock.Lock()
	oldState := c.state
	c.state = newState
	c.stateLock.Unlock()

	oldState.Abandon()
	return nil
}

func (c *FSM) Snapshot() (raft.FSMSnapshot, error) {
	return &snapshot{c.state.Snapshot()}, nil
}

// schemaFn is an interface function used to create and return
// new memdb schema structs for constructing an in-memory db.
type schemaFn func() *memdb.TableSchema

// schemas is used to register schemas with the state store.
var schemas []schemaFn

// registerSchema registers a new schema with the state store. This should
// get called at package init() time.
func registerSchema(fn schemaFn) {
	schemas = append(schemas, fn)
}

type Store struct {
	schema *memdb.DBSchema
	db     *memdb.MemDB
	// abandonCh is used to signal watchers this store has been abandoned
	// (usually during a restore).
	abandonCh chan struct{}
	tracer    opentracing.Tracer
	nodeID    NodeID
}

func NewStore(args ...interface{}) (*Store, error) {
	dbSchema := &memdb.DBSchema{
		Tables: make(map[string]*memdb.TableSchema),
	}
	for _, fn := range schemas {
		schema := fn()
		if _, ok := dbSchema.Tables[schema.Name]; ok {
			panic(fmt.Sprintf("duplicate table name: %s", schema.Name))
		}
		dbSchema.Tables[schema.Name] = schema
	}
	db, err := memdb.NewMemDB(dbSchema)
	if err != nil {
		return nil, err
	}
	s := &Store{
		schema:    dbSchema,
		db:        db,
		abandonCh: make(chan struct{}),
	}
	for _, arg := range args {
		switch a := arg.(type) {
		case NodeID:
			s.nodeID = a
		case Tracer:
			s.tracer = a
		}
	}
	return s, nil
}

// Abandon is used to signal that the given state store has been abandoned.
// Calling this more than one time will panic.
func (s *Store) Abandon() {
	close(s.abandonCh)
}

// AbandonCh returns a channel you can wait on to know if the state store was
// abandoned.
func (s *Store) AbandonCh() <-chan struct{} {
	return s.abandonCh
}

// GetNode is used to retrieve a node by node name ID.
func (s *Store) GetNode(id int32) (uint64, *structs.Node, error) {
	sp := s.tracer.StartSpan("store: get node")
	sp.LogKV("id", id)
	sp.SetTag("node id", s.nodeID)
	defer sp.Finish()

	tx := s.db.Txn(false)
	defer tx.Abort()

	idx := maxIndexTxn(tx, "nodes")

	node, err := tx.First("nodes", "id", id)
	if err != nil {
		return 0, nil, fmt.Errorf("node lookup failed: %s", err)
	}
	if node != nil {
		return idx, node.(*structs.Node), nil
	}
	return idx, nil, nil
}

func (s *Store) GetNodes() (uint64, []*structs.Node, error) {
	sp := s.tracer.StartSpan("store: get nodes")
	defer sp.Finish()

	tx := s.db.Txn(false)
	defer tx.Abort()

	idx := maxIndexTxn(tx, "nodes")
	it, err := tx.Get("nodes", "id")
	if err != nil {
		return 0, nil, fmt.Errorf("node lookup failed: %s", err)
	}
	var nodes []*structs.Node
	for next := it.Next(); next != nil; next = it.Next() {
		nodes = append(nodes, next.(*structs.Node))
	}
	return idx, nodes, nil
}

// EnsureNode is used to upsert nodes.
func (s *Store) EnsureNode(idx uint64, node *structs.Node) error {
	sp := s.tracer.StartSpan("store: ensure node")
	s.vlog(sp, "node", node)
	sp.SetTag("node id", s.nodeID)
	defer sp.Finish()

	tx := s.db.Txn(true)
	defer tx.Abort()

	if err := s.ensureNodeTxn(tx, idx, node); err != nil {
		return err
	}

	tx.Commit()
	return nil
}

// DeleteNode is used to delete nodes.
func (s *Store) DeleteNode(idx uint64, id int32) error {
	sp := s.tracer.StartSpan("store: delete node")
	sp.LogKV("id", id)
	sp.SetTag("node id", s.nodeID)
	defer sp.Finish()

	tx := s.db.Txn(true)
	defer tx.Abort()

	if err := s.deleteNodeTxn(tx, idx, id); err != nil {
		return err
	}

	tx.Commit()
	return nil
}

func (s *Store) deleteNodeTxn(tx *memdb.Txn, idx uint64, id int32) error {
	node, err := tx.First("nodes", "id", id)
	if err != nil {
		log.Error.Printf("fsm: node lookup error: %s", err)
		return err
	}
	if node == nil {
		return nil
	}
	// todo: delete anything attached to the node
	if err := tx.Delete("nodes", node); err != nil {
		log.Error.Printf("fsm: deleting node error: %s", err)
		return err
	}
	// update the index
	if err := tx.Insert("index", &IndexEntry{"nodes", idx}); err != nil {
		log.Error.Printf("fsm: updating index error: %s", err)
		return err
	}
	return nil
}

func (s *Store) EnsureRegistration(idx uint64, req *structs.RegisterNodeRequest) error {
	sp := s.tracer.StartSpan("store: ensure registration")
	s.vlog(sp, "req", req)
	sp.SetTag("node id", s.nodeID)
	defer sp.Finish()

	tx := s.db.Txn(true)
	defer tx.Abort()

	if err := s.ensureRegistration(tx, idx, req); err != nil {
		return err
	}

	tx.Commit()
	return nil
}

func (s *Store) ensureRegistration(tx *memdb.Txn, idx uint64, req *structs.RegisterNodeRequest) error {
	existing, err := tx.First("nodes", "id", req.Node.Node)
	if err != nil {
		log.Error.Printf("fsm: node lookup error: %s", err)
		return err
	}

	if existing == nil {
		if err := s.ensureNodeTxn(tx, idx, &req.Node); err != nil {
			return err
		}
	}

	return nil
}

// ensureNodeTxn is the inner function to actually create or modify a node.
func (s *Store) ensureNodeTxn(tx *memdb.Txn, idx uint64, node *structs.Node) error {
	var n *structs.Node
	existing, err := tx.First("nodes", "id", node.Node)
	if err != nil {
		return fmt.Errorf("node lookup failed: %s", err)
	}
	if existing != nil {
		n = existing.(*structs.Node)
	}

	if n != nil {
		node.CreateIndex = n.CreateIndex
		node.ModifyIndex = idx
	} else {
		node.CreateIndex = idx
		node.ModifyIndex = idx
	}

	if err := tx.Insert("nodes", node); err != nil {
		return fmt.Errorf("failed inserting node: %s", err)
	}
	if err := tx.Insert("index", &IndexEntry{"nodes", idx}); err != nil {
		return fmt.Errorf("failed updating index: %s", err)
	}

	return nil
}

func (s *Store) EnsureTopic(idx uint64, topic *structs.Topic) error {
	sp := s.tracer.StartSpan("store: ensure topic")
	s.vlog(sp, "topic", topic)
	sp.SetTag("node id", s.nodeID)

	if topic.Config == nil {
		topic.Config = structs.NewTopicConfig()
	}

	defer sp.Finish()

	tx := s.db.Txn(true)
	defer tx.Abort()
	if err := s.ensureTopicTxn(tx, idx, topic); err != nil {
		return err
	}
	tx.Commit()
	return nil
}

func (s *Store) ensureTopicTxn(tx *memdb.Txn, idx uint64, topic *structs.Topic) error {
	var t *structs.Topic
	existing, err := tx.First("topics", "id", topic.Topic)
	if err != nil {
		return fmt.Errorf("topic lookup failed: %s", err)
	}

	if existing != nil {
		t = existing.(*structs.Topic)
	}

	if t != nil {
		topic.CreateIndex = t.CreateIndex
		topic.ModifyIndex = idx
	} else {
		topic.CreateIndex = idx
		topic.ModifyIndex = idx
	}

	if err := tx.Insert("topics", topic); err != nil {
		return fmt.Errorf("failed inserting topic: %s", err)
	}

	if err := tx.Insert("index", &IndexEntry{"topics", idx}); err != nil {
		return fmt.Errorf("failed updating index: %s", err)
	}

	return nil
}

// GetTopic is used to get topics.
func (s *Store) GetTopic(id string) (uint64, *structs.Topic, error) {
	sp := s.tracer.StartSpan("store: get topic")
	sp.LogKV("id", id)
	sp.SetTag("node id", s.nodeID)
	defer sp.Finish()

	tx := s.db.Txn(false)
	defer tx.Abort()
	idx := maxIndexTxn(tx, "topics")

	topic, err := tx.First("topics", "id", id)
	if err != nil {
		return 0, nil, fmt.Errorf("topic lookup failed: %s", err)
	}
	if topic != nil {
		return idx, topic.(*structs.Topic), nil
	}

	return idx, nil, nil
}

func (s *Store) GetTopics() (uint64, []*structs.Topic, error) {
	sp := s.tracer.StartSpan("store: get topics")
	sp.SetTag("node id", s.nodeID)
	defer sp.Finish()

	tx := s.db.Txn(false)
	defer tx.Abort()
	idx := maxIndexTxn(tx, "topics")
	it, err := tx.Get("topics", "id")
	if err != nil {
		return 0, nil, err
	}
	var topics []*structs.Topic
	for next := it.Next(); next != nil; next = it.Next() {
		topics = append(topics, next.(*structs.Topic))
	}
	return idx, topics, nil
}

// DeleteTopic is used to delete topics.
func (s *Store) DeleteTopic(idx uint64, id string) error {
	tx := s.db.Txn(true)
	defer tx.Abort()

	if err := s.deleteTopicTxn(tx, idx, id); err != nil {
		return err
	}

	tx.Commit()
	return nil
}

func (s *Store) deleteTopicTxn(tx *memdb.Txn, idx uint64, id string) error {
	topic, err := tx.First("topics", "id", id)
	if err != nil {
		log.Error.Printf("fsm: topic lookup error: %s", err)
		return err
	}
	if topic == nil {
		return nil
	}
	if err := tx.Delete("topics", topic); err != nil {
		log.Error.Printf("fsm: deleting topic error: %s", err)
		return err
	}
	if err := tx.Insert("index", &IndexEntry{"topics", idx}); err != nil {
		log.Error.Printf("fsm: updating index error: %s", err)
		return err
	}
	return nil
}

func (s *Store) EnsureGroup(idx uint64, group *structs.Group) error {
	sp := s.tracer.StartSpan("store: ensure group")
	s.vlog(sp, "group", group)
	sp.SetTag("node id", s.nodeID)
	defer sp.Finish()

	tx := s.db.Txn(true)
	defer tx.Abort()
	if err := s.ensureGroupTxn(tx, idx, group); err != nil {
		return err
	}
	tx.Commit()
	return nil
}

func (s *Store) ensureGroupTxn(tx *memdb.Txn, idx uint64, group *structs.Group) error {
	var t *structs.Group
	existing, err := tx.First("groups", "id", group.Group)
	if err != nil {
		return fmt.Errorf("group lookup failed: %s", err)
	}

	if existing != nil {
		t = existing.(*structs.Group)
	}

	if t != nil {
		group.CreateIndex = t.CreateIndex
		group.ModifyIndex = idx
	} else {
		group.CreateIndex = idx
		group.ModifyIndex = idx
	}

	if err := tx.Insert("groups", group); err != nil {
		return fmt.Errorf("failed inserting group: %s", err)
	}

	if err := tx.Insert("index", &IndexEntry{"groups", idx}); err != nil {
		return fmt.Errorf("failed updating index: %s", err)
	}

	return nil
}

// GetGroup is used to get groups.
func (s *Store) GetGroup(id string) (uint64, *structs.Group, error) {
	sp := s.tracer.StartSpan("store: get group")
	sp.LogKV("id", id)
	sp.SetTag("node id", s.nodeID)
	defer sp.Finish()

	tx := s.db.Txn(false)
	defer tx.Abort()
	idx := maxIndexTxn(tx, "groups")

	group, err := tx.First("groups", "id", id)
	if err != nil {
		return 0, nil, fmt.Errorf("group lookup failed: %s", err)
	}
	if group != nil {
		return idx, group.(*structs.Group), nil
	}

	return idx, nil, nil
}

func (s *Store) GetGroups() (uint64, []*structs.Group, error) {
	sp := s.tracer.StartSpan("store: get groups")
	defer sp.Finish()

	tx := s.db.Txn(false)
	defer tx.Abort()

	idx := maxIndexTxn(tx, "groups")
	it, err := tx.Get("groups", "id")
	if err != nil {
		return 0, nil, fmt.Errorf("group lookup failed: %s", err)
	}
	var groups []*structs.Group
	for next := it.Next(); next != nil; next = it.Next() {
		groups = append(groups, next.(*structs.Group))
	}
	return idx, groups, nil
}

// GetGroupsByCoordinator looks up groups with the given coordinator.
func (s *Store) GetGroupsByCoordinator(coordinator int32) (uint64, []*structs.Group, error) {
	sp := s.tracer.StartSpan("store: get groups by coordinator")
	sp.LogKV("coordinator", coordinator)
	sp.SetTag("node id", s.nodeID)
	defer sp.Finish()

	tx := s.db.Txn(false)
	defer tx.Abort()
	idx := maxIndexTxn(tx, "groups")

	it, err := tx.Get("groups", "coordinator", coordinator)
	if err != nil {
		return 0, nil, fmt.Errorf("group lookup failed: %s", err)
	}

	var groups []*structs.Group
	for next := it.Next(); next != nil; next = it.Next() {
		groups = append(groups, next.(*structs.Group))
	}

	return idx, groups, nil
}

// DeleteGroup is used to delete groups.
func (s *Store) DeleteGroup(idx uint64, group string) error {
	sp := s.tracer.StartSpan("store: delete group")
	sp.LogKV("group", group, "group")
	sp.SetTag("node id", s.nodeID)
	defer sp.Finish()

	tx := s.db.Txn(true)
	defer tx.Abort()

	if err := s.deleteGroupTxn(tx, idx, group); err != nil {
		return err
	}

	tx.Commit()
	return nil
}

func (s *Store) deleteGroupTxn(tx *memdb.Txn, idx uint64, id string) error {
	group, err := tx.First("groups", "id", id)
	if err != nil {
		log.Error.Printf("fsm: group lookup error: %s", err)
		return err
	}
	if group == nil {
		return nil
	}
	if err := tx.Delete("groups", group); err != nil {
		log.Error.Printf("fsm: deleting group error: %s", err)
		return err
	}
	if err := tx.Insert("index", &IndexEntry{"groups", idx}); err != nil {
		log.Error.Printf("fsm: updating index error: %s", err)
		return err
	}
	return nil
}

func (s *Store) EnsurePartition(idx uint64, partition *structs.Partition) error {
	sp := s.tracer.StartSpan("store: ensure partition")
	s.vlog(sp, "partition", partition)
	sp.SetTag("node id", s.nodeID)
	defer sp.Finish()

	tx := s.db.Txn(true)
	defer tx.Abort()
	if err := s.ensurePartitionTxn(tx, idx, partition); err != nil {
		return err
	}
	tx.Commit()
	return nil
}

func (s *Store) ensurePartitionTxn(tx *memdb.Txn, idx uint64, partition *structs.Partition) error {
	var t *structs.Partition
	existing, err := tx.First("partitions", "id", partition.Topic, partition.Partition)
	if err != nil {
		return fmt.Errorf("partition lookup failed: %s", err)
	}

	if existing != nil {
		t = existing.(*structs.Partition)
	}

	if t != nil {
		partition.CreateIndex = t.CreateIndex
		partition.ModifyIndex = idx
	} else {
		partition.CreateIndex = idx
		partition.ModifyIndex = idx
	}

	if err := tx.Insert("partitions", partition); err != nil {
		return fmt.Errorf("failed inserting partition: %s", err)
	}

	if err := tx.Insert("index", &IndexEntry{"partitions", idx}); err != nil {
		return fmt.Errorf("failed updating index: %s", err)
	}

	return nil
}

// GetPartition is used to get partitions.
func (s *Store) GetPartition(topic string, id int32) (uint64, *structs.Partition, error) {
	sp := s.tracer.StartSpan("store: get partition")
	sp.LogKV("id", id)
	sp.SetTag("node id", s.nodeID)
	defer sp.Finish()

	tx := s.db.Txn(false)
	defer tx.Abort()
	idx := maxIndexTxn(tx, "partitions")

	partition, err := tx.First("partitions", "id", topic, id)
	if err != nil {
		return 0, nil, fmt.Errorf("partition lookup failed: %s", err)
	}
	if partition != nil {
		return idx, partition.(*structs.Partition), nil
	}

	return idx, nil, nil
}

// PartitionsByLeader is used to return all partitions for the given leader.
func (s *Store) PartitionsByLeader(leader int32) (uint64, []*structs.Partition, error) {
	sp := s.tracer.StartSpan("store: partitions by leader")
	sp.SetTag("leader", leader)
	defer sp.Finish()

	tx := s.db.Txn(false)
	defer tx.Abort()
	idx := maxIndexTxn(tx, "partitions")
	it, err := tx.Get("partitions", "leader", leader)
	if err != nil {
		return 0, nil, err
	}
	var partitions []*structs.Partition
	for next := it.Next(); next != nil; next = it.Next() {
		partitions = append(partitions, next.(*structs.Partition))
	}
	return idx, partitions, nil
}

func (s *Store) GetPartitions() (uint64, []*structs.Partition, error) {
	sp := s.tracer.StartSpan("store: get partitions")
	defer sp.Finish()

	tx := s.db.Txn(false)
	defer tx.Abort()
	idx := maxIndexTxn(tx, "partitions")
	it, err := tx.Get("partitions", "id")
	if err != nil {
		return 0, nil, err
	}
	var partitions []*structs.Partition
	for next := it.Next(); next != nil; next = it.Next() {
		partitions = append(partitions, next.(*structs.Partition))
	}
	return idx, partitions, nil
}

// DeletePartition is used to delete partitions.
func (s *Store) DeletePartition(idx uint64, topic string, partition int32) error {
	sp := s.tracer.StartSpan("store: delete partition")
	sp.LogKV("topic", topic, "partition", partition)
	sp.SetTag("node id", s.nodeID)
	defer sp.Finish()

	tx := s.db.Txn(true)
	defer tx.Abort()

	if err := s.deletePartitionTxn(tx, idx, topic, partition); err != nil {
		return err
	}

	tx.Commit()
	return nil
}

func (s *Store) deletePartitionTxn(tx *memdb.Txn, idx uint64, topic string, id int32) error {
	partition, err := tx.First("partitions", "id", topic, id)
	if err != nil {
		log.Error.Printf("fsm: partition lookup error: %s", err)
		return err
	}
	if partition == nil {
		return nil
	}
	if err := tx.Delete("partitions", partition); err != nil {
		log.Error.Printf("fsm: deleting partition error: %s", err)
		return err
	}
	if err := tx.Insert("index", &IndexEntry{"partitions", idx}); err != nil {
		log.Error.Printf("fsm: updating index error: %s", err)
		return err
	}
	return nil
}

// maxIndex is a helper used to retrieve the highest known index amongst a set of tables in the db.
func (s *Store) maxIndex(tables ...string) uint64 {
	tx := s.db.Txn(false)
	defer tx.Abort()
	return maxIndexTxn(tx, tables...)
}

func (s *Store) Restore() *Restore {
	tx := s.db.Txn(true)
	return &Restore{s, tx}
}

func (s *Store) vlog(span opentracing.Span, k string, i interface{}) {
	if fsmVerboseLogs {
		span.LogKV(k, util.Dump(i))
	}
}

// Restore is used to manage restoring a large amount of data into the state
// store. It works by doing all the restores inside of a single transaction.
type Restore struct {
	store *Store
	tx    *memdb.Txn
}

// Abort abandons the changes made by a restore.
func (s *Restore) Abort() {
	s.tx.Abort()
}

// Commit commits the changes made by a restore.
func (s *Restore) Commit() {
	s.tx.Commit()
}

func (s *Store) Snapshot() *Snapshot {
	tx := s.db.Txn(false)

	var tables []string
	for table := range s.schema.Tables {
		tables = append(tables, table)
	}
	idx := maxIndexTxn(tx, tables...)
	return &Snapshot{s, tx, idx}
}

// IndexEntry keeps a record of the last index per-table.
type IndexEntry struct {
	Key   string
	Value uint64
}

// maxIndexTxn is a helper used to retrieve the highest known index
// amongst a set of tables in the db.
func maxIndexTxn(tx *memdb.Txn, tables ...string) uint64 {
	var lindex uint64
	for _, table := range tables {
		ti, err := tx.First("index", "id", table)
		if err != nil {
			panic(fmt.Sprintf("unknown index: %s err: %s", table, err))
		}
		if idx, ok := ti.(*IndexEntry); ok && idx.Value > lindex {
			lindex = idx.Value
		}
	}
	return lindex
}

// Snapshot is used to provide a point-in-time snapshot. It works by startinga
// read transaction against the whole state store.
type Snapshot struct {
	store     *Store
	tx        *memdb.Txn
	lastIndex uint64
}

// LastIndex returns the last index that affects the snapshotted data.
func (s *Snapshot) LastIndex() uint64 {
	return s.lastIndex
}

// Close performs cleanup of a state snapshot.
func (s *Snapshot) Close() {
	s.tx.Abort()
}

// indexTableSchema returns a new table schema used for tracking various indexes for the Raft log.
func indexTableSchema() *memdb.TableSchema {
	return &memdb.TableSchema{
		Name: "index",
		Indexes: map[string]*memdb.IndexSchema{
			"id": &memdb.IndexSchema{
				Name:         "id",
				AllowMissing: false,
				Unique:       true,
				Indexer: &memdb.StringFieldIndex{
					Field:     "Key",
					Lowercase: true,
				},
			},
		},
	}
}

// nodesTableSchema returns a new table schema used for storing node
// information.
func nodesTableSchema() *memdb.TableSchema {
	return &memdb.TableSchema{
		Name: "nodes",
		Indexes: map[string]*memdb.IndexSchema{
			"id": &memdb.IndexSchema{
				Name:         "id",
				AllowMissing: false,
				Unique:       true,
				Indexer: &IntFieldIndex{
					Field: "Node",
				},
			},
			"meta": &memdb.IndexSchema{
				Name:         "meta",
				AllowMissing: true,
				Unique:       false,
				Indexer: &memdb.StringMapFieldIndex{
					Field:     "Meta",
					Lowercase: false,
				},
			},
		},
	}
}

// topicsTableSchema returns a new table schema used for storing topic
// information.
func topicsTableSchema() *memdb.TableSchema {
	return &memdb.TableSchema{
		Name: "topics",
		Indexes: map[string]*memdb.IndexSchema{
			"id": &memdb.IndexSchema{
				Name:         "id",
				AllowMissing: false,
				Unique:       true,
				Indexer: &memdb.StringFieldIndex{
					Field:     "Topic",
					Lowercase: true,
				},
			},
		},
	}
}

// partitionsTableSchema returns a new table schema used for storing partition
// information.
// TODO: may want to index the topic field.
func partitionsTableSchema() *memdb.TableSchema {
	return &memdb.TableSchema{
		Name: "partitions",
		Indexes: map[string]*memdb.IndexSchema{
			"id": &memdb.IndexSchema{
				Name:   "id",
				Unique: true,
				Indexer: &memdb.CompoundIndex{
					Indexes: []memdb.Indexer{
						&memdb.StringFieldIndex{Field: "Topic"},
						&IntFieldIndex{Field: "Partition"},
					},
				},
			},
			"partition": &memdb.IndexSchema{
				Name:         "partition",
				AllowMissing: false,
				Unique:       false,
				Indexer: &IntFieldIndex{
					Field: "Partition",
				},
			},
			"topic": &memdb.IndexSchema{
				Name:         "topic",
				AllowMissing: false,
				Unique:       false,
				Indexer: &memdb.StringFieldIndex{
					Field: "Topic",
				},
			},
			"leader": &memdb.IndexSchema{
				Name:         "leader",
				AllowMissing: false,
				Unique:       false,
				Indexer: &IntFieldIndex{
					Field: "Leader",
				},
			},
		},
	}
}

func groupTableSchema() *memdb.TableSchema {
	return &memdb.TableSchema{
		Name: "groups",
		Indexes: map[string]*memdb.IndexSchema{
			"id": &memdb.IndexSchema{
				Name:         "id",
				AllowMissing: false,
				Unique:       true,
				Indexer: &memdb.StringFieldIndex{
					Field:     "Group",
					Lowercase: true,
				},
			},
			"coordinator": &memdb.IndexSchema{
				Name: "coordinator",
				Indexer: &IntFieldIndex{
					Field: "Coordinator",
				},
			},
		},
	}
}

func init() {
	registerSchema(indexTableSchema)
	registerSchema(nodesTableSchema)
	registerSchema(topicsTableSchema)
	registerSchema(partitionsTableSchema)
	registerSchema(groupTableSchema)

	e := os.Getenv("JOCKODEBUG")
	if strings.Contains(e, "fsm=1") {
		fsmVerboseLogs = true
	}
}
