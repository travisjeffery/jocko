package fsm

import (
	"fmt"
	"io"
	"sync"

	memdb "github.com/hashicorp/go-memdb"
	"github.com/hashicorp/raft"
	"github.com/travisjeffery/jocko/broker/structs"
	"github.com/travisjeffery/jocko/log"
	"github.com/ugorji/go/codec"
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

type FSM struct {
	logger    log.Logger
	path      string
	apply     map[structs.MessageType]command
	stateLock sync.RWMutex
	state     *Store
}

func New(logger log.Logger) (*FSM, error) {
	store, err := NewStore(logger)
	if err != nil {
		return nil, err
	}
	fsm := &FSM{
		apply:  make(map[structs.MessageType]command),
		logger: logger,
		state:  store,
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

func (c *FSM) Apply(log *raft.Log) interface{} {
	buf := log.Data
	msgType := structs.MessageType(buf[0])
	if fn := c.apply[msgType]; fn != nil {
		return fn(buf[1:], log.Index)
	}
	return nil
}

func (c *FSM) Restore(old io.ReadCloser) error {
	defer old.Close()

	newState, err := NewStore(c.logger)
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
	logger log.Logger
	schema *memdb.DBSchema
	db     *memdb.MemDB
	// abandonCh is used to signal watchers this store has been abandoned
	// (usually during a restore).
	abandonCh chan struct{}
}

func NewStore(logger log.Logger) (*Store, error) {
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
		logger:    logger,
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
func (s *Store) GetNode(id string) (uint64, *structs.Node, error) {
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

// EnsureNode is used to upsert nodes.
func (s *Store) EnsureNode(idx uint64, node *structs.Node) error {
	tx := s.db.Txn(true)
	defer tx.Abort()

	if err := s.ensureNodeTxn(tx, idx, node); err != nil {
		return err
	}

	tx.Commit()
	return nil
}

// DeleteNode is used to delete nodes.
func (s *Store) DeleteNode(idx uint64, id string) error {
	tx := s.db.Txn(true)
	defer tx.Abort()

	if err := s.deleteNodeTxn(tx, idx, id); err != nil {
		return err
	}

	tx.Commit()
	return nil
}

func (s *Store) deleteNodeTxn(tx *memdb.Txn, idx uint64, id string) error {
	node, err := tx.First("nodes", "id", id)
	if err != nil {
		s.logger.Error("failed node lookup", log.Error("error", err))
		return err
	}
	if node == nil {
		return nil
	}
	// todo: delete anything attached to the node
	if err := tx.Delete("nodes", node); err != nil {
		s.logger.Error("failed deleting node", log.Error("error", err))
		return err
	}
	// update the index
	if err := tx.Insert("index", &IndexEntry{"nodes", idx}); err != nil {
		s.logger.Error("failed deleting index", log.Error("error", err))
		return err
	}
	return nil
}

func (s *Store) EnsureRegistration(idx uint64, req *structs.RegisterRequest) error {
	tx := s.db.Txn(true)
	defer tx.Abort()

	if err := s.ensureRegistration(tx, idx, req); err != nil {
		return err
	}

	tx.Commit()
	return nil
}

func (s *Store) ensureRegistration(tx *memdb.Txn, idx uint64, req *structs.RegisterRequest) error {
	node := &structs.Node{
		Node: req.Node,
	}

	existing, err := tx.First("nodes", "id", node.Node)
	if err != nil {
		s.logger.Error("node lookup failed", log.Error("error", err))
		return err
	}

	if existing == nil {
		if err := s.ensureNodeTxn(tx, idx, node); err != nil {
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

// SNapshot is used to provide a point-in-time snapshot. It works by startinga
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
				Indexer: &memdb.StringFieldIndex{
					Field:     "Node",
					Lowercase: true,
				},
			},
			"uuid": &memdb.IndexSchema{
				Name:         "uuid",
				AllowMissing: true,
				Unique:       true,
				Indexer: &memdb.UUIDFieldIndex{
					Field: "ID",
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

// servicesTableSchema returns a new table schema used to store information
// about services.
func servicesTableSchema() *memdb.TableSchema {
	return &memdb.TableSchema{
		Name: "services",
		Indexes: map[string]*memdb.IndexSchema{
			"id": &memdb.IndexSchema{
				Name:         "id",
				AllowMissing: false,
				Unique:       true,
				Indexer: &memdb.CompoundIndex{
					Indexes: []memdb.Indexer{
						&memdb.StringFieldIndex{
							Field:     "Node",
							Lowercase: true,
						},
						&memdb.StringFieldIndex{
							Field:     "ServiceID",
							Lowercase: true,
						},
					},
				},
			},
			"node": &memdb.IndexSchema{
				Name:         "node",
				AllowMissing: false,
				Unique:       false,
				Indexer: &memdb.StringFieldIndex{
					Field:     "Node",
					Lowercase: true,
				},
			},
			"service": &memdb.IndexSchema{
				Name:         "service",
				AllowMissing: true,
				Unique:       false,
				Indexer: &memdb.StringFieldIndex{
					Field:     "ServiceName",
					Lowercase: true,
				},
			},
		},
	}
}

// checksTableSchema returns a new table schema used for storing and indexing
// health check information. Health checks have a number of different attributes
// we want to filter by, so this table is a bit more complex.
func checksTableSchema() *memdb.TableSchema {
	return &memdb.TableSchema{
		Name: "checks",
		Indexes: map[string]*memdb.IndexSchema{
			"id": &memdb.IndexSchema{
				Name:         "id",
				AllowMissing: false,
				Unique:       true,
				Indexer: &memdb.CompoundIndex{
					Indexes: []memdb.Indexer{
						&memdb.StringFieldIndex{
							Field:     "Node",
							Lowercase: true,
						},
						&memdb.StringFieldIndex{
							Field:     "CheckID",
							Lowercase: true,
						},
					},
				},
			},
			"status": &memdb.IndexSchema{
				Name:         "status",
				AllowMissing: false,
				Unique:       false,
				Indexer: &memdb.StringFieldIndex{
					Field:     "Status",
					Lowercase: false,
				},
			},
			"service": &memdb.IndexSchema{
				Name:         "service",
				AllowMissing: true,
				Unique:       false,
				Indexer: &memdb.StringFieldIndex{
					Field:     "ServiceName",
					Lowercase: true,
				},
			},
			"node": &memdb.IndexSchema{
				Name:         "node",
				AllowMissing: true,
				Unique:       false,
				Indexer: &memdb.StringFieldIndex{
					Field:     "Node",
					Lowercase: true,
				},
			},
			"node_service_check": &memdb.IndexSchema{
				Name:         "node_service_check",
				AllowMissing: true,
				Unique:       false,
				Indexer: &memdb.CompoundIndex{
					Indexes: []memdb.Indexer{
						&memdb.StringFieldIndex{
							Field:     "Node",
							Lowercase: true,
						},
						&memdb.FieldSetIndex{
							Field: "ServiceID",
						},
					},
				},
			},
			"node_service": &memdb.IndexSchema{
				Name:         "node_service",
				AllowMissing: true,
				Unique:       false,
				Indexer: &memdb.CompoundIndex{
					Indexes: []memdb.Indexer{
						&memdb.StringFieldIndex{
							Field:     "Node",
							Lowercase: true,
						},
						&memdb.StringFieldIndex{
							Field:     "ServiceID",
							Lowercase: true,
						},
					},
				},
			},
		},
	}
}

func init() {
	registerSchema(indexTableSchema)
	registerSchema(nodesTableSchema)
	registerSchema(servicesTableSchema)
	registerSchema(checksTableSchema)
}
