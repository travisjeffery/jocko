package state

import (
	"fmt"

	memdb "github.com/hashicorp/go-memdb"
	"github.com/pkg/errors"
)

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

	// abandonCh is used to signal watchers that this state store has been
	// abandoned (usually during a restore). This is only ever closed.
	abandonCh chan struct{}
}

func NewStore() (*Store, error) {
	schema := storeSchema()
	db, err := memdb.NewMemDB(schema)
	if err != nil {
		return nil, errors.Wrap(err, "failed setting up state store")
	}
	s := &Store{
		schema:    schema,
		db:        db,
		abandonCh: make(chan struct{}),
	}
	return s, nil
}

func storeSchema() *memdb.DBSchema {
	db := &memdb.DBSchema{Tables: make(map[string]*memdb.TableSchema)}
	for _, fn := range schemas {
		schema := fn()
		if _, ok := db.Tables[schema.Name]; ok {
			panic(fmt.Sprintf("duplicate table names: %s", schema.Name))
		}
		db.Tables[schema.Name] = schema
	}
	return db
}
