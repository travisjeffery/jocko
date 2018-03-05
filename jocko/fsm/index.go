package fsm

import (
	"encoding/binary"
	"fmt"
	"reflect"
)

// IntFieldIndex is used to extract a int field from an object using
// reflection and builds an index on that field.
type IntFieldIndex struct {
	Field string
}

func (u *IntFieldIndex) FromObject(obj interface{}) (bool, []byte, error) {
	v := reflect.ValueOf(obj)
	v = reflect.Indirect(v) // Dereference the pointer if any

	fv := v.FieldByName(u.Field)
	if !fv.IsValid() {
		return false, nil,
			fmt.Errorf("field '%s' for %#v is invalid", u.Field, obj)
	}

	// Check the type
	k := fv.Kind()
	size, ok := IsIntType(k)
	if !ok {
		return false, nil, fmt.Errorf("field %q is of type %v; want a int", u.Field, k)
	}

	// Get the value and encode it
	val := fv.Int()
	buf := make([]byte, size)
	binary.PutVarint(buf, val)

	return true, buf, nil
}

func (u *IntFieldIndex) FromArgs(args ...interface{}) ([]byte, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("must provide only a single argument")
	}

	v := reflect.ValueOf(args[0])
	if !v.IsValid() {
		return nil, fmt.Errorf("%#v is invalid", args[0])
	}

	k := v.Kind()
	size, ok := IsIntType(k)
	if !ok {
		return nil, fmt.Errorf("arg is of type %v; want a int", k)
	}

	val := v.Int()
	buf := make([]byte, size)
	binary.PutVarint(buf, val)

	return buf, nil
}

// IsIntType returns whether the passed type is a type of int and the number
// of bytes needed to encode the type.
func IsIntType(k reflect.Kind) (size int, okay bool) {
	switch k {
	case reflect.Int:
		return binary.MaxVarintLen64, true
	case reflect.Int8:
		return 2, true
	case reflect.Int16:
		return binary.MaxVarintLen16, true
	case reflect.Int32:
		return binary.MaxVarintLen32, true
	case reflect.Int64:
		return binary.MaxVarintLen64, true
	default:
		return 0, false
	}
}
