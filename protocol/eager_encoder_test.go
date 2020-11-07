package protocol

import (
	"log"
	"testing"

	"github.com/stretchr/testify/require"
)

type Fake struct {
	b   bool
	arr []int32
}

func (e *Fake) Encode(pe PacketEncoder) error {
	pe.PutBool(e.b)
	return pe.PutInt32Array(e.arr)
}
func TestEagerEncode(t *testing.T) {
	e := NewEagerEncoder()
	e.PutBool(true)
	require.Equal(t, e.Bytes(), []byte{1})
	arr := []int32{1, 2, 3}
	e.PutInt32Array(arr)
	expected := []byte{1}
	arrLenAndData := make([]byte, 4*(1+len(arr)))
	Encoding.PutUint32(arrLenAndData, uint32(len(arr)))
	Encoding.PutUint32(arrLenAndData[4:], uint32(arr[0]))
	Encoding.PutUint32(arrLenAndData[8:], uint32(arr[1]))
	Encoding.PutUint32(arrLenAndData[12:], uint32(arr[2]))
	expected = append(expected, arrLenAndData...)
	require.Equal(t, e.Bytes(), expected)
	ebytes := e.Bytes()

	b2, _ := Encode(&Fake{b: true, arr: []int32{1, 2, 3}})
	require.Equal(t, e.Bytes(), b2)
	e2 := NewEagerEncoder()
	(&Fake{b: true, arr: []int32{1, 2, 3}}).Encode(e2)
	require.Equal(t, e2.Bytes(), b2)
	ebytes = e2.Bytes()
	log.Printf("bytes::::::::: %p %p", &ebytes[0], &expected[0])
}
