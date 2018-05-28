package protocol

import (
	"math"
)

type PacketEncoder interface {
	PutBool(in bool)
	PutInt8(in int8)
	PutInt16(in int16)
	PutInt32(in int32)
	PutInt64(in int64)
	PutArrayLength(in int) error
	PutRawBytes(in []byte) error
	PutBytes(in []byte) error
	PutString(in string) error
	PutNullableString(in *string) error
	PutStringArray(in []string) error
	PutInt32Array(in []int32) error
	PutInt64Array(in []int64) error
	Push(pe PushEncoder)
	Pop()
}

type PushEncoder interface {
	SaveOffset(in int)
	ReserveSize() int
	Fill(curOffset int, buf []byte) error
}

type Encoder interface {
	Encode(e PacketEncoder) error
}

func Encode(e Encoder) ([]byte, error) {
	lenEnc := new(LenEncoder)
	err := e.Encode(lenEnc)
	if err != nil {
		return nil, err
	}

	b := make([]byte, lenEnc.Length)
	byteEnc := NewByteEncoder(b)
	err = e.Encode(byteEnc)
	if err != nil {
		return nil, err
	}

	return b, nil
}

type LenEncoder struct {
	Length int
	stack  []int
}

func (e *LenEncoder) PutBool(in bool) {
	e.Length++
}

func (e *LenEncoder) PutInt8(in int8) {
	e.Length++
}

func (e *LenEncoder) PutInt16(in int16) {
	e.Length += 2
}

func (e *LenEncoder) PutInt32(in int32) {
	e.Length += 4
}

func (e *LenEncoder) PutInt64(in int64) {
	e.Length += 8
}

func (e *LenEncoder) PutArrayLength(in int) error {
	if in > math.MaxInt32 {
		return ErrInvalidArrayLength
	}
	e.Length += 4
	return nil
}

// arrays

func (e *LenEncoder) PutBytes(in []byte) error {
	e.Length += 4
	if in == nil {
		return nil
	}
	return e.PutRawBytes(in)
}

func (e *LenEncoder) PutRawBytes(in []byte) error {
	if len(in) > math.MaxInt32 {
		return ErrInvalidByteSliceLength
	}
	e.Length += len(in)
	return nil
}

func (e *LenEncoder) PutString(in string) error {
	e.Length += 2
	if len(in) > math.MaxInt16 {
		return ErrInvalidStringLength
	}
	e.Length += len(in)
	return nil
}

func (e *LenEncoder) PutNullableString(in *string) error {
	if in == nil {
		e.Length += 2
		return nil
	}
	return e.PutString(*in)
}

func (e *LenEncoder) PutStringArray(in []string) error {
	err := e.PutArrayLength(len(in))
	if err != nil {
		return err
	}

	for _, str := range in {
		if err := e.PutString(str); err != nil {
			return err
		}
	}

	return nil
}

func (e *LenEncoder) PutInt32Array(in []int32) error {
	err := e.PutArrayLength(len(in))
	if err != nil {
		return err
	}
	e.Length += 4 * len(in)
	return nil
}

func (e *LenEncoder) PutInt64Array(in []int64) error {
	err := e.PutArrayLength(len(in))
	if err != nil {
		return err
	}
	e.Length += 8 * len(in)
	return nil
}

func (e *LenEncoder) Push(pe PushEncoder) {
	e.Length += pe.ReserveSize()
}

func (e *LenEncoder) Pop() {}

type ByteEncoder struct {
	b     []byte
	off   int
	stack []PushEncoder
}

func (b *ByteEncoder) Bytes() []byte {
	return b.b
}

func NewByteEncoder(b []byte) *ByteEncoder {
	return &ByteEncoder{b: b}
}

func (e *ByteEncoder) PutBool(in bool) {
	if in {
		e.b[e.off] = byte(int8(1))
	}
	e.off++
}

func (e *ByteEncoder) PutInt8(in int8) {
	e.b[e.off] = byte(in)
	e.off++
}

func (e *ByteEncoder) PutInt16(in int16) {
	Encoding.PutUint16(e.b[e.off:], uint16(in))
	e.off += 2
}

func (e *ByteEncoder) PutInt32(in int32) {
	Encoding.PutUint32(e.b[e.off:], uint32(in))
	e.off += 4
}

func (e *ByteEncoder) PutInt64(in int64) {
	Encoding.PutUint64(e.b[e.off:], uint64(in))
	e.off += 8
}

func (e *ByteEncoder) PutArrayLength(in int) error {
	e.PutInt32(int32(in))
	return nil
}

func (e *ByteEncoder) PutRawBytes(in []byte) error {
	copy(e.b[e.off:], in)
	e.off += len(in)
	return nil
}

func (e *ByteEncoder) PutBytes(in []byte) error {
	if in == nil {
		e.PutInt32(-1)
		return nil
	}
	e.PutInt32(int32(len(in)))
	return e.PutRawBytes(in)
}

func (e *ByteEncoder) PutString(in string) error {
	e.PutInt16(int16(len(in)))
	copy(e.b[e.off:], in)
	e.off += len(in)
	return nil
}

func (e *ByteEncoder) PutNullableString(in *string) error {
	if in == nil {
		e.PutInt16(-1)
		return nil
	}
	return e.PutString(*in)
}

func (e *ByteEncoder) PutStringArray(in []string) error {
	err := e.PutArrayLength(len(in))
	if err != nil {
		return err
	}

	for _, val := range in {
		if err := e.PutString(val); err != nil {
			return err
		}
	}

	return nil
}

func (e *ByteEncoder) PutInt32Array(in []int32) error {
	err := e.PutArrayLength(len(in))
	if err != nil {
		return err
	}
	for _, val := range in {
		e.PutInt32(val)
	}
	return nil
}

func (e *ByteEncoder) PutInt64Array(in []int64) error {
	err := e.PutArrayLength(len(in))
	if err != nil {
		return err
	}
	for _, val := range in {
		e.PutInt64(val)
	}
	return nil
}

func (e *ByteEncoder) Push(pe PushEncoder) {
	pe.SaveOffset(e.off)
	e.off += pe.ReserveSize()
	e.stack = append(e.stack, pe)
}

func (e *ByteEncoder) Pop() {
	// this is go's ugly pop pattern (the inverse of append)
	pe := e.stack[len(e.stack)-1]
	e.stack = e.stack[:len(e.stack)-1]
	pe.Fill(e.off, e.b)
}
