package protocol

import (
	"errors"
	"math"
)

var ErrInsufficientData = errors.New("kafka: insufficient data to decode packet, more bytes expected")
var ErrInvalidStringLength = errors.New("kafka: invalid string length")
var ErrInvalidArrayLength = errors.New("kafka: invalid array length")
var ErrInvalidByteSliceLength = errors.New("invalid byteslice length")

type PacketDecoder interface {
	Bool() (bool, error)
	Int8() (int8, error)
	Int16() (int16, error)
	Int32() (int32, error)
	Int64() (int64, error)
	ArrayLength() (int, error)
	Bytes() ([]byte, error)
	String() (string, error)
	NullableString() (*string, error)
	Int32Array() ([]int32, error)
	Int64Array() ([]int64, error)
	StringArray() ([]string, error)
	Push(pd PushDecoder) error
	Pop() error
	remaining() int
}

type Decoder interface {
	Decode(d PacketDecoder) error
}

type VersionedDecoder interface {
	Decode(d PacketDecoder, version int16) error
}

type PushDecoder interface {
	SaveOffset(in int)
	ReserveSize() int
	Fill(curOffset int, buf []byte) error
	Check(curOffset int, buf []byte) error
}

func Decode(b []byte, in VersionedDecoder, version int16) error {
	d := NewDecoder(b)
	return in.Decode(d, version)
}

type ByteDecoder struct {
	b     []byte
	off   int
	stack []PushDecoder
}

func (d *ByteDecoder) Offset() int {
	return d.off
}

func NewDecoder(b []byte) *ByteDecoder {
	return &ByteDecoder{
		b: b,
	}
}

func (d *ByteDecoder) Bool() (bool, error) {
	i, err := d.Int8()
	if err != nil {
		return false, err
	}
	return i == 1, nil
}

func (d *ByteDecoder) Int8() (int8, error) {
	tmp := int8(d.b[d.off])
	d.off++
	return tmp, nil
}

func (d *ByteDecoder) Int16() (int16, error) {
	tmp := int16(Encoding.Uint16(d.b[d.off:]))
	d.off += 2
	return tmp, nil
}

func (d *ByteDecoder) Int32() (int32, error) {
	if d.remaining() < 4 {
		d.off = len(d.b)
		return -1, ErrInsufficientData
	}
	tmp := int32(Encoding.Uint32(d.b[d.off:]))
	d.off += 4
	return tmp, nil
}

func (d *ByteDecoder) Int64() (int64, error) {
	if d.remaining() < 8 {
		d.off = len(d.b)
		return -1, ErrInsufficientData
	}
	tmp := int64(Encoding.Uint64(d.b[d.off:]))
	d.off += 8
	return tmp, nil
}

func (d *ByteDecoder) ArrayLength() (int, error) {
	if d.remaining() < 4 {
		d.off = len(d.b)
		return -1, ErrInsufficientData
	}
	tmp := int(Encoding.Uint32(d.b[d.off:]))
	d.off += 4
	if tmp > d.remaining() {
		d.off = len(d.b)
		return -1, ErrInsufficientData
	} else if tmp > 2*math.MaxUint16 {
		return -1, ErrInvalidArrayLength
	}
	return tmp, nil
}

// collections

func (d *ByteDecoder) Bytes() ([]byte, error) {
	tmp, err := d.Int32()

	if err != nil {
		return nil, err
	}

	n := int(tmp)

	switch {
	case n < -1:
		return nil, ErrInvalidByteSliceLength
	case n == -1:
		return nil, nil
	case n == 0:
		return make([]byte, 0), nil
	case n > d.remaining():
		d.off = len(d.b)
		return nil, ErrInsufficientData
	}

	tmpStr := d.b[d.off : d.off+n]
	d.off += n
	return tmpStr, nil
}

func (d *ByteDecoder) String() (string, error) {
	tmp, err := d.Int16()

	if err != nil {
		return "", err
	}

	n := int(tmp)

	switch {
	case n < -1:
		return "", ErrInvalidStringLength
	case n == -1:
		return "", nil
	case n == 0:
		return "", nil
	case n > d.remaining():
		d.off = len(d.b)
		return "", ErrInsufficientData
	}

	tmpStr := string(d.b[d.off : d.off+n])
	d.off += n
	return tmpStr, nil
}

func (d *ByteDecoder) stringLength() (int, error) {
	l, err := d.Int16()
	if err != nil {
		return 0, err
	}
	n := int(l)
	switch {
	case n < -1:
		return 0, ErrInvalidStringLength
	case n > d.remaining():
		d.off = len(d.b)
		return 0, ErrInsufficientData
	}
	return n, nil
}

func (d *ByteDecoder) NullableString() (*string, error) {
	n, err := d.stringLength()
	if err != nil || n == -1 {
		return nil, err
	}
	tmpStr := string(d.b[d.off : d.off+n])
	d.off += n
	return &tmpStr, nil
}

func (d *ByteDecoder) Int32Array() ([]int32, error) {
	if d.remaining() < 4 {
		d.off = len(d.b)
		return nil, ErrInsufficientData
	}
	n := int(Encoding.Uint32(d.b[d.off:]))
	d.off += 4

	if d.remaining() < 4*n {
		d.off = len(d.b)
		return nil, ErrInsufficientData
	}

	if n == 0 {
		return nil, nil
	}

	if n < 0 {
		return nil, ErrInvalidArrayLength
	}

	ret := make([]int32, n)
	for i := range ret {
		ret[i] = int32(Encoding.Uint32(d.b[d.off:]))
		d.off += 4
	}
	return ret, nil
}

func (d *ByteDecoder) Int64Array() ([]int64, error) {
	if d.remaining() < 4 {
		d.off = len(d.b)
		return nil, ErrInsufficientData
	}
	n := int(Encoding.Uint32(d.b[d.off:]))
	d.off += 4

	if d.remaining() < 8*n {
		d.off = len(d.b)
		return nil, ErrInsufficientData
	}

	if n == 0 {
		return nil, nil
	}

	if n < 0 {
		return nil, ErrInvalidArrayLength
	}

	ret := make([]int64, n)
	for i := range ret {
		ret[i] = int64(Encoding.Uint64(d.b[d.off:]))
		d.off += 8
	}
	return ret, nil
}

func (d *ByteDecoder) StringArray() ([]string, error) {
	if d.remaining() < 4 {
		d.off = len(d.b)
		return nil, ErrInsufficientData
	}
	n := int32(Encoding.Uint32(d.b[d.off:]))
	d.off += 4

	if n == 0 || n == -1 {
		return nil, nil
	}

	if n < 0 {
		return nil, ErrInvalidArrayLength
	}

	ret := make([]string, n)
	for i := range ret {
		if str, err := d.String(); err != nil {
			return nil, err
		} else {
			ret[i] = str
		}
	}
	return ret, nil
}

func (d *ByteDecoder) Push(pd PushDecoder) error {
	pd.SaveOffset(d.off)
	reserved := pd.ReserveSize()
	if d.remaining() < reserved {
		d.off = len(d.b)
		return ErrInsufficientData
	}
	d.stack = append(d.stack, pd)
	d.off += reserved
	return nil
}

func (d *ByteDecoder) Pop() error {
	pd := d.stack[len(d.stack)-1]
	d.stack = d.stack[:len(d.stack)-1]
	return pd.Check(d.off, d.b)
}

func (d *ByteDecoder) remaining() int {
	return len(d.b) - d.off
}
