package protocol

import (
	"encoding/binary"
	"errors"
	"math"
)

var ErrInsufficientData = errors.New("kafka: insufficient data to decode packet, more bytes expected")
var ErrInvalidStringLength = errors.New("kafka: invalid string length")
var ErrInvalidArrayLength = errors.New("kafka: invalid array length")
var ErrInvalidByteSliceLength = errors.New("invalid byteslice length")

type PacketDecoder interface {
	FlexVar()
	Bool() (bool, error)
	Int8() (int8, error)
	Int16() (int16, error)
	Int32() (int32, error)
	Int64() (int64, error)
	Varint() (int64, error)
	Uvarint() (uint64, error)
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
	b       []byte
	off     int
	stack   []PushDecoder
	flexVar bool
}

func (d *ByteDecoder) FlexVar() {
	d.flexVar = true
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

func (d *ByteDecoder) Varint() (int64, error) {
	x, n := binary.Varint(d.b[d.off:])
	if n <= 0 {
		return -1, ErrInsufficientData
	}
	d.off += n
	return x, nil
}

func (d *ByteDecoder) Uvarint() (uint64, error) {
	x, n := binary.Uvarint(d.b[d.off:])
	if n <= 0 {
		return 0, ErrInsufficientData
	}
	d.off += n
	return x, nil
}

func (d *ByteDecoder) ArrayLength() (int, error) {
	var tmp int
	if d.flexVar {
		len, err := d.Uvarint()
		if err != nil {
			return -1, err
		}
		tmp = int(len)
	} else {
		len, err := d.Int32()
		if err != nil {
			return -1, err
		}
		tmp = int(len)
	}
	if tmp > 2*math.MaxUint16 {
		return -1, ErrInvalidArrayLength
	}
	return tmp, nil
}

// collections

func (d *ByteDecoder) Bytes() ([]byte, error) {
	n, err := d.ArrayLength()
	if err != nil {
		return nil, err
	}
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
	n, err := d.stringLength()
	if err != nil {
		return "", err
	}
	switch {
	case n <= -1:
		return "", ErrInvalidArrayLength
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
	var tmp int
	if d.flexVar {
		if len, err := d.Uvarint(); err != nil {
			return 0, err
		} else {
			tmp = int(len) - 1
		}
	} else {
		if len, err := d.Int16(); err != nil {
			return 0, err
		} else {
			tmp = int(len)
		}
	}
	if tmp > 2*math.MaxUint16 {
		return -1, ErrInvalidArrayLength
	}
	return tmp, nil
}

func (d *ByteDecoder) NullableString() (*string, error) {
	n, err := d.stringLength()
	if err != nil {
		return nil, err
	}
	var str string
	switch {
	case n < -1:
		return nil, ErrInvalidArrayLength
	case n == -1:
		return nil, nil
	case n == 0:
		return &str, nil
	case n > d.remaining():
		d.off = len(d.b)
		return nil, ErrInsufficientData
	}
	tmpStr := string(d.b[d.off : d.off+n])
	d.off += n
	return &tmpStr, nil
}

func (d *ByteDecoder) Int32Array() ([]int32, error) {
	n, err := d.ArrayLength()
	if err != nil {
		return nil, err
	}

	switch {
	case n < -1:
		return nil, ErrInvalidArrayLength
	case n == -1:
		return nil, nil
	case n == 0:
		return []int32{}, nil
	case d.remaining() < 4*n:
		d.off = len(d.b)
		return nil, ErrInsufficientData
	}

	ret := make([]int32, n)
	for i := range ret {
		if ret[i], err = d.Int32(); err != nil {
			return nil, err
		}
	}
	return ret, nil
}

func (d *ByteDecoder) Int64Array() ([]int64, error) {
	n, err := d.ArrayLength()
	if err != nil {
		return nil, err
	}

	switch {
	case n < -1:
		return nil, ErrInvalidArrayLength
	case n == -1:
		return nil, nil
	case n == 0:
		return []int64{}, nil
	case d.remaining() < 8*n:
		d.off = len(d.b)
		return nil, ErrInsufficientData
	}
	ret := make([]int64, n)
	for i := range ret {
		if ret[i], err = d.Int64(); err != nil {
			return nil, err
		}
	}
	return ret, nil
}

func (d *ByteDecoder) StringArray() ([]string, error) {
	n, err := d.ArrayLength()
	if err != nil {
		return nil, err
	}

	switch {
	case n < -1:
		return nil, ErrInvalidArrayLength
	case n == -1:
		return nil, nil
	case n == 0:
		return []string{}, nil
	}

	ret := make([]string, n)
	for i := range ret {
		if ret[i], err = d.String(); err != nil {
			return nil, err
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
