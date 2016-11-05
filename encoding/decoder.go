package encoding

import (
	"errors"
	"math"
)

var ErrInsufficientData = errors.New("kafka: insufficient data to decode packet, more bytes expected")
var ErrInvalidStringLength = errors.New("kafka: invalid string length")
var ErrInvalidArrayLength = errors.New("kafka: invalid array length")
var ErrInvalidByteSliceLength = errors.New("invalid byteslice length")

type Decoder struct {
	b   []byte
	off int
}

func NewDecoder(b []byte) *Decoder {
	return &Decoder{
		b: b,
	}
}

func (d *Decoder) Int16() (int16, error) {
	tmp := int16(Enc.Uint16(d.b[d.off:]))
	d.off += 2
	return tmp, nil
}

func (d *Decoder) Int32() (int32, error) {
	if d.remaining() < 4 {
		d.off = len(d.b)
		return -1, ErrInsufficientData
	}
	tmp := int32(Enc.Uint32(d.b[d.off:]))
	d.off += 4
	return tmp, nil
}

func (d *Decoder) Int64() (int64, error) {
	if d.remaining() < 8 {
		d.off = len(d.b)
		return -1, ErrInsufficientData
	}
	tmp := int64(Enc.Uint64(d.b[d.off:]))
	d.off += 8
	return tmp, nil
}

func (d *Decoder) ArrayLength() (int, error) {
	if d.remaining() < 4 {
		d.off = len(d.b)
		return -1, ErrInsufficientData
	}
	tmp := int(Enc.Uint32(d.b[d.off:]))
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

func (d *Decoder) Bytes() ([]byte, error) {
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

func (d *Decoder) String() (string, error) {
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

func (d *Decoder) Int32Array() ([]int32, error) {
	if d.remaining() < 4 {
		d.off = len(d.b)
		return nil, ErrInsufficientData
	}
	n := int(Enc.Uint32(d.b[d.off:]))
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
		ret[i] = int32(Enc.Uint32(d.b[d.off:]))
		d.off += 4
	}
	return ret, nil
}

func (d *Decoder) Int64Array() ([]int64, error) {
	if d.remaining() < 4 {
		d.off = len(d.b)
		return nil, ErrInsufficientData
	}
	n := int(Enc.Uint32(d.b[d.off:]))
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
		ret[i] = int64(Enc.Uint64(d.b[d.off:]))
		d.off += 8
	}
	return ret, nil
}

func (d *Decoder) StringArray() ([]string, error) {
	if d.remaining() < 4 {
		d.off = len(d.b)
		return nil, ErrInsufficientData
	}
	n := int(Enc.Uint32(d.b[d.off:]))
	d.off += 4

	if n == 0 {
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

func (d *Decoder) remaining() int {
	return len(d.b) - d.off
}
