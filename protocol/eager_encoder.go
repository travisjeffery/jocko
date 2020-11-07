package protocol

type EagerEncoder struct {
	encoder ByteEncoder
}

func NewEagerEncoder() *EagerEncoder {
	return &EagerEncoder{}
}
func (e *EagerEncoder) Bytes() []byte {
	return e.encoder.Bytes()
}

func (e *EagerEncoder) checkBuf(i int) {
	if cap(e.encoder.b) >= len(e.encoder.b)+i {
		e.encoder.b = e.encoder.b[:len(e.encoder.b)+i]
	} else {
		newb := make([]byte, len(e.encoder.b)+i)
		copy(newb, e.encoder.b)
		e.encoder.b = newb
	}
}
func (e *EagerEncoder) PutBool(in bool) {
	e.checkBuf(1)
	e.encoder.PutBool(in)
}
func (e *EagerEncoder) PutInt8(in int8) {
	e.checkBuf(1)
	e.encoder.PutInt8(in)
}
func (e *EagerEncoder) PutInt16(in int16) {
	e.checkBuf(2)
	e.encoder.PutInt16(in)
}
func (e *EagerEncoder) PutInt32(in int32) {
	e.checkBuf(4)
	e.encoder.PutInt32(in)
}
func (e *EagerEncoder) PutInt64(in int64) {
	e.checkBuf(8)
	e.encoder.PutInt64(in)
}
func (e *EagerEncoder) PutArrayLength(in int) error {
	e.checkBuf(4)
	return e.encoder.PutArrayLength(in)
}
func (e *EagerEncoder) PutRawBytes(in []byte) error {
	e.checkBuf(len(in))
	return e.encoder.PutRawBytes(in)
}
func (e *EagerEncoder) PutBytes(in []byte) error {
	e.checkBuf(4 + len(in))
	return e.encoder.PutBytes(in)
}
func (e *EagerEncoder) PutNotNullBytes(in []byte) error {
	e.checkBuf(4 + len(in))
	return e.encoder.PutNotNullBytes(in)
}
func (e *EagerEncoder) PutString(in string) error {
	e.checkBuf(2 + len(in))
	return e.encoder.PutString(in)
}
func (e *EagerEncoder) PutNullableString(in *string) error {
	e.checkBuf(2 + len(*in))
	return e.encoder.PutNullableString(in)
}
func (e *EagerEncoder) PutStringArray(in []string) error {
	e.checkBuf(1)
	return e.encoder.PutStringArray(in)
}
func (e *EagerEncoder) PutInt32Array(in []int32) error {
	e.checkBuf(4 + 4*len(in))
	return e.encoder.PutInt32Array(in)
}
func (e *EagerEncoder) PutInt64Array(in []int64) error {
	e.checkBuf(4 + 8*len(in))
	return e.encoder.PutInt64Array(in)
}
func (e *EagerEncoder) Push(pe PushEncoder) {
	//e.encoder.Push(pe)
}
func (e *EagerEncoder) Pop() {
	e.encoder.Pop()
}
