package protocol

type Body interface {
	Decode(d Decoder)
	Encode(e Encoder)
	Key() int16
	Version() int16
}
