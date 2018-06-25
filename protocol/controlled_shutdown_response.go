package protocol

type ControlledShutdownResponse struct {
	APIVersion int16
}

func (r *ControlledShutdownResponse) Encode(e PacketEncoder) (err error) {
	return nil
}

func (r *ControlledShutdownResponse) Decode(d PacketDecoder, version int16) (err error) {
	r.APIVersion = version

	return nil
}

func (r *ControlledShutdownResponse) Version() int16 {
	return r.APIVersion
}
