package protocol

type ControlledShutdownRequest struct {
	APIVersion int16
}

func (r *ControlledShutdownRequest) Encode(e PacketEncoder) (err error) {
	return nil
}

func (r *ControlledShutdownRequest) Decode(d PacketDecoder, version int16) (err error) {
	return nil
}

func (r *ControlledShutdownRequest) Key() int16 {
	return ControlledShutdownKey
}

func (r ControlledShutdownRequest) Version() int16 {
	return r.APIVersion
}
