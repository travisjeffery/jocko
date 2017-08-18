package protocol

type APIVersionsRequest struct{}

func (c *APIVersionsRequest) Encode(_ PacketEncoder) error {
	return nil
}

func (c *APIVersionsRequest) Decode(_ PacketDecoder) error {
	return nil
}

func (c *APIVersionsRequest) Key() int16 {
	return APIVersionsKey
}

func (c *APIVersionsRequest) Version() int16 {
	return 0
}
