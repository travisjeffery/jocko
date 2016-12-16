package protocol

type DeleteTopicsRequest struct {
	Topics  []string
	Timeout int32
}

func (c *DeleteTopicsRequest) Encode(e PacketEncoder) (err error) {
	if err = e.PutStringArray(c.Topics); err != nil {
		return err
	}
	e.PutInt32(c.Timeout)
	return nil
}

func (c *DeleteTopicsRequest) Decode(d PacketDecoder) (err error) {
	c.Topics, err = d.StringArray()
	if err != nil {
		return err
	}
	c.Timeout, err = d.Int32()
	return err
}

func (c *DeleteTopicsRequest) Key() int16 {
	return DeleteTopicsKey
}

func (c *DeleteTopicsRequest) Version() int16 {
	return 0
}
