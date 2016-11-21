package protocol

type DeleteTopicsResponse struct {
	TopicErrorCodes []*TopicErrorCode
}

func (c *DeleteTopicsResponse) Encode(e PacketEncoder) error {
	e.PutArrayLength(len(c.TopicErrorCodes))
	for _, t := range c.TopicErrorCodes {
		e.PutString(t.Topic)
		e.PutInt16(t.ErrorCode)
	}
	return nil
}

func (c *DeleteTopicsResponse) Decode(d PacketDecoder) error {
	l, err := d.ArrayLength()
	if err != nil {
		return err
	}
	c.TopicErrorCodes = make([]*TopicErrorCode, l)
	for i := range c.TopicErrorCodes {
		topic, err := d.String()
		if err != nil {
			return err
		}
		errorCode, err := d.Int16()
		if err != nil {
			return err
		}
		c.TopicErrorCodes[i] = &TopicErrorCode{
			Topic:     topic,
			ErrorCode: errorCode,
		}
	}
	return nil
}
