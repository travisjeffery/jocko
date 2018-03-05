package protocol

type TopicErrorCode struct {
	Topic     string
	ErrorCode int16
}

type CreateTopicsResponse struct {
	TopicErrorCodes []*TopicErrorCode
}

func (c *CreateTopicsResponse) Encode(e PacketEncoder) error {
	var err error
	if err = e.PutArrayLength(len(c.TopicErrorCodes)); err != nil {
		return err
	}
	for _, t := range c.TopicErrorCodes {
		if err = e.PutString(t.Topic); err != nil {
			return err
		}
		e.PutInt16(t.ErrorCode)
	}
	return nil
}

func (c *CreateTopicsResponse) Decode(d PacketDecoder) error {
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
