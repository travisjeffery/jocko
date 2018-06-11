package protocol

import (
	"go.uber.org/zap/zapcore"
)

import "time"

type TopicErrorCode struct {
	Topic        string
	ErrorCode    int16
	ErrorMessage *string
}

type CreateTopicsResponse struct {
	APIVersion int16

	ThrottleTime    time.Duration
	TopicErrorCodes []*TopicErrorCode
}

func (c *CreateTopicsResponse) Encode(e PacketEncoder) (err error) {
	if c.APIVersion >= 2 {
		e.PutInt32(int32(c.ThrottleTime / time.Millisecond))
	}
	if err = e.PutArrayLength(len(c.TopicErrorCodes)); err != nil {
		return err
	}
	for _, t := range c.TopicErrorCodes {
		if err = e.PutString(t.Topic); err != nil {
			return err
		}
		e.PutInt16(t.ErrorCode)
		if c.APIVersion >= 1 {
			e.PutNullableString(t.ErrorMessage)
		}
	}
	return nil
}

func (c *CreateTopicsResponse) Decode(d PacketDecoder, version int16) error {
	c.APIVersion = version

	if version >= 2 {
		throttleTime, err := d.Int32()
		if err != nil {
			return err
		}
		c.ThrottleTime = time.Duration(throttleTime) * time.Millisecond
	}

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
		var errorMessage *string
		if version >= 1 {
			errorMessage, err = d.NullableString()
			if err != nil {
				return err
			}
		}
		c.TopicErrorCodes[i] = &TopicErrorCode{
			Topic:        topic,
			ErrorCode:    errorCode,
			ErrorMessage: errorMessage,
		}
	}
	return nil
}

func (r *CreateTopicsResponse) Version() int16 {
	return r.APIVersion
}

func (r *CreateTopicsResponse) MarshalLogObject(e zapcore.ObjectEncoder) error {
	return nil
}
