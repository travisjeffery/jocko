package protocol

import (
	"go.uber.org/zap/zapcore"
)

// https://kafka.apache.org/protocol#The_Messages_FindCoordinator

type CoordinatorType int8

const (
	CoordinatorGroup       CoordinatorType = 0
	CoordinatorTransaction CoordinatorType = 0
)

type FindCoordinatorRequest struct {
	APIVersion int16

	CoordinatorKey  string
	CoordinatorType CoordinatorType
}

func (r *FindCoordinatorRequest) Encode(e PacketEncoder) (err error) {
	if err = e.PutString(r.CoordinatorKey); err != nil {
		return err
	}
	if r.APIVersion >= 1 {
		e.PutInt8(int8(r.CoordinatorType))
	}
	return nil
}

func (r *FindCoordinatorRequest) Decode(d PacketDecoder, version int16) (err error) {
	r.APIVersion = version

	if r.CoordinatorKey, err = d.String(); err != nil {
		return err
	}
	if version >= 1 {
		coordinatorType, err := d.Int8()
		if err != nil {
			return err
		}
		r.CoordinatorType = CoordinatorType(coordinatorType)
	}
	return nil
}

func (r *FindCoordinatorRequest) Key() int16 {
	return 10
}

func (r *FindCoordinatorRequest) Version() int16 {
	return r.APIVersion
}

func (r *FindCoordinatorRequest) MarshalLogObject(e zapcore.ObjectEncoder) error {
	return nil
}
