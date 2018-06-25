package protocol

import "fmt"

type RequestHeader struct {
	// Size of the request
	Size int32
	// ID of the API (e.g. produce, fetch, metadata)
	APIKey int16
	// Version of the API to use
	APIVersion int16
	// User defined ID to correlate requests between server and client
	CorrelationID int32
	// Size of the Client ID
	ClientID string
}

func (r *RequestHeader) Encode(e PacketEncoder) {
	e.PutInt32(r.Size)
	e.PutInt16(r.APIKey)
	e.PutInt16(r.APIVersion)
	e.PutInt32(r.CorrelationID)
	if err := e.PutString(r.ClientID); err != nil {
		// TODO: better err handling
		panic(err)
	}
}

func (r *RequestHeader) Decode(d PacketDecoder) error {
	var err error
	r.Size, err = d.Int32()
	if err != nil {
		return err
	}
	r.APIKey, err = d.Int16()
	if err != nil {
		return err
	}
	r.APIVersion, err = d.Int16()
	if err != nil {
		return err
	}
	r.CorrelationID, err = d.Int32()
	if err != nil {
		return err
	}
	r.ClientID, err = d.String()
	return err
}

func (r *RequestHeader) String() string {
	return fmt.Sprintf(
		"correlation id: %d, api key: %d, client: %s, size: %d",
		r.CorrelationID,
		r.APIKey,
		r.ClientID,
		r.Size,
	)
}
