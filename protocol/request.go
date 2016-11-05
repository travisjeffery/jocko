package protocol

import "math/rand"

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

func (r *RequestHeader) Encode(e Encoder) {
	e.PutInt32(r.Size)
	e.PutInt16(r.APIKey)
	e.PutInt16(r.APIVersion)
	e.PutInt32(r.CorrelationID)
	e.PutString(r.ClientID)
}

func (r *RequestHeader) Decode(d Decoder) error {
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

func CorrelationID() int32 {
	return int32(rand.Uint32())
}

func ClientID() string {
	return "jocko"
}
