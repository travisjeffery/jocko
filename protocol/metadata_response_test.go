package protocol

import (
	"reflect"
	"testing"
)

func strp(s string) *string { return &s }

// TestMetadataResponseV1RoundTrip locks in the Kafka Metadata v1 wire fields
// that v0 lacks: per-broker rack (nullable string), controller_id, and
// per-topic is_internal (boolean).
// See https://kafka.apache.org/protocol#The_Messages_Metadata
func TestMetadataResponseV1RoundTrip(t *testing.T) {
	exp := &MetadataResponse{
		APIVersion:   1,
		ControllerID: 7,
		Brokers: []*Broker{
			{NodeID: 1, Host: "a", Port: 9092, Rack: strp("rack-1")},
			{NodeID: 2, Host: "b", Port: 9092, Rack: nil}, // nullable rack absent
		},
		TopicMetadata: []*TopicMetadata{
			{
				TopicErrorCode: 0,
				Topic:          "demo",
				IsInternal:     false,
				PartitionMetadata: []*PartitionMetadata{
					{PartitionErrorCode: 0, PartitionID: 0, Leader: 1, Replicas: []int32{1, 2}, ISR: []int32{1, 2}},
				},
			},
			{
				TopicErrorCode:    0,
				Topic:             "__consumer_offsets",
				IsInternal:        true,
				PartitionMetadata: []*PartitionMetadata{},
			},
		},
	}

	b, err := Encode(exp)
	if err != nil {
		t.Fatal(err)
	}

	var act MetadataResponse
	if err := Decode(b, &act, exp.APIVersion); err != nil {
		t.Fatal(err)
	}

	if !reflect.DeepEqual(exp, &act) {
		t.Fatalf("v1 round trip mismatch:\nexp=%#v\nact=%#v", exp, &act)
	}
}

// TestMetadataResponseV0OmitsV1Fields ensures the v1-only fields (rack,
// controller_id, is_internal) are gated behind the version check and are not
// written when encoding a v0 response.
func TestMetadataResponseV0OmitsV1Fields(t *testing.T) {
	// Even though rack/is_internal are populated here, a v0 encode must drop
	// them, so the decoded value comes back with the zero values.
	in := &MetadataResponse{
		APIVersion:   0,
		ControllerID: 7,
		Brokers: []*Broker{
			{NodeID: 1, Host: "a", Port: 9092, Rack: strp("rack-1")},
		},
		TopicMetadata: []*TopicMetadata{
			{TopicErrorCode: 0, Topic: "demo", IsInternal: true, PartitionMetadata: []*PartitionMetadata{}},
		},
	}

	b, err := Encode(in)
	if err != nil {
		t.Fatal(err)
	}

	var act MetadataResponse
	if err := Decode(b, &act, 0); err != nil {
		t.Fatal(err)
	}

	if act.Brokers[0].Rack != nil {
		t.Fatalf("v0 should not carry broker rack, got %q", *act.Brokers[0].Rack)
	}
	if act.TopicMetadata[0].IsInternal {
		t.Fatal("v0 should not carry topic is_internal")
	}
	if act.ControllerID != 0 {
		t.Fatalf("v0 should not carry controller_id, got %d", act.ControllerID)
	}
}
