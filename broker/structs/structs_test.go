package structs

import (
	"reflect"
	"testing"
)

func TestEncodeDecode(t *testing.T) {
	in := RegisterPartitionRequest{
		Partition: Partition{
			ID:              1,
			Partition:       1,
			Topic:           "test-topic",
			ISR:             []int32{1, 2, 3},
			Leader:          1,
			ControllerEpoch: 1,
			LeaderEpoch:     1,
		},
	}
	b, err := Encode(RegisterPartitionRequestType, &in)
	if err != nil {
		t.Fatalf("err: %s", err)
	}
	var out RegisterPartitionRequest
	err = Decode(b[1:], &out)
	if err != nil {
		t.Fatalf("err: %s", err)
	}
	if !reflect.DeepEqual(in, out) {
		t.Fatal("in != out")
	}
}
