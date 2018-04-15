package structs

import (
	"reflect"
	"testing"
)

func TestEncodeDecode(t *testing.T) {
	type test struct {
		function func(*testing.T)
		name     string
	}
	tests := []test{
		{
			function: testPartition,
			name:     "partition",
		},
		{
			function: testGroup,
			name:     "group",
		},
	}
	for _, test := range tests {
		t.Run(test.name, test.function)
	}
}

func testPartition(t *testing.T) {
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

func testGroup(t *testing.T) {
	in := RegisterGroupRequest{
		Group: Group{
			Group:   "group-id",
			Members: map[string]Member{},
		},
	}
	b, err := Encode(RegisterGroupRequestType, &in)
	if err != nil {
		t.Fatalf("err: %s", err)
	}
	var out RegisterGroupRequest
	err = Decode(b[1:], &out)
	if err != nil {
		t.Fatalf("err: %s", err)
	}
	if !reflect.DeepEqual(in, out) {
		t.Fatal("in != out")
	}
}
