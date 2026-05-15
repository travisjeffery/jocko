package protocol

import (
	"reflect"
	"testing"
	"time"
)

func TestKafka42APIVersionsCatalog(t *testing.T) {
	tests := []struct {
		name string
		key  int16
		min  int16
		max  int16
	}{
		{name: "produce", key: ProduceKey, min: 3, max: 13},
		{name: "fetch", key: FetchKey, min: 4, max: 18},
		{name: "list offsets", key: ListOffsetsKey, min: 1, max: 11},
		{name: "api versions", key: APIVersionsKey, min: 0, max: 4},
		{name: "consumer group heartbeat", key: ConsumerGroupHeartbeatKey, min: 0, max: 1},
		{name: "share fetch", key: ShareFetchKey, min: 1, max: 2},
		{name: "streams group heartbeat", key: StreamsGroupHeartbeatKey, min: 0, max: 0},
		{name: "delete share group offsets", key: DeleteShareGroupOffsetsKey, min: 0, max: 0},
	}

	if got, want := len(Kafka42APIVersions), 77; got != want {
		t.Fatalf("Kafka42APIVersions length = %d, want %d", got, want)
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, ok := APIVersionForKey(tt.key, Kafka42APIVersions)
			if !ok {
				t.Fatalf("key %d not found", tt.key)
			}
			want := APIVersion{APIKey: tt.key, MinVersion: tt.min, MaxVersion: tt.max}
			if got != want {
				t.Fatalf("APIVersionForKey(%d) = %#v, want %#v", tt.key, got, want)
			}
		})
	}
}

func TestSupportedAPIVersionUsesAdvertisedImplementedSurface(t *testing.T) {
	if !SupportedAPIVersion(ProduceKey, 5) {
		t.Fatal("Produce v5 should be advertised as supported")
	}
	if SupportedAPIVersion(ProduceKey, 13) {
		t.Fatal("Produce v13 is in Kafka42APIVersions but should not be advertised as implemented")
	}
	if SupportedAPIVersion(ConsumerGroupHeartbeatKey, 0) {
		t.Fatal("ConsumerGroupHeartbeat is in Kafka42APIVersions but should not be advertised as implemented")
	}
}

func TestAPIVersionsResponseRoundTripIncludesErrorCode(t *testing.T) {
	exp := &APIVersionsResponse{
		APIVersion:   1,
		ErrorCode:    ErrUnsupportedVersion.Code(),
		APIVersions:  []APIVersion{{APIKey: APIVersionsKey, MinVersion: 0, MaxVersion: 1}},
		ThrottleTime: 2 * time.Millisecond,
	}

	b, err := Encode(exp)
	if err != nil {
		t.Fatal(err)
	}

	var act APIVersionsResponse
	if err := Decode(b, &act, exp.APIVersion); err != nil {
		t.Fatal(err)
	}

	if !reflect.DeepEqual(exp, &act) {
		t.Fatalf("round trip mismatch:\nexp=%#v\nact=%#v", exp, &act)
	}
}
