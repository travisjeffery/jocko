package metadata

import (
	"testing"

	"github.com/hashicorp/serf/serf"
)

func TestIsBroker(t *testing.T) {
	tests := []struct {
		name     string
		function func(t *testing.T)
	}{
		{
			name:     "minumum config",
			function: testMinimum,
		},
	}
	for _, test := range tests {
		t.Run(test.name, test.function)
	}
}

func testMinimum(t *testing.T) {
	b, ok := IsBroker(serf.Member{Tags: map[string]string{"id": "1", "role": "jocko"}})
	if !ok {
		t.Fatal("is broker not ok")
	}
	if b.ID != 1 {
		t.Fatal("broker id is not 1")
	}
}
