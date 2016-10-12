package commitlog

import (
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"testing"

	"github.com/bmizerany/assert"
)

func TestIndex(t *testing.T) {
	path := filepath.Join(os.TempDir(), fmt.Sprintf("commitlogtest%d", rand.Int63()))
	os.RemoveAll(path)
	os.MkdirAll(path, 0755)

	path = filepath.Join(path, "test.index")

	idx, err := newIndex(options{
		path: path,
	})
	if err != nil {
		t.Fatal(err)
	}
	entries := []entry{}
	for i := 0; i < 3; i++ {
		entries = append(entries, entry{
			int8(i),
			int8(i * 5),
		})
	}
	for _, e := range entries {
		idx.WriteEntry(e)
	}
	if err = idx.Sync(); err != nil {
		t.Fatal(err)
	}
	act := &entry{}
	for i, exp := range entries {
		if err = idx.ReadEntry(act, int64(i*entryWidth)); err != nil {
			t.Fatal(err)
		}
		assert.Equal(t, exp.Offset, act.Offset)
		assert.Equal(t, exp.Position, act.Position)
	}
}
