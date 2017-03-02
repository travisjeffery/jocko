package commitlog

import (
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestIndex(t *testing.T) {
	path := filepath.Join(os.TempDir(), fmt.Sprintf(indexNameFormat, rand.Int63()))
	totalEntries := rand.Intn(10) + 10
	//case for roundDown
	bytes := int64(totalEntries*entryWidth + 1)

	idx, err := newIndex(options{
		path:  path,
		bytes: bytes,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(path)

	stat, err := idx.file.Stat()
	if err != nil {
		t.Fatal(t)
	}
	assert.Equal(t, roundDown(bytes, entryWidth), stat.Size())
	entries := []Entry{}
	for i := 0; i < totalEntries; i++ {
		entries = append(entries, Entry{
			int64(i),
			int64(i * 5),
		})
	}
	for _, e := range entries {
		if err := idx.WriteEntry(e); err != nil {
			t.Fatal(err)
		}
	}
	if err = idx.Sync(); err != nil {
		t.Fatal(err)
	}
	act := &Entry{}
	for i, exp := range entries {
		if err = idx.ReadEntry(act, int64(i*entryWidth)); err != nil {
			t.Fatal(err)
		}
		assert.Equal(t, exp.Offset, act.Offset, act)
		assert.Equal(t, exp.Position, act.Position, act)
	}
	assert.Equal(t, nil, idx.SanityCheck())

	//dirty data
	idx.position++
	assert.NotEqual(t, nil, idx.SanityCheck())

	idx.position--
	if err = idx.Close(); err != nil {
		t.Fatal(err)
	}
	if stat, err = os.Stat(path); err != nil {
		t.Fatal(err)
	}

	assert.Equal(t, int64(totalEntries*entryWidth), stat.Size())
}
