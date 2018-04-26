package commitlog

import (
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestIndex(t *testing.T) {
	path := filepath.Join(os.TempDir(), fmt.Sprintf(fileFormat, rand.Int63(), indexSuffix))
	totalEntries := rand.Intn(10) + 10
	//case for roundDown
	bytes := int64(totalEntries*entryWidth + 1)

	idx, err := NewIndex(options{
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
	require.Equal(t, roundDown(bytes, entryWidth), stat.Size())
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
		if err = idx.ReadEntryAtFileOffset(act, int64(i*entryWidth)); err != nil {
			t.Fatal(err)
		}
		require.Equal(t, exp.Offset, act.Offset, act)
		require.Equal(t, exp.Position, act.Position, act)
	}
	require.Equal(t, nil, idx.SanityCheck())

	//dirty data
	idx.position++
	require.NotEqual(t, nil, idx.SanityCheck())

	idx.position--
	if err = idx.Close(); err != nil {
		t.Fatal(err)
	}
	if stat, err = os.Stat(path); err != nil {
		t.Fatal(err)
	}

	require.Equal(t, int64(totalEntries*entryWidth), stat.Size())

}

func TestIndexScanner(t *testing.T) {
	path := filepath.Join(os.TempDir(), fmt.Sprintf(fileFormat, rand.Int63(), indexSuffix))
	totalEntries := rand.Intn(10) + 10
	//case for roundDown
	bytes := int64(totalEntries*entryWidth + 1)

	idx, err := NewIndex(options{
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
	require.Equal(t, roundDown(bytes, entryWidth), stat.Size())
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

	scanner := NewIndexScanner(idx)
	for _, exp := range entries {
		act, err := scanner.Scan()
		require.NoError(t, err)
		require.Equal(t, exp.Offset, act.Offset)
		require.Equal(t, exp.Position, act.Position)
	}
	require.Equal(t, nil, idx.SanityCheck())

	act, err := scanner.Scan()
	require.Error(t, err)
	require.Nil(t, act)
}
