package commitlog

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"math/rand"
	"os"
	"path/filepath"
	"testing"
)

func TestNewCommitLog(t *testing.T) {
	l, err := New(filepath.Join(os.TempDir(), fmt.Sprintf("commitlogtest%d", rand.Int63())))
	defer l.deleteAll()

	if err != nil {
		t.Fatal(err)
	}

	_, err = l.Write([]byte("one"))
	if err != nil {
		t.Error(err)
	}

	_, err = l.Write([]byte("two"))
	if err != nil {
		t.Error(err)
	}

	r := l.NewReader(0)
	if err != nil {
		t.Error(err)
	}
	p, err := ioutil.ReadAll(r)
	if err != nil {
		t.Error(err)
	}

	check(t, p, []byte("onetwo"))

}

func check(t *testing.T, got, want []byte) {
	if !bytes.Equal(got, want) {
		t.Errorf("got = %s, want %s", string(got), string(want))
	}
}
