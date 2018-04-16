package util

import (
	"strings"

	"github.com/cespare/xxhash"
	"github.com/davecgh/go-spew/spew"
)

func Dump(i interface{}) string {
	return strings.Replace(spew.Sdump(i), "\n", "", -1)
}

func Hash(s string) uint64 {
	h := xxhash.New()
	if _, err := h.Write([]byte(s)); err != nil {
		panic(err)
	}
	return h.Sum64()
}
