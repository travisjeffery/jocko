package util

import (
	"hash/fnv"
	"strings"

	"github.com/davecgh/go-spew/spew"
)

func Dump(i interface{}) string {
	return strings.Replace(spew.Sdump(i), "\n", "", -1)
}

func Hash(s string) uint32 {
	h := fnv.New32()
	if _, err := h.Write([]byte(s)); err != nil {
		panic(err)
	}
	return h.Sum32()
}
