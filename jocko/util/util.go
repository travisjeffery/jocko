package util

import (
	"strings"

	"github.com/davecgh/go-spew/spew"
)

func Dump(i interface{}) string {
	return strings.Replace(spew.Sdump(i), "\n", "", -1)
}
