package testutil

import (
	"io/ioutil"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

// TempDirList is a container for acquiring and removing temporary
// directories from the OS.
type TempDirList struct {
	tempDirs []string
}

// NewTempDirList creates a new TempDirList.
func NewTempDirList() *TempDirList {
	return &TempDirList{}
}

// NewTempDir acquires a new temporary directory from the OS and
// returns a string representing it's location.
func (l *TempDirList) NewTempDir(t *testing.T) string {
	tmpDir, err := ioutil.TempDir("/tmp", "")
	assert.NoError(t, err)

	l.tempDirs = append(l.tempDirs, tmpDir)

	return tmpDir
}

// Cleanup removes all temporary directories that have been acquired
// from the OS.
func (l *TempDirList) Cleanup() {
	for _, dir := range l.tempDirs {
		os.RemoveAll(dir)
	}

	// clear tempDirs
	l.tempDirs = l.tempDirs[:0]
}
