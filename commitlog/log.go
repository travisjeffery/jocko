package commitlog

import (
	"bytes"
	"io"
	"os"
)

type log struct {
	*os.File
	size int64
}

func newLog(f *os.File) (*log, error) {
	fi, err := os.Stat(f.Name())
	if err != nil {
		return nil, err
	}
	size := int64(fi.Size())
	return &log{
		File: f,
		size: size,
	}, nil
}

// Append the bytes to the end of the log file and returns the position the bytes were written and
// the number of bytes written.
func (l *log) Append(p []byte) (int64, int64, error) {
	pos := l.size
	n, err := l.WriteAt(p, int64(pos))
	if err != nil {
		return 0, 0, err
	}
	l.size += int64(n)
	return int64(n), pos, nil
}

func (l *log) Size() int64 {
	return l.size
}

type logScanner struct {
	// log must be set
	log *log
	// buf must be set
	buf *bytes.Buffer
	pos int64
	off int64
	err error
	cur entry
}

func (s *logScanner) Scan() bool {
	s.buf.Truncate(0)

	// get offset and size
	if _, s.err = io.CopyN(s.buf, s.log, 12); s.err != nil {
		return false
	}

	size := int64(Encoding.Uint32(s.buf.Bytes()[8:12]))

	if _, s.err = io.CopyN(s.buf, s.log, size); s.err != nil {
		return false
	}

	s.entry = entry{Off: s.off, Pos: s.Pos}

	s.off++
	s.pos += size
}

func (s *logScanner) Err() error {
	return s.err
}

func (s *logScanner) Entry() entry {
	return s.cur
}
