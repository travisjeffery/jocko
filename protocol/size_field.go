package protocol

import "errors"

type SizeField struct {
	StartOffset int
}

func (s *SizeField) SaveOffset(in int) {
	s.StartOffset = in
}

func (s *SizeField) ReserveSize() int {
	return 4
}

func (s *SizeField) PutSize(curOffset int, buf []byte) error {
	Encoding.PutUint32(buf[s.StartOffset:], uint32(curOffset-s.StartOffset-4))
	return nil
}

func (s *SizeField) Check(curOffset int, buf []byte) error {
	if uint32(curOffset-s.StartOffset-4) != Encoding.Uint32(buf[s.StartOffset:]) {
		// replace this error
		return errors.New("length field invalid")
	}
	return nil
}
