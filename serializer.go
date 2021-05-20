package orm

import (
	"bytes"
	"encoding/binary"
	"math"
	"reflect"
	"unsafe"
)

func newSerializer() *serializer {
	return &serializer{
		buffer: &bytes.Buffer{},
	}
}

type serializer struct {
	buffer  *bytes.Buffer
	scratch [binary.MaxVarintLen64]byte
}

func (s *serializer) Reset(binary []byte) {
	s.buffer.Reset()
	s.buffer.Write(binary)
}

func (s *serializer) CopyBinary() []byte {
	return s.buffer.Bytes()
}

func (s *serializer) SetNumber(v uint64) {
	ln := binary.PutUvarint(s.scratch[:binary.MaxVarintLen64], v)
	_, _ = s.buffer.Write(s.scratch[0:ln])
}

func (s *serializer) SetNumberSigned(v int64) {
	ln := binary.PutVarint(s.scratch[:binary.MaxVarintLen64], v)
	_, _ = s.buffer.Write(s.scratch[0:ln])
}

func (s *serializer) SetBool(v bool) {
	if v {
		s.scratch[0] = 1
		_, _ = s.buffer.Write(s.scratch[:1])
		return
	}
	s.scratch[0] = 0
	_, _ = s.buffer.Write(s.scratch[:1])
}

func (s *serializer) SetFloat(v float64) {
	s.SetNumber(math.Float64bits(v))
}

func (s *serializer) SetString(v string) {
	str := str2Bytes(v)
	l := len(str)
	s.SetNumber(uint64(l))
	if l > 0 {
		_, _ = s.buffer.Write(str)
	}
}

func (s *serializer) SetBytes(val []byte) {
	l := len(val)
	s.SetNumber(uint64(l))
	if l > 0 {
		_, _ = s.buffer.Write(val)
	}
}

func (s *serializer) GetBool() bool {
	v, _ := s.ReadByte()
	return v == 1
}

func (s *serializer) GetNumber() uint64 {
	v, _ := binary.ReadUvarint(s)
	return v
}

func (s *serializer) GetNumberSigned() int64 {
	v, _ := binary.ReadVarint(s)
	return v
}

func (s *serializer) GetFloat() float64 {
	return math.Float64frombits(s.GetNumber())
}

func (s *serializer) GetFixed(ln int) []byte {
	buf := make([]byte, ln)
	_, _ = s.buffer.Read(buf)
	return buf
}

func (s *serializer) GetString() string {
	l := s.GetNumber()
	if l == 0 {
		return ""
	}
	return string(s.GetFixed(int(l)))
}

func (s *serializer) GetBytes() []byte {
	l := s.GetNumber()
	if l == 0 {
		return nil
	}
	return s.GetFixed(int(l))
}

func (s *serializer) ReadByte() (byte, error) {
	_, _ = s.buffer.Read(s.scratch[:1])
	return s.scratch[0], nil
}

func str2Bytes(str string) []byte {
	if len(str) == 0 {
		return nil
	}
	var b []byte
	byteHeader := (*reflect.SliceHeader)(unsafe.Pointer(&b))
	byteHeader.Data = (*reflect.StringHeader)(unsafe.Pointer(&str)).Data
	l := len(str)
	byteHeader.Len = l
	byteHeader.Cap = l
	return b
}
