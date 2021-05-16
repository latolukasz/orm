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
	s.buffer.Reset()
}

func (s *serializer) SetUvarint(v uint64) {
	ln := binary.PutUvarint(s.scratch[:binary.MaxVarintLen64], v)
	_, _ = s.buffer.Write(s.scratch[0:ln])
}

func (s *serializer) SetBool(v bool) {
	if v {
		s.SetUInt8(1)
		return
	}
	s.SetUInt8(0)
}

func (s *serializer) SetInt8(v int8) {
	s.SetUInt8(uint8(v))
}

func (s *serializer) SetInt16(v int16) {
	s.SetUInt16(uint16(v))
}

func (s *serializer) SetInt32(v int32) {
	s.SetUInt32(uint32(v))
}

func (s *serializer) SetInt64(v int64) {
	s.SetUInt64(uint64(v))
}

func (s *serializer) SetUInt8(v uint8) {
	s.scratch[0] = v
	_, _ = s.buffer.Write(s.scratch[:1])
}

func (s *serializer) SetUInt16(v uint16) {
	s.scratch[0] = byte(v)
	s.scratch[1] = byte(v >> 8)
	_, _ = s.buffer.Write(s.scratch[:2])
}

func (s *serializer) SetUInt32(v uint32) {
	s.scratch[0] = byte(v)
	s.scratch[1] = byte(v >> 8)
	s.scratch[2] = byte(v >> 16)
	s.scratch[3] = byte(v >> 24)
	_, _ = s.buffer.Write(s.scratch[:4])
}

func (s *serializer) SetUInt64(v uint64) {
	s.scratch[0] = byte(v)
	s.scratch[1] = byte(v >> 8)
	s.scratch[2] = byte(v >> 16)
	s.scratch[3] = byte(v >> 24)
	s.scratch[4] = byte(v >> 32)
	s.scratch[5] = byte(v >> 40)
	s.scratch[6] = byte(v >> 48)
	s.scratch[7] = byte(v >> 56)
	_, _ = s.buffer.Write(s.scratch[:8])
}

func (s *serializer) SetFloat32(v float32) {
	s.SetUInt32(math.Float32bits(v))
}

func (s *serializer) SetFloat64(v float64) {
	s.SetUInt64(math.Float64bits(v))
}

func (s *serializer) SetString(v string) {
	str := str2Bytes(v)
	l := len(str)
	s.SetUvarint(uint64(l))
	if l > 0 {
		_, _ = s.buffer.Write(str)
	}
}

func (s *serializer) SetBytes(val []byte) {
	l := len(val)
	s.SetUvarint(uint64(l))
	if l > 0 {
		_, _ = s.buffer.Write(val)
	}
}

func (s *serializer) GetBool() bool {
	v, _ := s.ReadByte()
	return v == 1
}

func (s *serializer) GetUvarint() uint64 {
	v, _ := binary.ReadUvarint(s)
	return v
}

func (s *serializer) GetInt8() int8 {
	v, _ := s.ReadByte()
	return int8(v)
}

func (s *serializer) GetInt16() int16 {
	return int16(s.GetUInt16())
}

func (s *serializer) GetInt32() int32 {
	return int32(s.GetUInt32())
}

func (s *serializer) GetInt64() int64 {
	return int64(s.GetUInt64())
}

func (s *serializer) GetUInt8() uint8 {
	v, _ := s.ReadByte()
	return v
}

func (s *serializer) GetUInt16() uint16 {
	_, _ = s.buffer.Read(s.scratch[:2])
	return uint16(s.scratch[0]) | uint16(s.scratch[1])<<8
}

func (s *serializer) GetUInt32() uint32 {
	_, _ = s.buffer.Read(s.scratch[:4])
	return uint32(s.scratch[0]) |
		uint32(s.scratch[1])<<8 |
		uint32(s.scratch[2])<<16 |
		uint32(s.scratch[3])<<24
}

func (s *serializer) GetUInt64() uint64 {
	_, _ = s.buffer.Read(s.scratch[:8])
	return uint64(s.scratch[0]) |
		uint64(s.scratch[1])<<8 |
		uint64(s.scratch[2])<<16 |
		uint64(s.scratch[3])<<24 |
		uint64(s.scratch[4])<<32 |
		uint64(s.scratch[5])<<40 |
		uint64(s.scratch[6])<<48 |
		uint64(s.scratch[7])<<56
}

func (s *serializer) GetFloat32() float32 {
	return math.Float32frombits(s.GetUInt32())
}

func (s *serializer) GetFloat64() float64 {
	return math.Float64frombits(s.GetUInt64())
}

func (s *serializer) GetFixed(ln int) []byte {
	buf := make([]byte, ln)
	_, _ = s.buffer.Read(buf)
	return buf
}

func (s *serializer) GetString() string {
	l := s.GetUvarint()
	if l == 0 {
		return ""
	}
	return string(s.GetFixed(int(l)))
}

func (s *serializer) GetBytes() []byte {
	l := s.GetUvarint()
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
