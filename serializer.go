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
		output: &bytes.Buffer{},
	}
}

type serializer struct {
	output  *bytes.Buffer
	scratch [binary.MaxVarintLen64]byte
}

func (s *serializer) Nullable(isNull bool) {
	nullablePrefix := uint8(0)
	if !isNull {
		nullablePrefix = uint8(1)
	}
	_, _ = s.output.Write([]byte{nullablePrefix})
}

func (s *serializer) Uvarint(v uint64) {
	ln := binary.PutUvarint(s.scratch[:binary.MaxVarintLen64], v)
	_, _ = s.output.Write(s.scratch[0:ln])
}

func (s *serializer) Bool(v bool) {
	if v {
		s.UInt8(1)
		return
	}
	s.UInt8(0)
}

func (s *serializer) Int8(v int8) {
	s.UInt8(uint8(v))
}

func (s *serializer) Int16(v int16) {
	s.UInt16(uint16(v))
}

func (s *serializer) Int32(v int32) {
	s.UInt32(uint32(v))
}

func (s *serializer) Int64(v int64) {
	s.UInt64(uint64(v))
}

func (s *serializer) UInt8(v uint8) {
	s.scratch[0] = v
	_, _ = s.output.Write(s.scratch[:1])
}

func (s *serializer) UInt16(v uint16) {
	s.scratch[0] = byte(v)
	s.scratch[1] = byte(v >> 8)
	_, _ = s.output.Write(s.scratch[:2])
}

func (s *serializer) UInt32(v uint32) {
	s.scratch[0] = byte(v)
	s.scratch[1] = byte(v >> 8)
	s.scratch[2] = byte(v >> 16)
	s.scratch[3] = byte(v >> 24)
	_, _ = s.output.Write(s.scratch[:4])
}

func (s *serializer) UInt64(v uint64) {
	s.scratch[0] = byte(v)
	s.scratch[1] = byte(v >> 8)
	s.scratch[2] = byte(v >> 16)
	s.scratch[3] = byte(v >> 24)
	s.scratch[4] = byte(v >> 32)
	s.scratch[5] = byte(v >> 40)
	s.scratch[6] = byte(v >> 48)
	s.scratch[7] = byte(v >> 56)
	_, _ = s.output.Write(s.scratch[:8])
}

func (s *serializer) Float32(v float32) {
	s.UInt32(math.Float32bits(v))
}

func (s *serializer) Float64(v float64) {
	s.UInt64(math.Float64bits(v))
}

func (s *serializer) String(v string) {
	str := str2Bytes(v)
	s.Uvarint(uint64(len(str)))
	_, _ = s.output.Write(str)
}

func (s *serializer) Bytes(val []byte) {
	s.Uvarint(uint64(len(val)))
	if val != nil {
		_, _ = s.output.Write(val)
	}
}

func newDeserializer(input []byte) *deserializer {
	return &deserializer{
		input: bytes.NewBuffer(input),
	}
}

type deserializer struct {
	input   *bytes.Buffer
	scratch [binary.MaxVarintLen64]byte
}

func (d *deserializer) Bool() bool {
	v, _ := d.ReadByte()
	return v == 1
}

func (d *deserializer) Uvarint() uint64 {
	v, _ := binary.ReadUvarint(d)
	return v
}

func (d *deserializer) Int8() int8 {
	v, _ := d.ReadByte()
	return int8(v)
}

func (d *deserializer) Int16() int16 {
	return int16(d.UInt16())
}

func (d *deserializer) Int32() int32 {
	return int32(d.UInt32())
}

func (d *deserializer) Int64() int64 {
	return int64(d.UInt64())
}

func (d *deserializer) UInt8() uint8 {
	v, _ := d.ReadByte()
	return v
}

func (d *deserializer) UInt16() uint16 {
	_, _ = d.input.Read(d.scratch[:2])
	return uint16(d.scratch[0]) | uint16(d.scratch[1])<<8
}

func (d *deserializer) UInt32() uint32 {
	_, _ = d.input.Read(d.scratch[:4])
	return uint32(d.scratch[0]) |
		uint32(d.scratch[1])<<8 |
		uint32(d.scratch[2])<<16 |
		uint32(d.scratch[3])<<24
}

func (d *deserializer) UInt64() uint64 {
	_, _ = d.input.Read(d.scratch[:8])
	return uint64(d.scratch[0]) |
		uint64(d.scratch[1])<<8 |
		uint64(d.scratch[2])<<16 |
		uint64(d.scratch[3])<<24 |
		uint64(d.scratch[4])<<32 |
		uint64(d.scratch[5])<<40 |
		uint64(d.scratch[6])<<48 |
		uint64(d.scratch[7])<<56
}

func (d *deserializer) Float32() float32 {
	return math.Float32frombits(d.UInt32())
}

func (d *deserializer) Float64() float64 {
	return math.Float64frombits(d.UInt64())
}

func (d *deserializer) Fixed(ln int) []byte {
	buf := make([]byte, ln)
	_, _ = d.input.Read(buf)
	return buf
}

func (d *deserializer) String() string {
	str := d.Fixed(int(d.Uvarint()))
	return string(str)
}

func (d *deserializer) Bytes() []byte {
	return d.Fixed(int(d.Uvarint()))
}

func (d *deserializer) ReadByte() (byte, error) {
	_, _ = d.input.Read(d.scratch[:1])
	return d.scratch[0], nil
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
