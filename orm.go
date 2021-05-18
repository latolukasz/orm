package orm

import (
	"database/sql"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"time"

	jsoniter "github.com/json-iterator/go"

	"github.com/pkg/errors"
)

type Entity interface {
	getORM() *ORM
	GetID() uint64
	markToDelete()
	forceMarkToDelete()
	IsLoaded() bool
	IsLazy() bool
	Fill(engine *Engine)
	IsDirty(engine *Engine) bool
	GetDirtyBind(engine *Engine) (bind Bind, has bool)
	SetOnDuplicateKeyUpdate(bind Bind)
	SetEntityLogMeta(key string, value interface{})
	SetField(field string, value interface{}) error
	GetFieldLazy(engine *Engine, field string) interface{}
}

type ORM struct {
	binary               []byte
	tableSchema          *tableSchema
	onDuplicateKeyUpdate map[string]interface{}
	initialised          bool
	loaded               bool
	lazy                 bool
	inDB                 bool
	delete               bool
	fakeDelete           bool
	value                reflect.Value
	elem                 reflect.Value
	idElem               reflect.Value
	logMeta              map[string]interface{}
}

func (orm *ORM) getORM() *ORM {
	return orm
}

func (orm *ORM) GetID() uint64 {
	if !orm.idElem.IsValid() {
		return 0
	}
	return orm.idElem.Uint()
}

func (orm *ORM) GetFieldLazy(engine *Engine, field string) interface{} {
	if !orm.lazy {
		panic(fmt.Errorf("entity is not lazy"))
	}
	index, has := orm.tableSchema.columnMapping[field]
	if !has {
		panic(fmt.Errorf("uknown field " + field))
	}
	fields := orm.tableSchema.fields
	serializer := engine.getSerializer()
	serializer.Reset(orm.binary)
	for _, i := range fields.uintegers8 {
		if i == index {
			return serializer.GetUInt8()
		}
		serializer.buffer.Next(1)
	}
	for _, i := range fields.uintegers16 {
		if i == index {
			return serializer.GetUInt16()
		}
		serializer.buffer.Next(2)
	}
	for _, i := range fields.uintegers32 {
		if i == index {
			return serializer.GetUInt32()
		}
		serializer.buffer.Next(4)
	}
	for _, i := range fields.uintegers64 {
		if i == index {
			return serializer.GetUInt64()
		}
		serializer.buffer.Next(8)
	}
	for _, i := range fields.integers8 {
		if i == index {
			return serializer.GetInt8()
		}
		serializer.buffer.Next(1)
	}
	for _, i := range fields.integers16 {
		if i == index {
			return serializer.GetInt16()
		}
		serializer.buffer.Next(2)
	}
	for _, i := range fields.integers32 {
		if i == index {
			return serializer.GetInt32()
		}
		serializer.buffer.Next(4)
	}
	for _, i := range fields.integers64 {
		if i == index {
			return serializer.GetInt64()
		}
		serializer.buffer.Next(8)
	}
	return nil
}

func (orm *ORM) markToDelete() {
	orm.fakeDelete = true
}

func (orm *ORM) forceMarkToDelete() {
	orm.delete = true
}

func (orm *ORM) IsLoaded() bool {
	return orm.loaded
}

func (orm *ORM) IsLazy() bool {
	return orm.lazy
}

func (orm *ORM) Fill(engine *Engine) {
	if orm.lazy && orm.loaded {
		orm.deserialize(engine)
		orm.lazy = false
	}
}

func (orm *ORM) SetOnDuplicateKeyUpdate(bind Bind) {
	orm.onDuplicateKeyUpdate = bind
}

func (orm *ORM) SetEntityLogMeta(key string, value interface{}) {
	if orm.logMeta == nil {
		orm.logMeta = make(map[string]interface{})
	}
	orm.logMeta[key] = value
}

func (orm *ORM) IsDirty(engine *Engine) bool {
	if !orm.loaded {
		return true
	}
	_, is := orm.GetDirtyBind(engine)
	return is
}

func (orm *ORM) GetDirtyBind(engine *Engine) (bind Bind, has bool) {
	bind, _, _, has = orm.getDirtyBind(engine)
	return bind, has
}

func (orm *ORM) GetDirtyBindFull(engine *Engine) (bind, before Bind, has bool) {
	bind, before, _, has = orm.getDirtyBind(engine)
	return bind, before, has
}

func (orm *ORM) getDirtyBind(engine *Engine) (bind, oldBind Bind, updateBind map[string]string, has bool) {
	if orm.delete {
		return nil, nil, nil, true
	}
	if orm.fakeDelete {
		if orm.tableSchema.hasFakeDelete {
			orm.elem.FieldByName("FakeDelete").SetBool(true)
		} else {
			orm.delete = true
			return nil, nil, nil, true
		}
	}
	id := orm.GetID()
	bind = make(Bind)
	if orm.inDB && !orm.delete {
		oldBind = make(Bind)
		updateBind = make(map[string]string)
	}
	serializer := engine.getSerializer()
	orm.buildBind(id, serializer, bind, oldBind, updateBind, orm.tableSchema, orm.tableSchema.fields, orm.elem, "")
	has = id == 0 || len(bind) > 0
	return bind, oldBind, updateBind, has
}

func (orm *ORM) serialize(serializer *serializer) {
	orm.serializeFields(serializer, orm.tableSchema.fields, orm.elem)
	copy(orm.binary, serializer.buffer.Bytes())
	serializer.buffer.Reset()
}

func (orm *ORM) deserializeFromDB(engine *Engine, pointers []interface{}) {
	serializer := engine.getSerializer()
	serializer.buffer.Reset()
	orm.deserializeStructFromDB(serializer, 0, orm.tableSchema.fields, pointers)
	orm.binary = serializer.CopyBinary()
}

func (orm *ORM) deserializeStructFromDB(serializer *serializer, index int, fields *tableFields, pointers []interface{}) {
	for range fields.uintegers8 {
		serializer.SetUInt8(uint8(*pointers[index].(*uint64)))
		index++
	}
	for range fields.uintegers16 {
		serializer.SetUInt16(uint16(*pointers[index].(*uint64)))
		index++
	}
	for range fields.uintegers32 {
		serializer.SetUInt32(uint32(*pointers[index].(*uint64)))
		index++
	}
	for range fields.uintegers64 {
		serializer.SetUInt64(*pointers[index].(*uint64))
		index++
	}
	for range fields.integers8 {
		serializer.SetInt8(int8(*pointers[index].(*int64)))
		index++
	}
	for range fields.integers16 {
		serializer.SetInt16(int16(*pointers[index].(*int64)))
		index++
	}
	for range fields.integers32 {
		serializer.SetInt32(int32(*pointers[index].(*int64)))
		index++
	}
	for range fields.integers64 {
		serializer.SetInt64(*pointers[index].(*int64))
		index++
	}
	for range fields.booleans {
		serializer.SetBool(*pointers[index].(*bool))
		index++
	}
	for range fields.floats32 {
		serializer.SetFloat32(float32(*pointers[index].(*float64)))
		index++
	}
	for range fields.floats64 {
		serializer.SetFloat64(*pointers[index].(*float64))
		index++
	}
	for range fields.times {
		serializer.SetUInt32(*pointers[index].(*uint32))
		index++
	}
	if fields.fakeDelete > 0 {
		serializer.SetBool(*pointers[index].(*uint64) > 0)
		index++
	}
	for range fields.refs8 {
		serializer.SetUInt8(uint8(*pointers[index].(*uint64)))
		index++
	}
	for range fields.refs16 {
		serializer.SetUInt16(uint16(*pointers[index].(*uint64)))
		index++
	}
	for range fields.refs32 {
		serializer.SetUInt32(uint32(*pointers[index].(*uint64)))
		index++
	}
	for range fields.refs64 {
		serializer.SetUInt64(*pointers[index].(*uint64))
		index++
	}
	for range fields.uintegers8Nullable {
		v := pointers[index].(*sql.NullInt64)
		serializer.SetBool(v.Valid)
		if v.Valid {
			serializer.SetUInt8(uint8(v.Int64))
		}
	}
	for range fields.uintegers16Nullable {
		v := pointers[index].(*sql.NullInt64)
		serializer.SetBool(v.Valid)
		if v.Valid {
			serializer.SetUInt16(uint16(v.Int64))
		}
	}
	for range fields.uintegers32Nullable {
		v := pointers[index].(*sql.NullInt64)
		serializer.SetBool(v.Valid)
		if v.Valid {
			serializer.SetUInt32(uint32(v.Int64))
		}
	}
	for range fields.uintegers64Nullable {
		v := pointers[index].(*sql.NullInt64)
		serializer.SetBool(v.Valid)
		if v.Valid {
			serializer.SetUInt64(uint64(v.Int64))
		}
	}

	for range fields.integers8Nullable {
		v := pointers[index].(*sql.NullInt64)
		serializer.SetBool(v.Valid)
		if v.Valid {
			serializer.SetInt8(int8(v.Int64))
		}
	}
	for range fields.integers16Nullable {
		v := pointers[index].(*sql.NullInt64)
		serializer.SetBool(v.Valid)
		if v.Valid {
			serializer.SetInt16(int16(v.Int64))
		}
	}
	for range fields.integers32Nullable {
		v := pointers[index].(*sql.NullInt64)
		serializer.SetBool(v.Valid)
		if v.Valid {
			serializer.SetInt32(int32(v.Int64))
		}
	}
	for range fields.integers64Nullable {
		v := pointers[index].(*sql.NullInt64)
		serializer.SetBool(v.Valid)
		if v.Valid {
			serializer.SetInt64(int64(v.Int64))
		}
	}
}

func (orm *ORM) serializeFields(serializer *serializer, fields *tableFields, elem reflect.Value) {
	for _, i := range fields.uintegers8 {
		serializer.SetUInt8(uint8(elem.Field(i).Uint()))
	}
	for _, i := range fields.uintegers16 {
		serializer.SetUInt16(uint16(elem.Field(i).Uint()))
	}
	for _, i := range fields.uintegers32 {
		serializer.SetUInt32(uint32(elem.Field(i).Uint()))
	}
	for _, i := range fields.uintegers64 {
		serializer.SetUInt64(elem.Field(i).Uint())
	}
	for _, i := range fields.integers8 {
		serializer.SetInt8(int8(elem.Field(i).Int()))
	}
	for _, i := range fields.integers16 {
		serializer.SetInt16(int16(elem.Field(i).Int()))
	}
	for _, i := range fields.integers32 {
		serializer.SetInt32(int32(elem.Field(i).Int()))
	}
	for _, i := range fields.integers64 {
		serializer.SetInt64(elem.Field(i).Int())
	}
	for _, i := range fields.booleans {
		serializer.SetBool(elem.Field(i).Bool())
	}
	for _, i := range fields.floats32 {
		serializer.SetFloat32(float32(elem.Field(i).Float()))
	}
	for _, i := range fields.floats64 {
		serializer.SetFloat64(elem.Field(i).Float())
	}
	for _, i := range fields.times {
		serializer.SetUInt32(uint32(elem.Field(i).Interface().(time.Time).Unix()))
	}
	if fields.fakeDelete > 0 {
		serializer.SetBool(elem.Field(fields.fakeDelete).Uint() > 0)
	}
	for _, i := range fields.refs8 {
		e := elem.Field(i).Interface().(Entity)
		id := uint8(0)
		if e != nil {
			id = uint8(e.GetID())
		}
		serializer.SetUInt8(id)
	}
	for _, i := range fields.refs16 {
		e := elem.Field(i).Interface().(Entity)
		id := uint16(0)
		if e != nil {
			id = uint16(e.GetID())
		}
		serializer.SetUInt16(id)
	}
	for _, i := range fields.refs32 {
		e := elem.Field(i).Interface().(Entity)
		id := uint32(0)
		if e != nil {
			id = uint32(e.GetID())
		}
		serializer.SetUInt32(id)
	}
	for _, i := range fields.refs64 {
		e := elem.Field(i).Interface().(Entity)
		id := uint64(0)
		if e != nil {
			id = e.GetID()
		}
		serializer.SetUInt64(id)
	}
	for _, i := range fields.uintegers8Nullable {
		f := elem.Field(i)
		if f.IsNil() {
			serializer.SetBool(false)
		} else {
			serializer.SetBool(true)
			serializer.SetUInt8(uint8(f.Uint()))
		}
	}
	for _, i := range fields.uintegers16Nullable {
		f := elem.Field(i)
		if f.IsNil() {
			serializer.SetBool(false)
		} else {
			serializer.SetBool(true)
			serializer.SetUInt16(uint16(f.Uint()))
		}
	}
	for _, i := range fields.uintegers32Nullable {
		f := elem.Field(i)
		if f.IsNil() {
			serializer.SetBool(false)
		} else {
			serializer.SetBool(true)
			serializer.SetUInt32(uint32(f.Uint()))
		}
	}
	for _, i := range fields.uintegers64Nullable {
		f := elem.Field(i)
		if f.IsNil() {
			serializer.SetBool(false)
		} else {
			serializer.SetBool(true)
			serializer.SetUInt64(f.Uint())
		}
	}
	for _, i := range fields.integers8Nullable {
		f := elem.Field(i)
		if f.IsNil() {
			serializer.SetBool(false)
		} else {
			serializer.SetBool(true)
			serializer.SetInt8(int8(f.Int()))
		}
	}
	for _, i := range fields.integers16Nullable {
		f := elem.Field(i)
		if f.IsNil() {
			serializer.SetBool(false)
		} else {
			serializer.SetBool(true)
			serializer.SetInt16(int16(f.Int()))
		}
	}
	for _, i := range fields.integers32Nullable {
		f := elem.Field(i)
		if f.IsNil() {
			serializer.SetBool(false)
		} else {
			serializer.SetBool(true)
			serializer.SetInt32(int32(f.Int()))
		}
	}
	for _, i := range fields.integers64Nullable {
		f := elem.Field(i)
		if f.IsNil() {
			serializer.SetBool(false)
		} else {
			serializer.SetBool(true)
			serializer.SetInt64(f.Int())
		}
	}
	k := 0
	for _, i := range fields.stringsEnums {
		val := elem.Field(i).String()
		if val == "" {
			serializer.SetUInt8(0)
		} else {
			serializer.SetUInt8(uint8(fields.enums[k].Index(val)))
		}
		k++
	}
	for _, i := range fields.strings {
		serializer.SetString(elem.Field(i).String())
	}
	for _, i := range fields.bytes {
		serializer.SetBytes(elem.Field(i).Bytes())
	}
	for _, i := range fields.sliceStringsSets {
		f := elem.Field(i)
		values := f.Interface().([]string)
		l := len(values)
		serializer.SetUvarint(uint64(l))
		if l > 0 {
			set := fields.sets[k]
			for _, val := range values {
				serializer.SetUInt8(uint8(set.Index(val)))
			}
		}
		k++
	}
	for _, i := range fields.booleansNullable {
		f := elem.Field(i)
		if f.IsNil() {
			serializer.SetBool(false)
		} else {
			serializer.SetBool(true)
			serializer.SetBool(f.Bool())
		}
	}
	for _, i := range fields.floats32Nullable {
		f := elem.Field(i)
		if f.IsNil() {
			serializer.SetBool(false)
		} else {
			serializer.SetBool(true)
			serializer.SetFloat32(float32(f.Float()))
		}
	}
	for _, i := range fields.floats64Nullable {
		f := elem.Field(i)
		if f.IsNil() {
			serializer.SetBool(false)
		} else {
			serializer.SetBool(true)
			serializer.SetFloat64(f.Float())
		}
	}
	for _, i := range fields.timesNullable {
		f := elem.Field(i)
		if f.IsNil() {
			serializer.SetBool(false)
		} else {
			serializer.SetBool(true)
			serializer.SetUInt32(uint32(elem.Field(i).Interface().(time.Time).Unix()))
		}
	}
	for i, subField := range fields.structs {
		orm.serializeFields(serializer, subField, elem.Field(i).Elem())
	}
	for _, i := range fields.jsons {
		f := elem.Field(i)
		if f.IsNil() {
			serializer.SetBytes(nil)
		} else {
			encoded, _ := jsoniter.ConfigFastest.Marshal(f.Interface())
			serializer.SetBytes(encoded)
		}
	}
	for _, i := range fields.refsMany8 {
		e := elem.Field(i)
		if e.IsNil() {
			serializer.SetUvarint(uint64(0))
		} else {
			l := e.Len()
			serializer.SetUvarint(uint64(l))
			for k := 0; k < l; k++ {
				serializer.SetUInt8(uint8(e.Index(k).Interface().(Entity).GetID()))
			}
		}
	}
	for _, i := range fields.refsMany16 {
		e := elem.Field(i)
		if e.IsNil() {
			serializer.SetUvarint(uint64(0))
		} else {
			l := e.Len()
			serializer.SetUvarint(uint64(l))
			for k := 0; k < l; k++ {
				serializer.SetUInt16(uint16(e.Index(k).Interface().(Entity).GetID()))
			}
		}
	}
	for _, i := range fields.refsMany32 {
		e := elem.Field(i)
		if e.IsNil() {
			serializer.SetUvarint(uint64(0))
		} else {
			l := e.Len()
			serializer.SetUvarint(uint64(l))
			for k := 0; k < l; k++ {
				serializer.SetUInt32(uint32(e.Index(k).Interface().(Entity).GetID()))
			}
		}
	}
	for _, i := range fields.refsMany64 {
		e := elem.Field(i)
		if e.IsNil() {
			serializer.SetUvarint(uint64(0))
		} else {
			l := e.Len()
			serializer.SetUvarint(uint64(l))
			for k := 0; k < l; k++ {
				serializer.SetUInt64(e.Index(k).Interface().(Entity).GetID())
			}
		}
	}
}

func (orm *ORM) deserialize(engine *Engine) {
	orm.deserializeFields(engine, orm.tableSchema.fields, orm.elem)
}

func (orm *ORM) deserializeFields(engine *Engine, fields *tableFields, elem reflect.Value) {
	serializer := engine.getSerializer()
	for _, i := range fields.uintegers8 {
		elem.Field(i).SetUint(uint64(serializer.GetUInt8()))
	}
	for _, i := range fields.uintegers16 {
		elem.Field(i).SetUint(uint64(serializer.GetUInt16()))
	}
	for _, i := range fields.uintegers32 {
		elem.Field(i).SetUint(uint64(serializer.GetUInt32()))
	}
	for _, i := range fields.uintegers64 {
		elem.Field(i).SetUint(serializer.GetUInt64())
	}
	for _, i := range fields.integers8 {
		elem.Field(i).SetInt(int64(serializer.GetInt8()))
	}
	for _, i := range fields.integers16 {
		elem.Field(i).SetInt(int64(serializer.GetInt16()))
	}
	for _, i := range fields.integers32 {
		elem.Field(i).SetInt(int64(serializer.GetInt32()))
	}
	for _, i := range fields.integers64 {
		elem.Field(i).SetInt(serializer.GetInt64())
	}
	for _, i := range fields.booleans {
		elem.Field(i).SetBool(serializer.GetBool())
	}
	for _, i := range fields.floats32 {
		elem.Field(i).SetFloat(float64(serializer.GetFloat32()))
	}
	for _, i := range fields.floats64 {
		elem.Field(i).SetFloat(serializer.GetFloat64())
	}
	for _, i := range fields.times {
		elem.Field(i).Set(reflect.ValueOf(time.Unix(int64(serializer.GetUInt32()), 0)))
	}
	if fields.fakeDelete > 0 {
		elem.Field(fields.fakeDelete).SetBool(serializer.GetBool())
	}
	k := 0
	for _, i := range fields.refs8 {
		orm.deserializeRef(elem, i, k, engine.registry, fields, uint64(serializer.GetUInt8()))
		k++
	}
	for _, i := range fields.refs16 {
		orm.deserializeRef(elem, i, k, engine.registry, fields, uint64(serializer.GetUInt16()))
		k++
	}
	for _, i := range fields.refs32 {
		orm.deserializeRef(elem, i, k, engine.registry, fields, uint64(serializer.GetUInt32()))
		k++
	}
	for _, i := range fields.refs64 {
		orm.deserializeRef(elem, i, k, engine.registry, fields, serializer.GetUInt64())
		k++
	}
	for _, i := range fields.uintegers8Nullable {
		if serializer.GetBool() {
			v := serializer.GetUInt8()
			elem.Field(i).Set(reflect.ValueOf(&v))
		}
		f := elem.Field(i)
		if !f.IsNil() {
			var v *uint8
			f.Set(reflect.ValueOf(&v))
		}
	}
	for _, i := range fields.uintegers16Nullable {
		if serializer.GetBool() {
			v := serializer.GetUInt16()
			elem.Field(i).Set(reflect.ValueOf(&v))
		}
		f := elem.Field(i)
		if !f.IsNil() {
			var v *uint16
			f.Set(reflect.ValueOf(&v))
		}
	}
	for _, i := range fields.uintegers32Nullable {
		if serializer.GetBool() {
			v := serializer.GetUInt32()
			elem.Field(i).Set(reflect.ValueOf(&v))
		}
		f := elem.Field(i)
		if !f.IsNil() {
			var v *uint32
			f.Set(reflect.ValueOf(&v))
		}
	}
	for _, i := range fields.uintegers64Nullable {
		if serializer.GetBool() {
			v := serializer.GetUInt64()
			elem.Field(i).Set(reflect.ValueOf(&v))
		}
		f := elem.Field(i)
		if !f.IsNil() {
			var v *uint64
			f.Set(reflect.ValueOf(&v))
		}
	}
	for _, i := range fields.integers8Nullable {
		if serializer.GetBool() {
			v := serializer.GetInt8()
			elem.Field(i).Set(reflect.ValueOf(&v))
		}
		f := elem.Field(i)
		if !f.IsNil() {
			var v *int8
			f.Set(reflect.ValueOf(&v))
		}
	}
	for _, i := range fields.integers16Nullable {
		if serializer.GetBool() {
			v := serializer.GetInt16()
			elem.Field(i).Set(reflect.ValueOf(&v))
		}
		f := elem.Field(i)
		if !f.IsNil() {
			var v *int16
			f.Set(reflect.ValueOf(&v))
		}
	}
	for _, i := range fields.integers32Nullable {
		if serializer.GetBool() {
			v := serializer.GetInt32()
			elem.Field(i).Set(reflect.ValueOf(&v))
		}
		f := elem.Field(i)
		if !f.IsNil() {
			var v *int32
			f.Set(reflect.ValueOf(&v))
		}
	}
	for _, i := range fields.integers64Nullable {
		if serializer.GetBool() {
			v := serializer.GetInt64()
			elem.Field(i).Set(reflect.ValueOf(&v))
		}
		f := elem.Field(i)
		if !f.IsNil() {
			var v *int64
			f.Set(reflect.ValueOf(&v))
		}
	}
	for _, i := range fields.stringsEnums {
		index := serializer.GetUInt8()
		if index == 0 {
			elem.Field(i).SetString("")
		} else {
			elem.Field(i).SetString(fields.enums[k].GetFields()[index-1])
		}
		k++
	}
	for _, i := range fields.strings {
		elem.Field(i).SetString(serializer.GetString())
	}
	for _, i := range fields.bytes {
		elem.Field(i).SetBytes(serializer.GetBytes())
	}
	for _, i := range fields.sliceStringsSets {
		l := int(serializer.GetUvarint())
		f := elem.Field(i)
		if l == 0 {
			if !f.IsNil() {
				f.Set(reflect.Zero(f.Type()))
			}
		} else {
			enum := fields.enums[k]
			v := make([]string, l)
			for j := 0; j < l; j++ {
				v[j] = enum.GetFields()[serializer.GetInt8()-1]
			}
			f.Set(reflect.ValueOf(v))
		}
		k++
	}
	for _, i := range fields.booleansNullable {
		if serializer.GetBool() {
			v := serializer.GetBool()
			elem.Field(i).Set(reflect.ValueOf(&v))
		}
		f := elem.Field(i)
		if !f.IsNil() {
			var v *bool
			f.Set(reflect.ValueOf(&v))
		}
	}
	for _, i := range fields.floats32Nullable {
		if serializer.GetBool() {
			v := serializer.GetFloat32()
			elem.Field(i).Set(reflect.ValueOf(&v))
		}
		f := elem.Field(i)
		if !f.IsNil() {
			var v *float32
			f.Set(reflect.ValueOf(&v))
		}
	}
	for _, i := range fields.floats64Nullable {
		if serializer.GetBool() {
			v := serializer.GetFloat64()
			elem.Field(i).Set(reflect.ValueOf(&v))
		}
		f := elem.Field(i)
		if !f.IsNil() {
			var v *float64
			f.Set(reflect.ValueOf(&v))
		}
	}
	for _, i := range fields.timesNullable {
		if serializer.GetBool() {
			v := time.Unix(int64(serializer.GetUInt32()), 0)
			elem.Field(i).Set(reflect.ValueOf(&v))
		}
		f := elem.Field(i)
		if !f.IsNil() {
			var v *time.Time
			f.Set(reflect.ValueOf(&v))
		}
	}
	for i, subField := range fields.structs {
		orm.deserializeFields(engine, subField, elem.Field(i).Elem())
	}
	for _, i := range fields.jsons {
		bytes := serializer.GetBytes()
		f := elem.Field(i)
		if bytes != nil {
			v := reflect.New(f.Type()).Interface()
			_ = jsoniter.ConfigFastest.Unmarshal(bytes, v)
			f.Set(reflect.ValueOf(v).Elem())
		} else {
			if !f.IsNil() {
				f.Set(reflect.Zero(f.Type()))
			}
		}
	}
	k = 0
	for _, i := range fields.refsMany8 {
		orm.deserializeRefMany(8, elem, serializer, i, k, engine.registry, fields)
		k++
	}
	for _, i := range fields.refsMany16 {
		orm.deserializeRefMany(16, elem, serializer, i, k, engine.registry, fields)
		k++
	}
	for _, i := range fields.refsMany32 {
		orm.deserializeRefMany(32, elem, serializer, i, k, engine.registry, fields)
		k++
	}
	for _, i := range fields.refsMany64 {
		orm.deserializeRefMany(64, elem, serializer, i, k, engine.registry, fields)
		k++
	}
}

func (orm *ORM) deserializeRef(elem reflect.Value, i, k int, registry *validatedRegistry, fields *tableFields, id uint64) {
	f := elem.Field(i)
	if id > 0 {
		e := getTableSchema(registry, fields.refsTypes[k]).newEntity()
		o := e.getORM()
		o.idElem.SetUint(id)
		o.inDB = true
		f.Set(o.value)
	} else if !f.IsNil() {
		elem.Field(i).Set(reflect.Zero(fields.refsTypes[k]))
	}
}

func (orm *ORM) deserializeRefMany(size int, elem reflect.Value, serializer *serializer, i, k int, registry *validatedRegistry, fields *tableFields) {
	l := int(serializer.GetUvarint())
	f := elem.Field(i)
	refType := fields.refsManyTypes[k]
	if l > 0 {
		slice := reflect.MakeSlice(reflect.SliceOf(refType), l, l)
		for j := 0; j < l; j++ {
			e := getTableSchema(registry, fields.refsTypes[k]).newEntity()
			o := e.getORM()
			switch size {
			case 8:
				o.idElem.SetUint(uint64(serializer.GetUInt8()))
			case 16:
				o.idElem.SetUint(uint64(serializer.GetUInt16()))
			case 32:
				o.idElem.SetUint(uint64(serializer.GetUInt32()))
			default:
				o.idElem.SetUint(serializer.GetUInt64())
			}
			o.inDB = true
			slice.Index(j).Set(o.value)
			f.Set(o.value)
		}
	} else {
		if !f.IsNil() {
			f.Set(reflect.Zero(reflect.SliceOf(refType)))
		}
	}
}

func (orm *ORM) SetField(field string, value interface{}) error {
	asString, isString := value.(string)
	if isString {
		asString = strings.ToLower(asString)
		if asString == "nil" || asString == "null" {
			value = nil
		}
	}
	if !orm.elem.IsValid() {
		return errors.New("entity is not loaded")
	}
	f := orm.elem.FieldByName(field)
	if !f.IsValid() {
		return fmt.Errorf("field %s not found", field)
	}
	if !f.CanSet() {
		return fmt.Errorf("field %s is not public", field)
	}
	typeName := f.Type().String()
	switch typeName {
	case "uint",
		"uint8",
		"uint16",
		"uint32",
		"uint64":
		val := uint64(0)
		if value != nil {
			parsed, err := strconv.ParseUint(fmt.Sprintf("%v", value), 10, 64)
			if err != nil {
				return fmt.Errorf("%s value %v not valid", field, value)
			}
			val = parsed
		}
		f.SetUint(val)
	case "*uint",
		"*uint8",
		"*uint16",
		"*uint32",
		"*uint64":
		if value != nil {
			val := uint64(0)
			parsed, err := strconv.ParseUint(fmt.Sprintf("%v", reflect.Indirect(reflect.ValueOf(value)).Interface()), 10, 64)
			if err != nil {
				return fmt.Errorf("%s value %v not valid", field, value)
			}
			val = parsed
			switch typeName {
			case "*uint":
				v := uint(val)
				f.Set(reflect.ValueOf(&v))
			case "*uint8":
				v := uint8(val)
				f.Set(reflect.ValueOf(&v))
			case "*uint16":
				v := uint16(val)
				f.Set(reflect.ValueOf(&v))
			case "*uint32":
				v := uint32(val)
				f.Set(reflect.ValueOf(&v))
			default:
				f.Set(reflect.ValueOf(&val))
			}
		} else {
			f.Set(reflect.Zero(f.Type()))
		}
	case "int",
		"int8",
		"int16",
		"int32",
		"int64":
		val := int64(0)
		if value != nil {
			parsed, err := strconv.ParseInt(fmt.Sprintf("%v", value), 10, 64)
			if err != nil {
				return fmt.Errorf("%s value %v not valid", field, value)
			}
			val = parsed
		}
		f.SetInt(val)
	case "*int",
		"*int8",
		"*int16",
		"*int32",
		"*int64":
		if value != nil {
			val := int64(0)
			parsed, err := strconv.ParseInt(fmt.Sprintf("%v", reflect.Indirect(reflect.ValueOf(value)).Interface()), 10, 64)
			if err != nil {
				return fmt.Errorf("%s value %v not valid", field, value)
			}
			val = parsed
			switch typeName {
			case "*int":
				v := int(val)
				f.Set(reflect.ValueOf(&v))
			case "*int8":
				v := int8(val)
				f.Set(reflect.ValueOf(&v))
			case "*int16":
				v := int16(val)
				f.Set(reflect.ValueOf(&v))
			case "*int32":
				v := int32(val)
				f.Set(reflect.ValueOf(&v))
			default:
				f.Set(reflect.ValueOf(&val))
			}
		} else {
			f.Set(reflect.Zero(f.Type()))
		}
	case "string":
		if value == nil {
			f.SetString("")
		} else {
			f.SetString(fmt.Sprintf("%v", value))
		}
	case "[]string":
		_, ok := value.([]string)
		if !ok {
			return fmt.Errorf("%s value %v not valid", field, value)
		}
		f.Set(reflect.ValueOf(value))
	case "[]uint8":
		_, ok := value.([]uint8)
		if !ok {
			return fmt.Errorf("%s value %v not valid", field, value)
		}
		f.Set(reflect.ValueOf(value))
	case "bool":
		val := false
		asString := strings.ToLower(fmt.Sprintf("%v", value))
		if asString == "true" || asString == "1" {
			val = true
		}
		f.SetBool(val)
	case "*bool":
		if value == nil {
			f.Set(reflect.Zero(f.Type()))
		} else {
			val := false
			asString := strings.ToLower(fmt.Sprintf("%v", reflect.Indirect(reflect.ValueOf(value)).Interface()))
			if asString == "true" || asString == "1" {
				val = true
			}
			f.Set(reflect.ValueOf(&val))
		}
	case "float32",
		"float64":
		val := float64(0)
		if value != nil {
			valueString := fmt.Sprintf("%v", value)
			valueString = strings.ReplaceAll(valueString, ",", ".")
			parsed, err := strconv.ParseFloat(valueString, 64)
			if err != nil {
				return fmt.Errorf("%s value %v is not valid", field, value)
			}
			val = parsed
		}
		f.SetFloat(val)
	case "*float32",
		"*float64":
		if value == nil {
			f.Set(reflect.Zero(f.Type()))
		} else {
			val := float64(0)
			valueString := fmt.Sprintf("%v", reflect.Indirect(reflect.ValueOf(value)).Interface())
			valueString = strings.ReplaceAll(valueString, ",", ".")
			parsed, err := strconv.ParseFloat(valueString, 64)
			if err != nil {
				return fmt.Errorf("%s value %v is not valid", field, value)
			}
			val = parsed
			f.Set(reflect.ValueOf(&val))
		}
	case "*time.Time":
		if value == nil {
			f.Set(reflect.Zero(f.Type()))
		} else {
			_, ok := value.(*time.Time)
			if !ok {
				return fmt.Errorf("%s value %v is not valid", field, value)
			}
			f.Set(reflect.ValueOf(value))
		}
	case "time.Time":
		_, ok := value.(time.Time)
		if !ok {
			return fmt.Errorf("%s value %v is not valid", field, value)
		}
		f.Set(reflect.ValueOf(value))
	default:
		k := f.Type().Kind().String()
		if k == "struct" || k == "slice" {
			f.Set(reflect.ValueOf(value))
		} else if k == "ptr" {
			modelType := reflect.TypeOf((*Entity)(nil)).Elem()
			if f.Type().Implements(modelType) {
				if value == nil || (isString && (value == "" || value == "0")) {
					f.Set(reflect.Zero(f.Type()))
				} else {
					asEntity, ok := value.(Entity)
					if ok {
						f.Set(reflect.ValueOf(asEntity))
					} else {
						id, err := strconv.ParseUint(fmt.Sprintf("%v", value), 10, 64)
						if err != nil {
							return fmt.Errorf("%s value %v is not valid", field, value)
						}
						if id == 0 {
							f.Set(reflect.Zero(f.Type()))
						} else {
							val := reflect.New(f.Type().Elem())
							val.Elem().FieldByName("ID").SetUint(id)
							f.Set(val)
						}
					}
				}
			} else {
				return fmt.Errorf("field %s is not supported", field)
			}
		} else {
			return fmt.Errorf("field %s is not supported", field)
		}
	}
	return nil
}

func (orm *ORM) checkNil(field reflect.Value, name string, hasOld bool, old interface{}, bind Bind, updateBind map[string]string) bool {
	isNil := field.IsZero()
	if isNil {
		if hasOld && old == nil {
			return false
		}
		bind[name] = nil
		if updateBind != nil {
			updateBind[name] = "NULL"
		}
		return false
	}
	return true
}

func (orm *ORM) buildBind(id uint64, serializer *serializer, bind, oldBind Bind, updateBind map[string]string, tableSchema *tableSchema,
	fields *tableFields, value reflect.Value, prefix string) {
	hasUpdate := updateBind != nil
	noPrefix := prefix == ""
	var hasOld = orm.inDB
	serializer.Reset(orm.binary)
	for _, i := range fields.uintegers8 {
		if i == 1 && noPrefix {
			continue
		}
		val := uint8(value.Field(i).Uint())
		if hasOld {
			if hasOld && serializer.GetUInt8() == val {
				continue
			}
		}
		name := prefix + fields.fields[i].Name
		bind[name] = val
		if hasUpdate {
			updateBind[name] = strconv.FormatUint(uint64(val), 10)
		}
	}
	for _, i := range fields.uintegers16 {
		if i == 1 && noPrefix {
			continue
		}
		val := uint16(value.Field(i).Uint())
		if hasOld {
			if hasOld && serializer.GetUInt16() == val {
				continue
			}
		}
		name := prefix + fields.fields[i].Name
		bind[name] = val
		if hasUpdate {
			updateBind[name] = strconv.FormatUint(uint64(val), 10)
		}
	}
	for _, i := range fields.uintegers32 {
		if i == 1 && noPrefix {
			continue
		}
		val := uint32(value.Field(i).Uint())
		if hasOld {
			if hasOld && serializer.GetUInt32() == val {
				continue
			}
		}
		name := prefix + fields.fields[i].Name
		bind[name] = val
		if hasUpdate {
			updateBind[name] = strconv.FormatUint(uint64(val), 10)
		}
	}
	for _, i := range fields.uintegers64 {
		if i == 1 && noPrefix {
			continue
		}
		val := value.Field(i).Uint()
		if hasOld {
			if hasOld && serializer.GetUInt64() == val {
				continue
			}
		}
		name := prefix + fields.fields[i].Name
		bind[name] = val
		if hasUpdate {
			updateBind[name] = strconv.FormatUint(val, 10)
		}
	}
	for _, i := range fields.integers8 {
		val := int8(value.Field(i).Int())
		if hasOld {
			if hasOld && serializer.GetInt8() == val {
				continue
			}
		}
		name := prefix + fields.fields[i].Name
		bind[name] = val
		if hasUpdate {
			updateBind[name] = strconv.FormatInt(int64(val), 10)
		}
	}
	for _, i := range fields.integers16 {
		val := int16(value.Field(i).Int())
		if hasOld {
			if hasOld && serializer.GetInt16() == val {
				continue
			}
		}
		name := prefix + fields.fields[i].Name
		bind[name] = val
		if hasUpdate {
			updateBind[name] = strconv.FormatInt(int64(val), 10)
		}
	}
	for _, i := range fields.integers32 {
		val := int32(value.Field(i).Int())
		if hasOld {
			if hasOld && serializer.GetInt32() == val {
				continue
			}
		}
		name := prefix + fields.fields[i].Name
		bind[name] = val
		if hasUpdate {
			updateBind[name] = strconv.FormatInt(int64(val), 10)
		}
	}
	for _, i := range fields.integers64 {
		val := value.Field(i).Int()
		if hasOld {
			if hasOld && serializer.GetInt64() == val {
				continue
			}
		}
		name := prefix + fields.fields[i].Name
		bind[name] = val
		if hasUpdate {
			updateBind[name] = strconv.FormatInt(val, 10)
		}
	}
	for _, i := range fields.booleans {
		val := value.Field(i).Bool()
		if hasOld {
			if hasOld && serializer.GetBool() == val {
				continue
			}
		}
		name := prefix + fields.fields[i].Name
		bind[name] = val
		if hasUpdate {
			if val {
				updateBind[name] = "1"
			} else {
				updateBind[name] = "0"
			}
		}
	}
}

//func (orm *ORM) fillBindToRemove(id uint64, bind Bind, updateBind map[string]string, tableSchema *tableSchema,
//	fields *tableFields, value reflect.Value, prefix string) {
//	var hasOld = orm.inDB
//	hasUpdate := updateBind != nil
//	for _, i := range fields.uintegersNullable {
//		field, name, old := orm.prepareFieldBind(prefix, tableSchema, fields, value, i)
//		if !orm.checkNil(field, name, hasOld, old, bind, updateBind) {
//			continue
//		}
//		val := field.Elem().Uint()
//		if hasOld && old == val {
//			continue
//		}
//		bind[name] = val
//		if hasUpdate {
//			updateBind[name] = strconv.FormatUint(val, 10)
//		}
//	}
//	for _, i := range fields.integersNullable {
//		field, name, old := orm.prepareFieldBind(prefix, tableSchema, fields, value, i)
//		if !orm.checkNil(field, name, hasOld, old, bind, updateBind) {
//			continue
//		}
//		val := field.Elem().Int()
//		if hasOld && old == val {
//			continue
//		}
//		bind[name] = val
//		if hasUpdate {
//			updateBind[name] = strconv.FormatInt(val, 10)
//		}
//	}
//	for _, i := range fields.strings {
//		field, name, old := orm.prepareFieldBind(prefix, tableSchema, fields, value, i)
//		value := field.String()
//		if hasOld && (old == value || (old == nil && value == "")) {
//			continue
//		}
//		if value != "" {
//			bind[name] = value
//			if hasUpdate {
//				updateBind[name] = orm.escapeSQLParam(value)
//			}
//		} else {
//			attributes := tableSchema.tags[name]
//			required, hasRequired := attributes["required"]
//			if hasRequired && required == "true" {
//				bind[name] = ""
//				if hasUpdate {
//					updateBind[name] = "''"
//				}
//			} else {
//				bind[name] = nil
//				if hasUpdate {
//					updateBind[name] = "NULL"
//				}
//			}
//		}
//	}
//	for _, i := range fields.bytes {
//		field, name, old := orm.prepareFieldBind(prefix, tableSchema, fields, value, i)
//		value := field.Bytes()
//		valueAsString := string(value)
//		if hasOld && ((old == nil && valueAsString == "") || (old != nil && old.(string) == valueAsString)) {
//			continue
//		}
//		if valueAsString == "" {
//			bind[name] = nil
//			if hasUpdate {
//				updateBind[name] = "NULL"
//			}
//		} else {
//			bind[name] = valueAsString
//			if hasUpdate {
//				updateBind[name] = orm.escapeSQLParam(valueAsString)
//			}
//		}
//	}
//	if fields.fakeDelete > 0 {
//		field, name, old := orm.prepareFieldBind(prefix, tableSchema, fields, value, fields.fakeDelete)
//		value := uint64(0)
//		if field.Bool() {
//			value = id
//		}
//		if !hasOld || old != value {
//			bind[name] = value
//			if hasUpdate {
//				updateBind[name] = strconv.FormatUint(value, 10)
//			}
//		}
//	}
//	for _, i := range fields.booleans {
//		field, name, old := orm.prepareFieldBind(prefix, tableSchema, fields, value, i)
//		value := field.Bool()
//		if hasOld && old == value {
//			continue
//		}
//		bind[name] = value
//		if hasUpdate {
//			if value {
//				updateBind[name] = "1"
//			} else {
//				updateBind[name] = "0"
//			}
//		}
//	}
//	for _, i := range fields.booleansNullable {
//		field, name, old := orm.prepareFieldBind(prefix, tableSchema, fields, value, i)
//		if !orm.checkNil(field, name, hasOld, old, bind, updateBind) {
//			continue
//		}
//		value := field.Elem().Bool()
//		if hasOld && old == value {
//			continue
//		}
//		bind[name] = value
//		if hasUpdate {
//			if value {
//				updateBind[name] = "1"
//			} else {
//				updateBind[name] = "0"
//			}
//		}
//	}
//	for _, i := range fields.floats {
//		field, name, old := orm.prepareFieldBind(prefix, tableSchema, fields, value, i)
//		val := field.Float()
//		precision := 16
//		fieldAttributes := tableSchema.tags[name]
//		precisionAttribute, has := fieldAttributes["precision"]
//		if has {
//			userPrecision, _ := strconv.Atoi(precisionAttribute)
//			precision = userPrecision
//		}
//		attributes := tableSchema.tags[name]
//		decimal, has := attributes["decimal"]
//		if has {
//			decimalArgs := strings.Split(decimal, ",")
//			size, _ := strconv.ParseFloat(decimalArgs[1], 64)
//			sizeNumber := math.Pow(10, size)
//			val = math.Round(val*sizeNumber) / sizeNumber
//			if hasOld {
//				valOld := math.Round(old.(float64)*sizeNumber) / sizeNumber
//				if val == valOld {
//					continue
//				}
//			}
//		} else {
//			sizeNumber := math.Pow(10, float64(precision))
//			val = math.Round(val*sizeNumber) / sizeNumber
//			if hasOld {
//				valOld := math.Round(old.(float64)*sizeNumber) / sizeNumber
//				if valOld == val {
//					continue
//				}
//			}
//		}
//		bind[name] = val
//		if hasUpdate {
//			updateBind[name] = strconv.FormatFloat(val, 'f', -1, 64)
//		}
//	}
//	for _, i := range fields.floatsNullable {
//		field, name, old := orm.prepareFieldBind(prefix, tableSchema, fields, value, i)
//		if !orm.checkNil(field, name, hasOld, old, bind, updateBind) {
//			continue
//		}
//		var val float64
//		isZero := field.IsZero()
//		if !isZero {
//			val = field.Elem().Float()
//		}
//		precision := 10
//		fieldAttributes := tableSchema.tags[name]
//		precisionAttribute, has := fieldAttributes["precision"]
//		if has {
//			userPrecision, _ := strconv.Atoi(precisionAttribute)
//			precision = userPrecision
//		}
//		attributes := tableSchema.tags[name]
//		decimal, has := attributes["decimal"]
//		if has {
//			decimalArgs := strings.Split(decimal, ",")
//			size, _ := strconv.ParseFloat(decimalArgs[1], 64)
//			sizeNumber := math.Pow(10, size)
//			val = math.Round(val*sizeNumber) / sizeNumber
//			if hasOld && old != nil {
//				valOld := math.Round(old.(float64)*sizeNumber) / sizeNumber
//				if val == valOld {
//					continue
//				}
//			}
//			bind[name] = val
//			if hasUpdate {
//				updateBind[name] = strconv.FormatFloat(val, 'f', -1, 64)
//			}
//		} else {
//			sizeNumber := math.Pow(10, float64(precision))
//			val = math.Round(val*sizeNumber) / sizeNumber
//			if hasOld && old != nil {
//				valOld := math.Round(old.(float64)*sizeNumber) / sizeNumber
//				if valOld == val {
//					continue
//				}
//			}
//			bind[name] = val
//			if hasUpdate {
//				updateBind[name] = strconv.FormatFloat(val, 'f', -1, 64)
//			}
//		}
//	}
//	for _, i := range fields.times {
//		field, name, old := orm.prepareFieldBind(prefix, tableSchema, fields, value, i)
//		value := field.Interface().(time.Time)
//		layout := "2006-01-02"
//		var valueAsString string
//		if tableSchema.tags[name]["time"] == "true" {
//			if value.Year() == 1 {
//				valueAsString = "0001-01-01 00:00:00"
//			} else {
//				layout += " 15:04:05"
//			}
//		} else if value.Year() == 1 {
//			valueAsString = "0001-01-01"
//		}
//		if valueAsString == "" {
//			valueAsString = value.Format(layout)
//		}
//		if hasOld && old == valueAsString {
//			continue
//		}
//		bind[name] = valueAsString
//		if hasUpdate {
//			updateBind[name] = "'" + valueAsString + "'"
//		}
//	}
//	for _, i := range fields.timesNullable {
//		field, name, old := orm.prepareFieldBind(prefix, tableSchema, fields, value, i)
//		if !orm.checkNil(field, name, hasOld, old, bind, updateBind) {
//			continue
//		}
//		value := field.Interface().(*time.Time)
//		layout := "2006-01-02"
//		var valueAsString string
//		if tableSchema.tags[name]["time"] == "true" {
//			if value != nil {
//				layout += " 15:04:05"
//			}
//		}
//		if value != nil {
//			valueAsString = value.Format(layout)
//		}
//		if hasOld && (old == valueAsString || (valueAsString == "" && (old == nil || old == "nil"))) {
//			continue
//		}
//		if valueAsString == "" {
//			bind[name] = nil
//			if hasUpdate {
//				updateBind[name] = "NULL"
//			}
//		} else {
//			bind[name] = valueAsString
//			if hasUpdate {
//				updateBind[name] = "'" + valueAsString + "'"
//			}
//		}
//	}
//	for _, i := range fields.sliceStringsSets {
//		field, name, old := orm.prepareFieldBind(prefix, tableSchema, fields, value, i)
//		value := field.Interface().([]string)
//		var valueAsString string
//		if value != nil {
//			valueAsString = strings.Join(value, ",")
//		}
//		if hasOld && (old == valueAsString || (valueAsString == "" && old == nil)) {
//			continue
//		}
//		if valueAsString != "" {
//			bind[name] = valueAsString
//			if hasUpdate {
//				updateBind[name] = orm.escapeSQLParam(valueAsString)
//			}
//		} else {
//			attributes := tableSchema.tags[name]
//			required, hasRequired := attributes["required"]
//			if hasRequired && required == "true" {
//				bind[name] = ""
//				if hasUpdate {
//					updateBind[name] = "''"
//				}
//			} else {
//				bind[name] = nil
//				if hasUpdate {
//					updateBind[name] = "NULL"
//				}
//			}
//		}
//	}
//	for i, subFields := range fields.structs {
//		field, _, _ := orm.prepareFieldBind(prefix, tableSchema, fields, value, i)
//		orm.fillBind(0, bind, updateBind, tableSchema, subFields, reflect.ValueOf(field.Interface()), fields.fields[i].Name)
//	}
//	for _, i := range fields.refs {
//		field, name, old := orm.prepareFieldBind(prefix, tableSchema, fields, value, i)
//		value := uint64(0)
//		if !field.IsNil() {
//			value = field.Elem().Field(1).Uint()
//		}
//		if hasOld && (old == value || ((old == nil || old == 0) && value == 0)) {
//			continue
//		}
//		if value == 0 {
//			bind[name] = nil
//			if hasUpdate {
//				updateBind[name] = "NULL"
//			}
//		} else {
//			bind[name] = value
//			if hasUpdate {
//				updateBind[name] = strconv.FormatUint(value, 10)
//			}
//		}
//	}
//	for _, i := range fields.refsMany {
//		field, name, old := orm.prepareFieldBind(prefix, tableSchema, fields, value, i)
//		if !orm.checkNil(field, name, hasOld, old, bind, updateBind) {
//			continue
//		}
//		var valString string
//		length := field.Len()
//		if length > 0 {
//			ids := make([]uint64, length)
//			for i := 0; i < length; i++ {
//				ids[i] = field.Index(i).Interface().(Entity).GetID()
//			}
//			encoded, _ := jsoniter.ConfigFastest.Marshal(ids)
//			valString = string(encoded)
//		}
//		if hasOld && (old == valString || ((old == nil || old == "0") && valString == "")) {
//			continue
//		}
//		if valString == "" {
//			bind[name] = nil
//			if hasUpdate {
//				updateBind[name] = "NULL"
//			}
//		} else {
//			bind[name] = valString
//			if hasUpdate {
//				updateBind[name] = "'" + valString + "'"
//			}
//		}
//	}
//	for _, i := range fields.jsons {
//		field, name, old := orm.prepareFieldBind(prefix, tableSchema, fields, value, i)
//		value := field.Interface()
//		var valString string
//		if !field.IsZero() {
//			var encoded []byte
//			if hasOld && old != nil && old != "" {
//				oldMap := reflect.New(field.Type()).Interface()
//				newMap := reflect.New(field.Type()).Interface()
//				_ = jsoniter.ConfigFastest.UnmarshalFromString(old.(string), oldMap)
//				oldValue := reflect.ValueOf(oldMap).Elem().Interface()
//				encoded, _ = jsoniter.ConfigFastest.Marshal(value)
//				_ = jsoniter.ConfigFastest.Unmarshal(encoded, newMap)
//				newValue := reflect.ValueOf(newMap).Elem().Interface()
//				if cmp.Equal(newValue, oldValue) {
//					continue
//				}
//			} else {
//				encoded, _ = jsoniter.ConfigFastest.Marshal(value)
//			}
//			valString = string(encoded)
//		} else if hasOld && old == nil {
//			continue
//		}
//		if valString != "" {
//			bind[name] = valString
//			if hasUpdate {
//				updateBind[name] = "'" + valString + "'"
//			}
//		} else {
//			attributes := tableSchema.tags[name]
//			required, hasRequired := attributes["required"]
//			if hasRequired && required == "true" {
//				bind[name] = ""
//				if hasUpdate {
//					updateBind[name] = "''"
//				}
//			} else {
//				bind[name] = nil
//				if hasUpdate {
//					updateBind[name] = "NULL"
//				}
//			}
//		}
//	}
//}

func (orm *ORM) escapeSQLParam(val string) string {
	dest := make([]byte, 0, 2*len(val))
	var escape byte
	for i := 0; i < len(val); i++ {
		c := val[i]
		escape = 0
		switch c {
		case 0:
			escape = '0'
		case '\n':
			escape = 'n'
		case '\r':
			escape = 'r'
		case '\\':
			escape = '\\'
		case '\'':
			escape = '\''
		case '"':
			escape = '"'
		case '\032':
			escape = 'Z'
		}
		if escape != 0 {
			dest = append(dest, '\\', escape)
		} else {
			dest = append(dest, c)
		}
	}
	return "'" + string(dest) + "'"
}

func checkError(err error) {
	if err != nil {
		panic(err)
	}
}
