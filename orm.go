package orm

import (
	"database/sql"
	"fmt"
	"math"
	"reflect"
	"strconv"
	"strings"
	"time"

	jsoniter "github.com/json-iterator/go"

	"github.com/pkg/errors"
)

const timeFormat = "2006-01-02 15:04:05"
const dateformat = "2006-01-02"

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
	return getFieldByName(engine, orm.tableSchema, orm.binary, field)
}

func getFieldByName(engine *Engine, tableSchema *tableSchema, binary []byte, field string) interface{} {
	index, has := tableSchema.columnMapping[field]
	if !has {
		panic(fmt.Errorf("uknown field " + field))
	}
	return getField(engine, tableSchema, binary, index)
}

func getField(engine *Engine, tableSchema *tableSchema, binary []byte, index int) interface{} {
	fields := tableSchema.fields
	serializer := engine.getSerializer()
	serializer.Reset(binary)
	v, _, _ := getFieldForStruct(fields, serializer, index, 0)
	return v
}

func getFieldForStruct(fields *tableFields, serializer *serializer, index, i int) (interface{}, bool, int) {
	for range fields.refs {
		v := serializer.GetUInteger()
		if i == index {
			return v, true, i
		}
		i++
	}
	for range fields.uintegers {
		v := serializer.GetUInteger()
		if i == index {
			return v, true, i
		}
		i++
	}
	for range fields.integers {
		v := serializer.GetInteger()
		if i == index {
			return v, true, i
		}
		i++
	}
	for range fields.booleans {
		if i == index {
			return serializer.GetBool(), true, i
		}
		serializer.buffer.Next(1)
		i++
	}
	for range fields.floats {
		v := serializer.GetFloat()
		if i == index {
			return v, true, i
		}
		i++
	}
	for range fields.times {
		v := serializer.GetUInteger()
		if i == index {
			return v, true, i
		}
		i++
	}
	for range fields.dates {
		v := serializer.GetUInteger()
		if i == index {
			return v, true, i
		}
		i++
	}
	if fields.fakeDelete > 0 {
		if i == index {
			return serializer.GetBool(), true, i
		}
		serializer.buffer.Next(1)
		i++
	}
	for range fields.strings {
		if i == index {
			return serializer.GetString(), true, i
		}
		if l := serializer.GetUInteger(); l > 0 {
			serializer.buffer.Next(int(l))
		}
		i++
	}
	for range fields.uintegersNullable {
		isNil := serializer.GetBool()
		if i == index {
			if isNil {
				return nil, true, i
			}
			return serializer.GetUInteger(), true, i
		}
		serializer.GetUInteger()
		i++
	}
	for range fields.integersNullable {
		isNil := serializer.GetBool()
		if i == index {
			if isNil {
				return nil, true, i
			}
			return serializer.GetInteger(), true, i
		}
		serializer.GetInteger()
		i++
	}
	for range fields.stringsEnums {
		v := serializer.GetUInteger()
		if i == index {
			return v, true, i
		}
		i++
	}
	for range fields.bytes {
		if i == index {
			return serializer.GetBytes(), true, i
		}
		if l := serializer.GetUInteger(); l > 0 {
			serializer.buffer.Next(int(l))
		}
		i++
	}
	for range fields.sliceStringsSets {
		l := int(serializer.GetUInteger())
		if i == index {
			val := make([]uint64, l)
			for k := 0; k < l; k++ {
				val[k] = serializer.GetUInteger()
			}
			return val, true, i
		}
		serializer.buffer.Next(l)
		i++
	}
	for range fields.booleansNullable {
		isNil := serializer.GetBool()
		if i == index {
			if isNil {
				return nil, true, i
			}
			return serializer.GetBool(), true, i
		}
		serializer.GetBool()
		i++
	}
	for range fields.floatsNullable {
		isNil := serializer.GetBool()
		if i == index {
			if isNil {
				return nil, true, i
			}
			return serializer.GetFloat(), true, i
		}
		serializer.GetFloat()
		i++
	}
	for range fields.timesNullable {
		isNil := serializer.GetBool()
		if i == index {
			if isNil {
				return nil, true, i
			}
			return serializer.GetInteger(), true, i
		}
		serializer.GetInteger()
		i++
	}
	for range fields.datesNullable {
		isNil := serializer.GetBool()
		if i == index {
			if isNil {
				return nil, true, i
			}
			return serializer.GetInteger(), true, i
		}
		serializer.GetInteger()
		i++
	}
	for range fields.jsons {
		if i == index {
			return serializer.GetBytes(), true, i
		}
		if l := serializer.GetUInteger(); l > 0 {
			serializer.buffer.Next(int(l))
		}
		i++
	}
	for range fields.refsMany {
		l := int(serializer.GetUInteger())
		if i == index {
			val := make([]uint64, l)
			for k := 0; k < l; k++ {
				val[k] = serializer.GetUInteger()
			}
			return val, true, i
		}
		serializer.buffer.Next(l)
		i++
	}
	for _, subFields := range fields.structs {
		v, has, j := getFieldForStruct(subFields, serializer, index, i)
		if has {
			return v, true, j
		}
		i = j
	}
	return nil, false, 0
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
	bind, _, _, _, has = orm.getDirtyBind(engine)
	return bind, has
}

func (orm *ORM) GetDirtyBindFull(engine *Engine) (bind, before Bind, has bool) {
	bind, before, _, _, has = orm.getDirtyBind(engine)
	return bind, before, has
}

func (orm *ORM) getDirtyBind(engine *Engine) (bind, oldBind, current Bind, updateBind map[string]string, has bool) {
	if orm.delete {
		return nil, nil, nil, nil, true
	}
	if orm.fakeDelete {
		if orm.tableSchema.hasFakeDelete {
			orm.elem.FieldByName("FakeDelete").SetBool(true)
		} else {
			orm.delete = true
			return nil, nil, nil, nil, true
		}
	}
	id := orm.GetID()
	bind = make(Bind)
	if orm.tableSchema.cachedIndexesAll != nil {
		current = make(Bind)
	}
	if orm.inDB && !orm.delete {
		oldBind = make(Bind)
		updateBind = make(map[string]string)
	}
	serializer := engine.getSerializer()
	orm.buildBind(id, serializer, bind, oldBind, current, updateBind, orm.tableSchema, orm.tableSchema.fields, orm.elem, "")
	has = id == 0 || len(bind) > 0
	return bind, oldBind, current, updateBind, has
}

func (orm *ORM) serialize(serializer *serializer) {
	orm.serializeFields(serializer, orm.tableSchema.fields, orm.elem)
	orm.binary = serializer.CopyBinary()
	serializer.buffer.Reset()
}

func (orm *ORM) deserializeFromDB(engine *Engine, pointers []interface{}) {
	serializer := engine.getSerializer()
	serializer.buffer.Reset()
	deserializeStructFromDB(serializer, 0, orm.tableSchema.fields, pointers)
	orm.binary = serializer.CopyBinary()
}

func deserializeStructFromDB(serializer *serializer, index int, fields *tableFields, pointers []interface{}) int {
	for range fields.refs {
		serializer.SetUInteger(*pointers[index].(*uint64))
		index++
	}
	for range fields.uintegers {
		serializer.SetUInteger(*pointers[index].(*uint64))
		index++
	}
	for range fields.integers {
		serializer.SetInteger(*pointers[index].(*int64))
		index++
	}
	for range fields.booleans {
		serializer.SetBool(*pointers[index].(*bool))
		index++
	}
	for range fields.floats {
		serializer.SetFloat(*pointers[index].(*float64))
		index++
	}
	for range fields.times {
		serializer.SetUInteger(*pointers[index].(*uint64))
		index++
	}
	for range fields.dates {
		serializer.SetUInteger(*pointers[index].(*uint64))
		index++
	}
	if fields.fakeDelete > 0 {
		serializer.SetBool(*pointers[index].(*uint64) > 0)
		index++
	}
	for range fields.strings {
		serializer.SetString(pointers[index].(*sql.NullString).String)
		index++
	}
	for range fields.uintegersNullable {
		v := pointers[index].(*sql.NullInt64)
		serializer.SetBool(v.Valid)
		if v.Valid {
			serializer.SetUInteger(uint64(v.Int64))
		}
		index++
	}
	for range fields.integersNullable {
		v := pointers[index].(*sql.NullInt64)
		serializer.SetBool(v.Valid)
		if v.Valid {
			serializer.SetInteger(v.Int64)
		}
		index++
	}
	k := 0
	for range fields.stringsEnums {
		v := pointers[index].(*sql.NullString)
		if v.Valid {
			serializer.SetUInteger(uint64(fields.enums[k].Index(v.String)))
		} else {
			serializer.SetUInteger(0)
		}
		index++
		k++
	}
	for range fields.bytes {
		serializer.SetBytes([]byte(pointers[index].(*sql.NullString).String))
		index++
	}
	for range fields.sliceStringsSets {
		v := pointers[index].(*sql.NullString)
		if v.Valid {
			values := strings.Split(v.String, ",")
			serializer.SetUInteger(uint64(len(values)))
			enum := fields.enums[k]
			for _, set := range values {
				serializer.SetUInteger(uint64(enum.Index(set)))
			}
		} else {
			serializer.SetUInteger(0)
		}
		k++
	}
	for range fields.booleansNullable {
		v := pointers[index].(*sql.NullBool)
		serializer.SetBool(v.Valid)
		if v.Valid {
			serializer.SetBool(v.Bool)
		}
		index++
	}
	for range fields.floatsNullable {
		v := pointers[index].(*sql.NullFloat64)
		serializer.SetBool(v.Valid)
		if v.Valid {
			serializer.SetFloat(v.Float64)
		}
		index++
	}
	for range fields.timesNullable {
		v := pointers[index].(*sql.NullInt64)
		serializer.SetBool(v.Valid)
		if v.Valid {
			serializer.SetUInteger(uint64(v.Int64))
		}
		index++
	}
	for range fields.datesNullable {
		v := pointers[index].(*sql.NullInt64)
		serializer.SetBool(v.Valid)
		if v.Valid {
			serializer.SetUInteger(uint64(v.Int64))
		}
		index++
	}
	for range fields.jsons {
		v := pointers[index].(*sql.NullString)
		if v.Valid {
			serializer.SetBytes([]byte(v.String))
		} else {
			serializer.SetBytes(nil)
		}
		index++
	}
	for range fields.refsMany {
		v := pointers[index].(*sql.NullString)
		if v.Valid {
			var slice []uint8
			_ = jsoniter.ConfigFastest.UnmarshalFromString(v.String, &slice)
			serializer.SetUInteger(uint64(len(slice)))
			for _, i := range slice {
				serializer.SetUInteger(uint64(i))
			}
		} else {
			serializer.SetUInteger(0)
		}
	}
	for _, subField := range fields.structs {
		index += deserializeStructFromDB(serializer, index, subField, pointers)
	}
	return index
}

func (orm *ORM) serializeFields(serializer *serializer, fields *tableFields, elem reflect.Value) {
	for _, i := range fields.refs {
		f := elem.Field(i)
		id := uint64(0)
		if !f.IsNil() {
			id = f.Elem().Field(1).Uint()
		}
		serializer.SetUInteger(id)
	}
	for _, i := range fields.uintegers {
		serializer.SetUInteger(elem.Field(i).Uint())
	}
	for _, i := range fields.integers {
		serializer.SetInteger(elem.Field(i).Int())
	}
	for _, i := range fields.booleans {
		serializer.SetBool(elem.Field(i).Bool())
	}
	for i := range fields.floats {
		serializer.SetFloat(elem.Field(i).Float())
	}
	for _, i := range fields.times {
		serializer.SetUInteger(uint64(elem.Field(i).Interface().(time.Time).Unix()))
	}
	for _, i := range fields.dates {
		serializer.SetUInteger(uint64(elem.Field(i).Interface().(time.Time).Unix()))
	}
	if fields.fakeDelete > 0 {
		serializer.SetBool(elem.Field(fields.fakeDelete).Bool())
	}
	for _, i := range fields.strings {
		serializer.SetString(elem.Field(i).String())
	}
	for _, i := range fields.uintegersNullable {
		f := elem.Field(i)
		if f.IsNil() {
			serializer.SetBool(false)
		} else {
			serializer.SetBool(true)
			serializer.SetUInteger(f.Uint())
		}
	}
	for _, i := range fields.integersNullable {
		f := elem.Field(i)
		if f.IsNil() {
			serializer.SetBool(false)
		} else {
			serializer.SetBool(true)
			serializer.SetInteger(f.Int())
		}
	}
	k := 0
	for _, i := range fields.stringsEnums {
		val := elem.Field(i).String()
		if val == "" {
			serializer.SetUInteger(0)
		} else {
			serializer.SetUInteger(uint64(fields.enums[k].Index(val)))
		}
		k++
	}
	for _, i := range fields.bytes {
		serializer.SetBytes(elem.Field(i).Bytes())
	}
	k = 0
	for _, i := range fields.sliceStringsSets {
		f := elem.Field(i)
		values := f.Interface().([]string)
		l := len(values)
		serializer.SetUInteger(uint64(l))
		if l > 0 {
			set := fields.sets[k]
			for _, val := range values {
				serializer.SetUInteger(uint64(set.Index(val)))
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
	for i := range fields.floatsNullable {
		f := elem.Field(i)
		if f.IsNil() {
			serializer.SetBool(false)
		} else {
			serializer.SetBool(true)
			serializer.SetFloat(f.Float())
		}
	}
	for _, i := range fields.timesNullable {
		f := elem.Field(i)
		if f.IsNil() {
			serializer.SetBool(false)
		} else {
			serializer.SetBool(true)
			serializer.SetUInteger(uint64(f.Interface().(*time.Time).Unix()))
		}
	}
	for _, i := range fields.datesNullable {
		f := elem.Field(i)
		if f.IsNil() {
			serializer.SetBool(false)
		} else {
			serializer.SetBool(true)
			serializer.SetUInteger(uint64(elem.Field(i).Interface().(time.Time).Unix()))
		}
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
	for _, i := range fields.refsMany {
		e := elem.Field(i)
		if e.IsNil() {
			serializer.SetUInteger(0)
		} else {
			l := e.Len()
			serializer.SetUInteger(uint64(l))
			for k := 0; k < l; k++ {
				serializer.SetUInteger(e.Index(k).Elem().Field(1).Uint())
			}
		}
	}
	for i, subField := range fields.structs {
		orm.serializeFields(serializer, subField, elem.Field(i).Elem())
	}
}

func (orm *ORM) deserialize(engine *Engine) {
	orm.deserializeFields(engine, orm.tableSchema.fields, orm.elem)
}

func (orm *ORM) deserializeFields(engine *Engine, fields *tableFields, elem reflect.Value) {
	serializer := engine.getSerializer()
	k := 0
	for _, i := range fields.refs {
		id := serializer.GetUInteger()
		f := elem.Field(i)
		if id > 0 {
			e := getTableSchema(engine.registry, fields.refsTypes[k]).newEntity()
			o := e.getORM()
			o.idElem.SetUint(id)
			o.inDB = true
			f.Set(o.value)
		} else if !f.IsNil() {
			elem.Field(i).Set(reflect.Zero(fields.refsTypes[k]))
		}
		k++
	}
	for _, i := range fields.uintegers {
		elem.Field(i).SetUint(serializer.GetUInteger())
	}
	for _, i := range fields.integers {
		elem.Field(i).SetInt(serializer.GetInteger())
	}
	for _, i := range fields.booleans {
		elem.Field(i).SetBool(serializer.GetBool())
	}
	for i := range fields.floats {
		elem.Field(i).SetFloat(serializer.GetFloat())
	}
	for _, i := range fields.times {
		elem.Field(i).Set(reflect.ValueOf(time.Unix(int64(serializer.GetUInteger()), 0)))
	}
	for _, i := range fields.dates {
		elem.Field(i).Set(reflect.ValueOf(time.Unix(int64(serializer.GetUInteger()), 0)))
	}
	if fields.fakeDelete > 0 {
		elem.Field(fields.fakeDelete).SetBool(serializer.GetBool())
	}
	for _, i := range fields.strings {
		elem.Field(i).SetString(serializer.GetString())
	}
	for _, i := range fields.uintegersNullable {
		if serializer.GetBool() {
			v := serializer.GetUInteger()
			elem.Field(i).Set(reflect.ValueOf(&v))
		}
		f := elem.Field(i)
		if !f.IsNil() {
			var v *uint8
			f.Set(reflect.ValueOf(&v))
		}
	}
	for _, i := range fields.integersNullable {
		if serializer.GetBool() {
			v := serializer.GetInteger()
			elem.Field(i).Set(reflect.ValueOf(&v))
		}
		f := elem.Field(i)
		if !f.IsNil() {
			var v *int8
			f.Set(reflect.ValueOf(&v))
		}
	}
	for _, i := range fields.stringsEnums {
		index := serializer.GetUInteger()
		if index == 0 {
			elem.Field(i).SetString("")
		} else {
			elem.Field(i).SetString(fields.enums[k].GetFields()[index-1])
		}
		k++
	}
	for _, i := range fields.bytes {
		elem.Field(i).SetBytes(serializer.GetBytes())
	}
	for _, i := range fields.sliceStringsSets {
		l := int(serializer.GetUInteger())
		f := elem.Field(i)
		if l == 0 {
			if !f.IsNil() {
				f.Set(reflect.Zero(f.Type()))
			}
		} else {
			enum := fields.enums[k]
			v := make([]string, l)
			for j := 0; j < l; j++ {
				v[j] = enum.GetFields()[serializer.GetUInteger()-1]
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
	for i := range fields.floatsNullable {
		if serializer.GetBool() {
			v := serializer.GetFloat()
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
			v := time.Unix(int64(serializer.GetUInteger()), 0)
			elem.Field(i).Set(reflect.ValueOf(&v))
		}
		f := elem.Field(i)
		if !f.IsNil() {
			var v *time.Time
			f.Set(reflect.ValueOf(&v))
		}
	}
	for _, i := range fields.datesNullable {
		if serializer.GetBool() {
			v := time.Unix(int64(serializer.GetUInteger()), 0)
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
	for _, i := range fields.refsMany {
		l := int(serializer.GetUInteger())
		f := elem.Field(i)
		refType := fields.refsManyTypes[k]
		if l > 0 {
			slice := reflect.MakeSlice(reflect.SliceOf(refType), l, l)
			for j := 0; j < l; j++ {
				e := getTableSchema(engine.registry, fields.refsTypes[k]).newEntity()
				o := e.getORM()
				o.idElem.SetUint(uint64(serializer.GetUInteger()))
				o.inDB = true
				slice.Index(j).Set(o.value)
			}
			f.Set(slice)
		} else {
			if !f.IsNil() {
				f.Set(reflect.Zero(reflect.SliceOf(refType)))
			}
		}
		k++
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

func (orm *ORM) buildBind(id uint64, serializer *serializer, bind, oldBind, current Bind, updateBind map[string]string, tableSchema *tableSchema,
	fields *tableFields, value reflect.Value, prefix string) {
	hasUpdate := updateBind != nil
	noPrefix := prefix == ""
	var hasOld = orm.inDB && !orm.delete
	serializer.Reset(orm.binary)
	for _, i := range fields.refs {
		f := value.Field(i)
		val := uint64(0)
		if !f.IsNil() {
			val = f.Elem().Field(1).Uint()
		}
		if hasOld && serializer.GetUInteger() == val {
			continue
		}
		name := prefix + fields.fields[i].Name
		if val == 0 {
			bind[name] = nil
			if hasUpdate {
				updateBind[name] = "NULL"
			}
		} else {
			bind[name] = val
			if hasUpdate {
				updateBind[name] = strconv.FormatUint(val, 10)
			}
		}
	}
	for _, i := range fields.uintegers {
		if i == 1 && noPrefix {
			continue
		}
		val := value.Field(i).Uint()
		if hasOld && serializer.GetUInteger() == val {
			continue
		}
		name := prefix + fields.fields[i].Name
		bind[name] = val
		if hasUpdate {
			updateBind[name] = strconv.FormatUint(val, 10)
		}
	}
	for _, i := range fields.integers {
		val := value.Field(i).Int()
		if hasOld && serializer.GetInteger() == val {
			continue
		}
		name := prefix + fields.fields[i].Name
		bind[name] = val
		if hasUpdate {
			updateBind[name] = strconv.FormatInt(val, 10)
		}
	}
	for _, i := range fields.booleans {
		val := value.Field(i).Bool()
		if hasOld && serializer.GetBool() == val {
			continue
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
	for i, precision := range fields.floats {
		val := value.Field(i).Float()
		if hasOld && math.Abs(val-serializer.GetFloat()) < precision {
			continue
		}
		name := prefix + fields.fields[i].Name
		bind[name] = val
		if hasUpdate {
			updateBind[name] = strconv.FormatFloat(val, 'f', -1, 64)
		}
	}
	for _, i := range fields.times {
		t := value.Field(i).Interface().(time.Time)
		if hasOld && serializer.GetInteger() == t.Unix() {
			continue
		}
		name := prefix + fields.fields[i].Name
		asString := t.Format(timeFormat)
		bind[name] = asString
		if hasUpdate {
			updateBind[name] = "'" + asString + "'"
		}
	}
	for _, i := range fields.dates {
		t := value.Field(i).Interface().(time.Time)
		t = time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, t.Location())
		if hasOld && serializer.GetInteger() == t.Unix() {
			continue
		}
		name := prefix + fields.fields[i].Name
		asString := t.Format(dateformat)
		bind[name] = asString
		if hasUpdate {
			updateBind[name] = "'" + asString + "'"
		}
	}
	if fields.fakeDelete > 0 {
		val := value.Field(fields.fakeDelete).Bool()
		if !hasOld || serializer.GetBool() != val {
			name := prefix + fields.fields[fields.fakeDelete].Name
			bind[name] = id
			if hasUpdate {
				updateBind[name] = strconv.FormatUint(id, 10)
			}
		}
	}
	for _, i := range fields.strings {
		val := value.Field(i).String()
		if hasOld && serializer.GetString() == val {
			continue
		}
		name := prefix + fields.fields[i].Name
		if val != "" {
			bind[name] = val
			if hasUpdate {
				updateBind[name] = orm.escapeSQLParam(val)
			}
		} else {
			attributes := tableSchema.tags[name]
			required, hasRequired := attributes["required"]
			if hasRequired && required == "true" {
				bind[name] = ""
				if hasUpdate {
					updateBind[name] = "''"
				}
			} else {
				bind[name] = nil
				if hasUpdate {
					updateBind[name] = "NULL"
				}
			}
		}
	}
	for _, i := range fields.uintegersNullable {
		f := value.Field(i)
		isNil := f.IsNil()
		val := uint64(0)
		if !isNil {
			val = f.Elem().Uint()
		}
		if hasOld {
			if serializer.GetBool() {
				if !isNil && serializer.GetUInteger() == val {
					continue
				}
			} else if isNil {
				continue
			}
		}
		name := prefix + fields.fields[i].Name
		if isNil {
			bind[name] = nil
			if hasUpdate {
				updateBind[name] = "NULL"
			}
		} else {
			bind[name] = val
			if hasUpdate {
				updateBind[name] = strconv.FormatUint(val, 10)
			}
		}
	}
	for _, i := range fields.integersNullable {
		f := value.Field(i)
		isNil := f.IsNil()
		val := int64(0)
		if !isNil {
			val = f.Elem().Int()
		}
		if hasOld {
			if serializer.GetBool() {
				if !isNil && serializer.GetInteger() == val {
					continue
				}
			} else if isNil {
				continue
			}
		}
		name := prefix + fields.fields[i].Name
		if isNil {
			bind[name] = nil
			if hasUpdate {
				updateBind[name] = "NULL"
			}
		} else {
			bind[name] = val
			if hasUpdate {
				updateBind[name] = strconv.FormatInt(val, 10)
			}
		}
	}
	k := 0
	for _, i := range fields.stringsEnums {
		val := value.Field(i).String()
		enum := fields.enums[k]
		k++
		if hasOld && serializer.GetUInteger() == uint64(enum.Index(val)) {
			continue
		}
		name := prefix + fields.fields[i].Name
		if val != "" {
			if !enum.Has(val) {
				panic(errors.New("unknown enum value for " + name + " - " + val))
			}
			bind[name] = val
			if hasUpdate {
				updateBind[name] = "'" + val + "'"
			}
		} else {
			attributes := tableSchema.tags[name]
			required, hasRequired := attributes["required"]
			if hasRequired && required == "true" {
				bind[name] = enum.GetDefault()
				if hasUpdate {
					updateBind[name] = "'" + enum.GetDefault() + "'"
				}
			} else {
				bind[name] = nil
				if hasUpdate {
					updateBind[name] = "NULL"
				}
			}
		}
	}
	for _, i := range fields.bytes {
		val := string(value.Field(i).Bytes())
		if hasOld && serializer.GetString() == val {
			continue
		}
		name := prefix + fields.fields[i].Name
		if val != "" {
			bind[name] = val
			if hasUpdate {
				updateBind[name] = orm.escapeSQLParam(val)
			}
		} else {
			bind[name] = nil
			if hasUpdate {
				updateBind[name] = "NULL"
			}
		}
	}
	k = 0
	for _, i := range fields.sliceStringsSets {
		val := value.Field(i).Interface().([]string)
		set := fields.sets[k]
		l := len(val)
		k++
		if hasOld && l == int(serializer.GetUInteger()) {
			old := make([]int, l)
			for j := range val {
				old[j] = int(serializer.GetUInteger())
			}
			valid := true
		MAIN:
			for _, v := range val {
				index := set.Index(v)
				if index == 0 {
					panic(errors.New("unknown set value for " + prefix + fields.fields[i].Name + " - " + v))
				}
				for _, o := range old {
					if o == index {
						continue MAIN
					}
				}
				valid = false
				break
			}
			if valid {
				continue
			}
		}
		name := prefix + fields.fields[i].Name
		if l > 0 {
			valAsString := strings.Join(val, ",")
			bind[name] = valAsString
			if hasUpdate {
				updateBind[name] = "'" + valAsString + "'"
			}
		} else {
			attributes := tableSchema.tags[name]
			required, hasRequired := attributes["required"]
			if hasRequired && required == "true" {
				bind[name] = set.GetDefault()
				if hasUpdate {
					updateBind[name] = "'" + set.GetDefault() + "'"
				}
			} else {
				bind[name] = nil
				if hasUpdate {
					updateBind[name] = "NULL"
				}
			}
		}
	}
	for _, i := range fields.booleansNullable {
		f := value.Field(i)
		isNil := f.IsNil()
		val := false
		if !isNil {
			val = f.Elem().Bool()
		}
		if hasOld {
			if serializer.GetBool() {
				if !isNil && serializer.GetBool() == val {
					continue
				}
			} else if isNil {
				continue
			}
		}
		name := prefix + fields.fields[i].Name
		if isNil {
			bind[name] = nil
			if hasUpdate {
				updateBind[name] = "NULL"
			}
		} else {
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
	for i, precision := range fields.floatsNullable {
		f := value.Field(i)
		isNil := f.IsNil()
		val := float64(0)
		if !isNil {
			val = f.Elem().Float()
		}
		if hasOld {
			if serializer.GetBool() {
				if !isNil && math.Abs(val-serializer.GetFloat()) < precision {
					continue
				}
			} else if isNil {
				continue
			}
		}
		name := prefix + fields.fields[i].Name
		if isNil {
			bind[name] = nil
			if hasUpdate {
				updateBind[name] = "NULL"
			}
		} else {
			bind[name] = val
			if hasUpdate {
				updateBind[name] = strconv.FormatFloat(val, 'f', -1, 64)
			}
		}
	}
	for _, i := range fields.timesNullable {
		f := value.Field(i)
		isNil := f.IsNil()
		var val *time.Time
		if !isNil {
			val = f.Interface().(*time.Time)
		}
		if hasOld {
			if serializer.GetBool() {
				if !isNil && serializer.GetInteger() == val.Unix() {
					continue
				}
			} else if isNil {
				continue
			}
		}
		name := prefix + fields.fields[i].Name
		if isNil {
			bind[name] = nil
			if hasUpdate {
				updateBind[name] = "NULL"
			}
		} else {
			asString := val.Format(timeFormat)
			bind[name] = asString
			if hasUpdate {
				updateBind[name] = "'" + asString + "'"
			}
		}
	}
	for _, i := range fields.datesNullable {
		f := value.Field(i)
		isNil := f.IsNil()
		var val time.Time
		if !isNil {
			val = *f.Interface().(*time.Time)
			val = time.Date(val.Year(), val.Month(), val.Day(), 0, 0, 0, 0, val.Location())
		}
		if hasOld {
			if serializer.GetBool() {
				if !isNil && serializer.GetInteger() == val.Unix() {
					continue
				}
			} else if isNil {
				continue
			}
		}
		name := prefix + fields.fields[i].Name
		if isNil {
			bind[name] = nil
			if hasUpdate {
				updateBind[name] = "NULL"
			}
		} else {
			asString := val.Format(dateformat)
			bind[name] = asString
			if hasUpdate {
				updateBind[name] = "'" + asString + "'"
			}
		}
	}
	for _, i := range fields.jsons {
		f := value.Field(i)
		isNil := f.IsNil()
		var val string
		if !isNil {
			v := f.Interface()
			encoded, err := jsoniter.ConfigFastest.Marshal(v)
			checkError(err)
			val = string(encoded)
		}
		if hasOld && serializer.GetString() == val {
			continue
		}
		name := prefix + fields.fields[i].Name
		if len(val) > 0 {
			bind[name] = val
			if hasUpdate {
				updateBind[name] = orm.escapeSQLParam(val)
			}
		} else {
			attributes := tableSchema.tags[name]
			required, hasRequired := attributes["required"]
			if hasRequired && required == "true" {
				bind[name] = ""
				if hasUpdate {
					updateBind[name] = "''"
				}
			} else {
				bind[name] = nil
				if hasUpdate {
					updateBind[name] = "NULL"
				}
			}
		}
	}
	for range fields.refsMany {
		// TODO
	}
	for range fields.structs {
		// TODO
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
