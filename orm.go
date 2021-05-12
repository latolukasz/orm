package orm

import (
	"fmt"
	"math"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/google/go-cmp/cmp"
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
	IsDirty() bool
	GetDirtyBind() (bind Bind, has bool)
	SetOnDuplicateKeyUpdate(bind Bind)
	SetEntityLogMeta(key string, value interface{})
	SetField(field string, value interface{}) error
	GetFieldLazy(field string) interface{}
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

func (orm *ORM) GetFieldLazy(field string) interface{} {
	if !orm.lazy {
		panic(fmt.Errorf("entity is not lazy"))
	}
	i, has := orm.tableSchema.columnMapping[field]
	if !has {
		panic(fmt.Errorf("unknown field %s", field))
	}
	return orm.dBData[i]
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
		fillStruct(engine.registry, 0, orm.dBData, orm.tableSchema.fields, orm, orm.elem)
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

func (orm *ORM) IsDirty() bool {
	if !orm.loaded {
		return true
	}
	_, is := orm.GetDirtyBind()
	return is
}

func (orm *ORM) GetDirtyBind() (bind Bind, has bool) {
	bind, _, has = orm.getDirtyBind()
	return bind, has
}

func (orm *ORM) getDirtyBind() (bind Bind, updateBind map[string]string, has bool) {
	if orm.delete {
		return nil, nil, true
	}
	if orm.fakeDelete {
		if orm.tableSchema.hasFakeDelete {
			orm.elem.FieldByName("FakeDelete").SetBool(true)
		} else {
			orm.delete = true
			return nil, nil, true
		}
	}
	id := orm.GetID()
	bind = make(Bind)
	if orm.inDB && !orm.delete {
		updateBind = make(map[string]string)
	}
	orm.fillBind(id, bind, updateBind, orm.tableSchema, orm.tableSchema.fields, orm.elem, orm.dBData, "")
	has = id == 0 || len(bind) > 0
	return bind, updateBind, has
}

func (orm *ORM) serialize(serializer *serializer, fields *tableFields) {
	orm.serializeFields(serializer, fields, orm.elem)
	copy(orm.binary, serializer.output.Bytes())
	serializer.output.Reset()
}

func (orm *ORM) serializeFields(serializer *serializer, fields *tableFields, elem reflect.Value) {
	for _, i := range fields.uintegers8 {
		serializer.UInt8(uint8(elem.Field(i).Uint()))
	}
	for _, i := range fields.uintegers16 {
		serializer.UInt16(uint16(elem.Field(i).Uint()))
	}
	for _, i := range fields.uintegers32 {
		serializer.UInt32(uint32(elem.Field(i).Uint()))
	}
	for _, i := range fields.uintegers64 {
		serializer.UInt64(elem.Field(i).Uint())
	}
	for _, i := range fields.integers8 {
		serializer.Int8(int8(elem.Field(i).Int()))
	}
	for _, i := range fields.integers16 {
		serializer.Int16(int16(elem.Field(i).Int()))
	}
	for _, i := range fields.integers32 {
		serializer.Int32(int32(elem.Field(i).Int()))
	}
	for _, i := range fields.integers64 {
		serializer.Int64(elem.Field(i).Int())
	}
	for _, i := range fields.booleans {
		serializer.Bool(elem.Field(i).Bool())
	}
	for _, i := range fields.floats32 {
		serializer.Float32(float32(elem.Field(i).Float()))
	}
	for _, i := range fields.floats64 {
		serializer.Float64(elem.Field(i).Float())
	}
	for _, i := range fields.floats64 {
		f := elem.Field(i)
		if f.IsNil() {
			serializer.Nullable(true)
		} else {
			serializer.Nullable(false)
			serializer.Float64(f.Float())
		}
	}
	for _, i := range fields.times {
		serializer.UInt32(uint32(elem.Field(i).Interface().(time.Time).Unix()))
	}
	if fields.fakeDelete > 0 {
		serializer.Bool(elem.Field(fields.fakeDelete).Uint() > 0)
	}
	for _, i := range fields.refs8 {
		e := elem.Field(i).Interface().(Entity)
		id := uint8(0)
		if e != nil {
			id = uint8(e.GetID())
		}
		serializer.UInt8(id)
	}
	for _, i := range fields.refs16 {
		e := elem.Field(i).Interface().(Entity)
		id := uint16(0)
		if e != nil {
			id = uint16(e.GetID())
		}
		serializer.UInt16(id)
	}
	for _, i := range fields.refs32 {
		e := elem.Field(i).Interface().(Entity)
		id := uint32(0)
		if e != nil {
			id = uint32(e.GetID())
		}
		serializer.UInt32(id)
	}
	for _, i := range fields.refs64 {
		e := elem.Field(i).Interface().(Entity)
		id := uint64(0)
		if e != nil {
			id = e.GetID()
		}
		serializer.UInt64(id)
	}
	for _, i := range fields.uintegers8Nullable {
		f := elem.Field(i)
		if f.IsNil() {
			serializer.Nullable(true)
		} else {
			serializer.Nullable(false)
			serializer.UInt8(uint8(f.Uint()))
		}
	}
	for _, i := range fields.uintegers16Nullable {
		f := elem.Field(i)
		if f.IsNil() {
			serializer.Nullable(true)
		} else {
			serializer.Nullable(false)
			serializer.UInt16(uint16(f.Uint()))
		}
	}
	for _, i := range fields.uintegers32Nullable {
		f := elem.Field(i)
		if f.IsNil() {
			serializer.Nullable(true)
		} else {
			serializer.Nullable(false)
			serializer.UInt32(uint32(f.Uint()))
		}
	}
	for _, i := range fields.uintegers64Nullable {
		f := elem.Field(i)
		if f.IsNil() {
			serializer.Nullable(true)
		} else {
			serializer.Nullable(false)
			serializer.UInt64(f.Uint())
		}
	}
	for _, i := range fields.integers8Nullable {
		f := elem.Field(i)
		if f.IsNil() {
			serializer.Nullable(true)
		} else {
			serializer.Nullable(false)
			serializer.Int8(int8(f.Int()))
		}
	}
	for _, i := range fields.integers16Nullable {
		f := elem.Field(i)
		if f.IsNil() {
			serializer.Nullable(true)
		} else {
			serializer.Nullable(false)
			serializer.Int16(int16(f.Int()))
		}
	}
	for _, i := range fields.integers32Nullable {
		f := elem.Field(i)
		if f.IsNil() {
			serializer.Nullable(true)
		} else {
			serializer.Nullable(false)
			serializer.Int32(int32(f.Int()))
		}
	}
	for _, i := range fields.integers64Nullable {
		f := elem.Field(i)
		if f.IsNil() {
			serializer.Nullable(true)
		} else {
			serializer.Nullable(false)
			serializer.Int64(f.Int())
		}
	}
	for k, i := range fields.stringsEnums {
		val := elem.Field(i).String()
		if val == "" {
			serializer.UInt8(0)
		} else {
			serializer.UInt8(uint8(fields.enums[k].Index(val)))
		}
	}
	for _, i := range fields.strings {
		serializer.String(elem.Field(i).String())
	}
	for _, i := range fields.bytes {
		serializer.Bytes(elem.Field(i).Bytes())
	}
	for k, i := range fields.sliceStringsSets {
		f := elem.Field(i)
		values := f.Interface().([]string)
		l := len(values)
		serializer.Uvarint(uint64(l))
		if l > 0 {
			set := fields.sets[k]
			for _, val := range values {
				serializer.UInt8(uint8(set.Index(val)))
			}
		}
	}
	for _, i := range fields.booleansNullable {
		f := elem.Field(i)
		if f.IsNil() {
			serializer.Nullable(true)
		} else {
			serializer.Nullable(false)
			serializer.Bool(f.Bool())
		}
	}
	for _, i := range fields.floats32Nullable {
		f := elem.Field(i)
		if f.IsNil() {
			serializer.Nullable(true)
		} else {
			serializer.Nullable(false)
			serializer.Float32(float32(f.Float()))
		}
	}
	for _, i := range fields.timesNullable {
		f := elem.Field(i)
		if f.IsNil() {
			serializer.Nullable(true)
		} else {
			serializer.Nullable(false)
			serializer.UInt32(uint32(elem.Field(i).Interface().(time.Time).Unix()))
		}
	}
	for i, subField := range fields.structs {
		orm.serializeFields(serializer, subField, elem.Field(i).Elem())
	}
	// dynamic

	for _, i := range fields.jsons {
		f := elem.Field(i)
		if f.IsNil() {
			serializer.Nullable(true)
		} else {
			serializer.Nullable(false)
			encoded, _ := jsoniter.ConfigFastest.Marshal(f.Interface())
			serializer.Bytes(encoded)
		}
	}
	for _, i := range fields.refsMany8 {
		e := elem.Field(i)
		if e.IsNil() {
			serializer.Uvarint(uint64(0))
		} else {
			l := e.Len()
			for k := 0; k < l; k++ {
				serializer.UInt8(uint8(e.Index(k).Interface().(Entity).GetID()))
			}
		}
	}
	for _, i := range fields.refsMany16 {
		e := elem.Field(i)
		if e.IsNil() {
			serializer.Uvarint(uint64(0))
		} else {
			l := e.Len()
			for k := 0; k < l; k++ {
				serializer.UInt16(uint16(e.Index(k).Interface().(Entity).GetID()))
			}
		}
	}
	for _, i := range fields.refsMany32 {
		e := elem.Field(i)
		if e.IsNil() {
			serializer.Uvarint(uint64(0))
		} else {
			l := e.Len()
			for k := 0; k < l; k++ {
				serializer.UInt32(uint32(e.Index(k).Interface().(Entity).GetID()))
			}
		}
	}
	for _, i := range fields.refsMany32 {
		e := elem.Field(i)
		if e.IsNil() {
			serializer.Uvarint(uint64(0))
		} else {
			l := e.Len()
			for k := 0; k < l; k++ {
				serializer.UInt32(uint32(e.Index(k).Interface().(Entity).GetID()))
			}
		}
	}
	for _, i := range fields.refsMany64 {
		e := elem.Field(i)
		if e.IsNil() {
			serializer.Uvarint(uint64(0))
		} else {
			l := e.Len()
			for k := 0; k < l; k++ {
				serializer.UInt64(e.Index(k).Interface().(Entity).GetID())
			}
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

func (orm *ORM) prepareFieldBind(prefix string, schema *tableSchema, fields *tableFields, value reflect.Value,
	oldData []interface{}, index int) (reflect.Value, string, interface{}) {
	name := prefix + fields.fields[index].Name
	field := value.Field(index)
	if orm.inDB {
		return field, name, oldData[schema.columnMapping[name]]
	}
	return field, name, nil
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

func (orm *ORM) fillBind(id uint64, bind Bind, updateBind map[string]string, tableSchema *tableSchema,
	fields *tableFields, value reflect.Value, oldData []interface{}, prefix string) {
	var hasOld = orm.inDB
	hasUpdate := updateBind != nil
	for _, i := range fields.uintegers {
		if i == 1 && prefix == "" {
			continue
		}
		field, name, old := orm.prepareFieldBind(prefix, tableSchema, fields, value, oldData, i)
		val := field.Uint()
		if hasOld && old == val {
			continue
		}
		bind[name] = val
		if hasUpdate {
			updateBind[name] = strconv.FormatUint(val, 10)
		}
	}
	for _, i := range fields.uintegersNullable {
		field, name, old := orm.prepareFieldBind(prefix, tableSchema, fields, value, oldData, i)
		if !orm.checkNil(field, name, hasOld, old, bind, updateBind) {
			continue
		}
		val := field.Elem().Uint()
		if hasOld && old == val {
			continue
		}
		bind[name] = val
		if hasUpdate {
			updateBind[name] = strconv.FormatUint(val, 10)
		}
	}
	for _, i := range fields.integers {
		field, name, old := orm.prepareFieldBind(prefix, tableSchema, fields, value, oldData, i)
		val := field.Int()
		if hasOld && old == val {
			continue
		}
		bind[name] = val
		if hasUpdate {
			updateBind[name] = strconv.FormatInt(val, 10)
		}
	}
	for _, i := range fields.integersNullable {
		field, name, old := orm.prepareFieldBind(prefix, tableSchema, fields, value, oldData, i)
		if !orm.checkNil(field, name, hasOld, old, bind, updateBind) {
			continue
		}
		val := field.Elem().Int()
		if hasOld && old == val {
			continue
		}
		bind[name] = val
		if hasUpdate {
			updateBind[name] = strconv.FormatInt(val, 10)
		}
	}
	for _, i := range fields.strings {
		field, name, old := orm.prepareFieldBind(prefix, tableSchema, fields, value, oldData, i)
		value := field.String()
		if hasOld && (old == value || (old == nil && value == "")) {
			continue
		}
		if value != "" {
			bind[name] = value
			if hasUpdate {
				updateBind[name] = orm.escapeSQLParam(value)
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
	for _, i := range fields.bytes {
		field, name, old := orm.prepareFieldBind(prefix, tableSchema, fields, value, oldData, i)
		value := field.Bytes()
		valueAsString := string(value)
		if hasOld && ((old == nil && valueAsString == "") || (old != nil && old.(string) == valueAsString)) {
			continue
		}
		if valueAsString == "" {
			bind[name] = nil
			if hasUpdate {
				updateBind[name] = "NULL"
			}
		} else {
			bind[name] = valueAsString
			if hasUpdate {
				updateBind[name] = orm.escapeSQLParam(valueAsString)
			}
		}
	}
	if fields.fakeDelete > 0 {
		field, name, old := orm.prepareFieldBind(prefix, tableSchema, fields, value, oldData, fields.fakeDelete)
		value := uint64(0)
		if field.Bool() {
			value = id
		}
		if !hasOld || old != value {
			bind[name] = value
			if hasUpdate {
				updateBind[name] = strconv.FormatUint(value, 10)
			}
		}
	}
	for _, i := range fields.booleans {
		field, name, old := orm.prepareFieldBind(prefix, tableSchema, fields, value, oldData, i)
		value := field.Bool()
		if hasOld && old == value {
			continue
		}
		bind[name] = value
		if hasUpdate {
			if value {
				updateBind[name] = "1"
			} else {
				updateBind[name] = "0"
			}
		}
	}
	for _, i := range fields.booleansNullable {
		field, name, old := orm.prepareFieldBind(prefix, tableSchema, fields, value, oldData, i)
		if !orm.checkNil(field, name, hasOld, old, bind, updateBind) {
			continue
		}
		value := field.Elem().Bool()
		if hasOld && old == value {
			continue
		}
		bind[name] = value
		if hasUpdate {
			if value {
				updateBind[name] = "1"
			} else {
				updateBind[name] = "0"
			}
		}
	}
	for _, i := range fields.floats {
		field, name, old := orm.prepareFieldBind(prefix, tableSchema, fields, value, oldData, i)
		val := field.Float()
		precision := 16
		fieldAttributes := tableSchema.tags[name]
		precisionAttribute, has := fieldAttributes["precision"]
		if has {
			userPrecision, _ := strconv.Atoi(precisionAttribute)
			precision = userPrecision
		}
		attributes := tableSchema.tags[name]
		decimal, has := attributes["decimal"]
		if has {
			decimalArgs := strings.Split(decimal, ",")
			size, _ := strconv.ParseFloat(decimalArgs[1], 64)
			sizeNumber := math.Pow(10, size)
			val = math.Round(val*sizeNumber) / sizeNumber
			if hasOld {
				valOld := math.Round(old.(float64)*sizeNumber) / sizeNumber
				if val == valOld {
					continue
				}
			}
		} else {
			sizeNumber := math.Pow(10, float64(precision))
			val = math.Round(val*sizeNumber) / sizeNumber
			if hasOld {
				valOld := math.Round(old.(float64)*sizeNumber) / sizeNumber
				if valOld == val {
					continue
				}
			}
		}
		bind[name] = val
		if hasUpdate {
			updateBind[name] = strconv.FormatFloat(val, 'f', -1, 64)
		}
	}
	for _, i := range fields.floatsNullable {
		field, name, old := orm.prepareFieldBind(prefix, tableSchema, fields, value, oldData, i)
		if !orm.checkNil(field, name, hasOld, old, bind, updateBind) {
			continue
		}
		var val float64
		isZero := field.IsZero()
		if !isZero {
			val = field.Elem().Float()
		}
		precision := 10
		fieldAttributes := tableSchema.tags[name]
		precisionAttribute, has := fieldAttributes["precision"]
		if has {
			userPrecision, _ := strconv.Atoi(precisionAttribute)
			precision = userPrecision
		}
		attributes := tableSchema.tags[name]
		decimal, has := attributes["decimal"]
		if has {
			decimalArgs := strings.Split(decimal, ",")
			size, _ := strconv.ParseFloat(decimalArgs[1], 64)
			sizeNumber := math.Pow(10, size)
			val = math.Round(val*sizeNumber) / sizeNumber
			if hasOld && old != nil {
				valOld := math.Round(old.(float64)*sizeNumber) / sizeNumber
				if val == valOld {
					continue
				}
			}
			bind[name] = val
			if hasUpdate {
				updateBind[name] = strconv.FormatFloat(val, 'f', -1, 64)
			}
		} else {
			sizeNumber := math.Pow(10, float64(precision))
			val = math.Round(val*sizeNumber) / sizeNumber
			if hasOld && old != nil {
				valOld := math.Round(old.(float64)*sizeNumber) / sizeNumber
				if valOld == val {
					continue
				}
			}
			bind[name] = val
			if hasUpdate {
				updateBind[name] = strconv.FormatFloat(val, 'f', -1, 64)
			}
		}
	}
	for _, i := range fields.times {
		field, name, old := orm.prepareFieldBind(prefix, tableSchema, fields, value, oldData, i)
		value := field.Interface().(time.Time)
		layout := "2006-01-02"
		var valueAsString string
		if tableSchema.tags[name]["time"] == "true" {
			if value.Year() == 1 {
				valueAsString = "0001-01-01 00:00:00"
			} else {
				layout += " 15:04:05"
			}
		} else if value.Year() == 1 {
			valueAsString = "0001-01-01"
		}
		if valueAsString == "" {
			valueAsString = value.Format(layout)
		}
		if hasOld && old == valueAsString {
			continue
		}
		bind[name] = valueAsString
		if hasUpdate {
			updateBind[name] = "'" + valueAsString + "'"
		}
	}
	for _, i := range fields.timesNullable {
		field, name, old := orm.prepareFieldBind(prefix, tableSchema, fields, value, oldData, i)
		if !orm.checkNil(field, name, hasOld, old, bind, updateBind) {
			continue
		}
		value := field.Interface().(*time.Time)
		layout := "2006-01-02"
		var valueAsString string
		if tableSchema.tags[name]["time"] == "true" {
			if value != nil {
				layout += " 15:04:05"
			}
		}
		if value != nil {
			valueAsString = value.Format(layout)
		}
		if hasOld && (old == valueAsString || (valueAsString == "" && (old == nil || old == "nil"))) {
			continue
		}
		if valueAsString == "" {
			bind[name] = nil
			if hasUpdate {
				updateBind[name] = "NULL"
			}
		} else {
			bind[name] = valueAsString
			if hasUpdate {
				updateBind[name] = "'" + valueAsString + "'"
			}
		}
	}
	for _, i := range fields.sliceStringsSets {
		field, name, old := orm.prepareFieldBind(prefix, tableSchema, fields, value, oldData, i)
		value := field.Interface().([]string)
		var valueAsString string
		if value != nil {
			valueAsString = strings.Join(value, ",")
		}
		if hasOld && (old == valueAsString || (valueAsString == "" && old == nil)) {
			continue
		}
		if valueAsString != "" {
			bind[name] = valueAsString
			if hasUpdate {
				updateBind[name] = orm.escapeSQLParam(valueAsString)
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
	for i, subFields := range fields.structs {
		field, _, _ := orm.prepareFieldBind(prefix, tableSchema, fields, value, oldData, i)
		orm.fillBind(0, bind, updateBind, tableSchema, subFields, reflect.ValueOf(field.Interface()), oldData, fields.fields[i].Name)
	}
	for _, i := range fields.refs {
		field, name, old := orm.prepareFieldBind(prefix, tableSchema, fields, value, oldData, i)
		value := uint64(0)
		if !field.IsNil() {
			value = field.Elem().Field(1).Uint()
		}
		if hasOld && (old == value || ((old == nil || old == 0) && value == 0)) {
			continue
		}
		if value == 0 {
			bind[name] = nil
			if hasUpdate {
				updateBind[name] = "NULL"
			}
		} else {
			bind[name] = value
			if hasUpdate {
				updateBind[name] = strconv.FormatUint(value, 10)
			}
		}
	}
	for _, i := range fields.refsMany {
		field, name, old := orm.prepareFieldBind(prefix, tableSchema, fields, value, oldData, i)
		if !orm.checkNil(field, name, hasOld, old, bind, updateBind) {
			continue
		}
		var valString string
		length := field.Len()
		if length > 0 {
			ids := make([]uint64, length)
			for i := 0; i < length; i++ {
				ids[i] = field.Index(i).Interface().(Entity).GetID()
			}
			encoded, _ := jsoniter.ConfigFastest.Marshal(ids)
			valString = string(encoded)
		}
		if hasOld && (old == valString || ((old == nil || old == "0") && valString == "")) {
			continue
		}
		if valString == "" {
			bind[name] = nil
			if hasUpdate {
				updateBind[name] = "NULL"
			}
		} else {
			bind[name] = valString
			if hasUpdate {
				updateBind[name] = "'" + valString + "'"
			}
		}
	}
	for _, i := range fields.jsons {
		field, name, old := orm.prepareFieldBind(prefix, tableSchema, fields, value, oldData, i)
		value := field.Interface()
		var valString string
		if !field.IsZero() {
			var encoded []byte
			if hasOld && old != nil && old != "" {
				oldMap := reflect.New(field.Type()).Interface()
				newMap := reflect.New(field.Type()).Interface()
				_ = jsoniter.ConfigFastest.UnmarshalFromString(old.(string), oldMap)
				oldValue := reflect.ValueOf(oldMap).Elem().Interface()
				encoded, _ = jsoniter.ConfigFastest.Marshal(value)
				_ = jsoniter.ConfigFastest.Unmarshal(encoded, newMap)
				newValue := reflect.ValueOf(newMap).Elem().Interface()
				if cmp.Equal(newValue, oldValue) {
					continue
				}
			} else {
				encoded, _ = jsoniter.ConfigFastest.Marshal(value)
			}
			valString = string(encoded)
		} else if hasOld && old == nil {
			continue
		}
		if valString != "" {
			bind[name] = valString
			if hasUpdate {
				updateBind[name] = "'" + valString + "'"
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
}

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
