package orm

import (
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"time"
)

func createUint64AttrToStringSetter(setter fieldBindSetter) func(any, bool) (string, error) {
	return func(v any, fromBind bool) (string, error) {
		if fromBind {
			return strconv.FormatUint(v.(uint64), 10), nil
		}
		v2, err := setter(v)
		if err != nil {
			return "", err
		}
		return strconv.FormatUint(v2.(uint64), 10), nil
	}
}

func createInt64AttrToStringSetter(setter fieldBindSetter) func(any, bool) (string, error) {
	return func(v any, fromBind bool) (string, error) {
		if fromBind {
			return strconv.FormatInt(v.(int64), 10), nil
		}
		v2, err := setter(v)
		if err != nil {
			return "", err
		}
		return strconv.FormatInt(v2.(int64), 10), nil
	}
}

func createNumberFieldBindSetter(columnName string, unsigned, nullable bool, min int64, max uint64) func(v any) (any, error) {
	return func(v any) (any, error) {
		if v == nil {
			if !nullable {
				return nil, &BindError{columnName, "nil is not allowed"}
			}
			return nil, nil
		}
		var asUint64 uint64
		var asInt64 int64
		isNil := false
		switch v.(type) {
		case string:
			var err error
			if unsigned {
				asUint64, err = strconv.ParseUint(v.(string), 10, 64)
				if err != nil {
					return nil, &BindError{columnName, fmt.Sprintf("invalid number %s", v.(string))}
				}
			} else {
				asInt64, err = strconv.ParseInt(v.(string), 10, 64)
				if err != nil {
					return nil, &BindError{columnName, fmt.Sprintf("invalid number %s", v.(string))}
				}
			}
		case uint8:
			asUint64 = uint64(v.(uint8))
		case *uint8:
			i := v.(*uint8)
			if i == nil {
				isNil = true
			} else {
				asUint64 = uint64(*i)
			}
		case uint16:
			asUint64 = uint64(v.(uint16))
		case *uint16:
			i := v.(*uint16)
			if i == nil {
				isNil = true
			} else {
				asUint64 = uint64(*i)
			}
		case uint:
			asUint64 = uint64(v.(uint))
		case *uint:
			i := v.(*uint)
			if i == nil {
				isNil = true
			} else {
				asUint64 = uint64(*i)
			}
		case uint32:
			asUint64 = uint64(v.(uint32))
		case *uint32:
			i := v.(*uint32)
			if i == nil {
				isNil = true
			} else {
				asUint64 = uint64(*i)
			}
		case uint64:
			asUint64 = v.(uint64)
		case *uint64:
			i := v.(*uint64)
			if i == nil {
				isNil = true
			} else {
				asUint64 = *i
			}
		case int8:
			asInt64 = int64(v.(int8))
		case *int8:
			i := v.(*int8)
			if i == nil {
				isNil = true
			} else {
				asInt64 = int64(*i)
			}
		case int16:
			asInt64 = int64(v.(int16))
		case *int16:
			i := v.(*int16)
			if i == nil {
				isNil = true
			} else {
				asInt64 = int64(*i)
			}
		case int:
			asInt64 = int64(v.(int))
		case *int:
			i := v.(*int)
			if i == nil {
				isNil = true
			} else {
				asInt64 = int64(*i)
			}
		case int32:
			asInt64 = int64(v.(int32))
		case *int32:
			i := v.(*int32)
			if i == nil {
				isNil = true
			} else {
				asInt64 = int64(*i)
			}
		case int64:
			asInt64 = v.(int64)
		case *int64:
			i := v.(*int64)
			if i == nil {
				isNil = true
			} else {
				asInt64 = *i
			}
		case float32:
			asInt64 = int64(v.(float32))
		case *float32:
			i := v.(*float32)
			if i == nil {
				isNil = true
			} else {
				asInt64 = int64(*i)
			}
		case float64:
			asInt64 = int64(v.(float64))
		case *float64:
			i := v.(*float64)
			if i == nil {
				isNil = true
			} else {
				asInt64 = int64(*i)
			}
		default:
			return nil, &BindError{columnName, "invalid value"}
		}
		if isNil {
			if !nullable {
				return nil, &BindError{columnName, "nil is not allowed"}
			}
			return nil, nil
		}
		if !unsigned {
			if asUint64 > 0 {
				asInt64 = int64(asUint64)
			}
			if asInt64 > 0 && uint64(asInt64) > max {
				return nil, &BindError{columnName, fmt.Sprintf("value %d exceeded max allowed value", v)}
			}
			if asInt64 < 0 && asInt64 < min {
				return nil, &BindError{columnName, fmt.Sprintf("value %d exceeded min allowed value", v)}
			}
			return asInt64, nil
		}
		if asInt64 < 0 {
			return nil, &BindError{columnName, fmt.Sprintf("negative number %d not allowed", v)}
		} else if asInt64 > 0 {
			asUint64 = uint64(asInt64)
		}
		if asUint64 > max {
			return nil, &BindError{columnName, fmt.Sprintf("value %d exceeded max allowed value", v)}
		}
		return asUint64, nil
	}
}

func createReferenceFieldBindSetter(columnName string, t reflect.Type, idSetter fieldBindSetter, nullable bool) func(v any) (any, error) {
	return func(v any) (any, error) {
		if v == nil {
			if !nullable {
				return nil, &BindError{columnName, "nil is not allowed"}
			}
			return nil, nil
		}
		elem := reflect.Indirect(reflect.ValueOf(v))
		if elem.Type() == t {
			id := elem.Field(0).Uint()
			if id == 0 {
				if !nullable {
					return nil, &BindError{columnName, "nil is not allowed"}
				}
				return nil, nil
			}
			return id, nil
		}
		reference, is := v.(ReferenceInterface)
		if is {
			if reference.getType() != t {
				return nil, &BindError{columnName, "invalid reference type"}
			}
			id := reference.GetID()
			if id == 0 {
				if !nullable {
					return nil, &BindError{columnName, "nil is not allowed"}
				}
				return nil, nil
			}
			return id, nil
		}
		id, err := idSetter(v)
		if err != nil {
			return nil, err
		}
		if id == uint64(0) {
			if !nullable {
				return nil, &BindError{columnName, "nil is not allowed"}
			}
			return nil, nil
		}
		return id, nil
	}
}

func createEnumFieldBindSetter(columnName string, stringSetter fieldBindSetter, def *enumDefinition) func(v any) (any, error) {
	return func(v any) (any, error) {
		if v == nil {
			if def.required {
				return nil, &BindError{columnName, "nil is not allowed"}
			}
			return nil, nil
		}
		v = fmt.Sprintf("%s", v)
		if v == "" {
			if def.required {
				return nil, &BindError{columnName, "nil is not allowed"}
			}
			return nil, nil
		}
		val, err := stringSetter(v)
		if err != nil {
			return nil, err
		}
		if !def.Has(val.(string)) {
			return nil, &BindError{columnName, fmt.Sprintf("invalid value: %s", v)}
		}
		return val, nil
	}
}

func createSetFieldBindSetter(columnName string, enumSetter fieldBindSetter, def *enumDefinition) func(v any) (any, error) {
	return func(v any) (any, error) {
		if v == nil || v == "" {
			if def.required {
				return nil, &BindError{columnName, "nil is not allowed"}
			}
			return nil, nil
		}
		toReturn := strings.Trim(strings.ReplaceAll(fmt.Sprintf("%v", v), " ", ","), "[]")
		if toReturn == "" {
			if def.required {
				return nil, &BindError{columnName, "nil is not allowed"}
			}
			return nil, nil
		}
		values := strings.Split(toReturn, ",")
		for _, option := range values {
			_, err := enumSetter(option)
			if err != nil {
				return nil, err
			}
		}
		return toReturn, nil
	}
}

func createStringAttrToStringSetter(setter fieldBindSetter) func(any, bool) (string, error) {
	return func(v any, fromBind bool) (string, error) {
		if fromBind {
			return v.(string), nil
		}
		v2, err := setter(v)
		if err != nil {
			return "", err
		}
		return v2.(string), nil
	}
}

func createStringFieldBindSetter(columnName string, length int, required bool) func(v any) (any, error) {
	return func(v any) (any, error) {
		if v == nil {
			if required {
				return nil, &BindError{Field: columnName, Message: "empty string not allowed"}
			}
			return nil, nil
		}
		switch v.(type) {
		case string:
			asString := v.(string)
			if v == "" {
				if required {
					return nil, &BindError{Field: columnName, Message: "empty string not allowed"}
				}
				return nil, nil
			}
			if length > 0 && len(asString) > length {
				return nil, &BindError{Field: columnName,
					Message: fmt.Sprintf("text too long, max %d allowed", length)}
			}
			return asString, nil
		default:
			return nil, &BindError{columnName, "invalid value"}
		}
	}
}

func createBytesFieldBindSetter(columnName string) func(v any) (any, error) {
	return func(v any) (any, error) {
		if v == nil {
			return nil, nil
		}
		switch v.(type) {
		case string:
			asString := v.(string)
			if v == "" {
				return nil, nil
			}
			return asString, nil
		case []byte:
			asString := string(v.([]byte))
			if v == "" {
				return nil, nil
			}
			return asString, nil
		default:
			return nil, &BindError{columnName, "invalid value"}
		}
	}
}

func createBoolFieldBindSetter(columnName string) func(v any) (any, error) {
	return func(v any) (any, error) {
		switch v.(type) {
		case bool:
			return v, nil
		case *bool:
			b := v.(*bool)
			if b == nil {
				return nil, nil
			}
			return *b, nil
		case int:
			return v.(int) == 1, nil
		case string:
			s := strings.ToLower(v.(string))
			return s == "true" || s == "1" || s == "yes", nil
		default:
			return nil, &BindError{columnName, "invalid value"}
		}
	}
}

func createDateFieldBindSetter(columnName string, layout string, nullable bool) func(v any) (any, error) {
	return func(v any) (any, error) {
		if v == nil {
			if !nullable {
				return nil, &BindError{columnName, "nil is not allowed"}
			}
			return nil, nil
		}
		switch v.(type) {
		case time.Time:
			t := v.(time.Time)
			if t.Location().String() != time.UTC.String() {
				return nil, &BindError{Field: columnName, Message: "time must be in UTC location"}
			}
			return t.Format(layout), nil
		case *time.Time:
			t := v.(*time.Time)
			if t.Location().String() != time.UTC.String() {
				return nil, &BindError{Field: columnName, Message: "time must be in UTC location"}
			}
			return t.Format(layout), nil
		case string:
			t, err := time.ParseInLocation(layout, v.(string), time.UTC)
			if err != nil {
				return nil, &BindError{columnName, fmt.Sprintf("invalid time %s", v)}
			}
			return t.Format(layout), nil
		default:
			return nil, &BindError{columnName, "invalid value"}
		}
	}
}

func createFloatFieldBindSetter(columnName string, unsigned, nullable bool, floatsPrecision, floatsSize, floatsDecimalSize int) func(v any) (any, error) {
	return func(v any) (any, error) {
		if v == nil {
			if !nullable {
				return nil, &BindError{columnName, "nil is not allowed"}
			}
			return nil, nil
		}
		var asFloat64 float64
		var err error
		switch v.(type) {
		case string:
			asFloat64, err = strconv.ParseFloat(v.(string), 10)
			if err != nil {
				return nil, &BindError{columnName, fmt.Sprintf("invalid number %s", v.(string))}
			}
		case uint8:
			asFloat64 = float64(v.(uint8))
		case uint16:
			asFloat64 = float64(v.(uint16))
		case uint:
			asFloat64 = float64(v.(uint))
		case uint32:
			asFloat64 = float64(v.(uint32))
		case uint64:
			asFloat64 = float64(v.(uint64))
		case int8:
			asFloat64 = float64(v.(int8))
		case int16:
			asFloat64 = float64(v.(int16))
		case int:
			asFloat64 = float64(v.(int))
		case int32:
			asFloat64 = float64(v.(int32))
		case int64:
			asFloat64 = float64(v.(int64))
		case float32:
			asFloat64 = float64(v.(float32))
		case *float32:
			f := v.(*float32)
			if f == nil {
				return nil, nil
			}
			asFloat64 = float64(*f)
		case float64:
			asFloat64 = v.(float64)
		case *float64:
			f := v.(*float64)
			if f == nil {
				return nil, nil
			}
			asFloat64 = *f
		default:
			return nil, &BindError{columnName, "invalid value"}
		}
		if unsigned && asFloat64 < 0 {
			return nil, &BindError{columnName, fmt.Sprintf("negative number %d not allowed", v)}
		}
		if asFloat64 == 0 {
			return "0", nil
		}
		roundV := roundFloat(asFloat64, floatsPrecision)
		val := strconv.FormatFloat(roundV, 'f', floatsPrecision, floatsSize)
		decimalSize := floatsDecimalSize
		if decimalSize != -1 && strings.Index(val, ".") > decimalSize {
			return nil, &BindError{Field: columnName, Message: fmt.Sprintf("decimal size too big, max %d allowed", decimalSize)}
		}
		return val, nil
	}
}

func createNullableFieldBindSetter(setter fieldBindSetter) func(v any) (any, error) {
	return func(v any) (any, error) {
		if v == nil {
			return nil, nil
		}
		return setter(v)
	}
}

func createStringFieldSetter(attributes schemaFieldAttributes, arrayIndex int) func(v any, elem reflect.Value) {
	return func(v any, elem reflect.Value) {
		field := getSetterField(elem, attributes, arrayIndex)
		if v == nil {
			field.SetString("")
		} else {
			field.SetString(v.(string))
		}
	}
}

func createBytesFieldSetter(attributes schemaFieldAttributes, arrayIndex int) func(v any, elem reflect.Value) {
	return func(v any, elem reflect.Value) {
		field := getSetterField(elem, attributes, arrayIndex)
		if v == nil {
			field.SetZero()
		} else {
			field.SetBytes([]byte(v.(string)))
		}
	}
}

func createBoolFieldSetter(attributes schemaFieldAttributes, arrayIndex int) func(v any, elem reflect.Value) {
	return func(v any, elem reflect.Value) {
		getSetterField(elem, attributes, arrayIndex).SetBool(v.(bool))
	}
}

func createTimeFieldSetter(attributes schemaFieldAttributes, layout string, arrayIndex int) func(v any, elem reflect.Value) {
	return func(v any, elem reflect.Value) {
		field := getSetterField(elem, attributes, arrayIndex)
		t, _ := time.ParseInLocation(layout, v.(string), time.UTC)
		field.Set(reflect.ValueOf(t))
	}
}

func createTimeNullableFieldSetter(attributes schemaFieldAttributes, layout string, arrayIndex int) func(v any, elem reflect.Value) {
	return func(v any, elem reflect.Value) {
		field := getSetterField(elem, attributes, arrayIndex)
		if v == nil {
			field.SetZero()
			return
		}
		t, _ := time.ParseInLocation(layout, v.(string), time.UTC)
		field.Set(reflect.ValueOf(&t))
	}
}

func getSetterField(elem reflect.Value, attributes schemaFieldAttributes, arrayIndex int) reflect.Value {
	field := elem
	for _, i := range attributes.Parents {
		if i < 0 {
			field = field.Index((i + 1) * -1)
		} else {
			field = field.Field(i)
		}
	}
	field = field.Field(attributes.Index)
	if attributes.IsArray {
		field = field.Index(arrayIndex)
	}
	return field
}

func createFloatFieldSetter(attributes schemaFieldAttributes, arrayIndex int) func(v any, elem reflect.Value) {
	return func(v any, elem reflect.Value) {
		field := getSetterField(elem, attributes, arrayIndex)
		f, _ := strconv.ParseFloat(v.(string), 64)
		field.SetFloat(f)
	}
}

func createFloatNullableFieldSetter(attributes schemaFieldAttributes, arrayIndex int) func(v any, elem reflect.Value) {
	return func(v any, elem reflect.Value) {
		field := getSetterField(elem, attributes, arrayIndex)
		if v == nil {
			field.SetZero()
			return
		}
		f, _ := strconv.ParseFloat(v.(string), 64)
		val := reflect.New(field.Type().Elem())
		val.Elem().SetFloat(f)
		field.Set(val)
	}
}

func createBoolNullableFieldSetter(attributes schemaFieldAttributes, arrayIndex int) func(v any, elem reflect.Value) {
	return func(v any, elem reflect.Value) {
		field := getSetterField(elem, attributes, arrayIndex)
		if v == nil {
			field.SetZero()
			return
		}
		val := reflect.New(field.Type().Elem())
		val.Elem().SetBool(v.(bool))
		field.Set(val)
	}
}

func createSetFieldSetter(attributes schemaFieldAttributes, arrayIndex int) func(v any, elem reflect.Value) {
	return func(v any, elem reflect.Value) {
		field := getSetterField(elem, attributes, arrayIndex)
		if v == nil {
			field.SetZero()
			return
		}
		values := strings.Split(v.(string), ",")
		val := reflect.MakeSlice(field.Type(), len(values), len(values))
		for i, value := range values {
			val.Index(i).SetString(value)
		}
		field.Set(val)
	}
}

func createFieldGetter(attributes schemaFieldAttributes, nullable bool, arrayIndex int) func(elem reflect.Value) any {
	return func(elem reflect.Value) any {
		field := getSetterField(elem, attributes, arrayIndex)
		if nullable && field.IsNil() {
			return nil
		}
		return field.Interface()
	}
}

func createNumberFieldSetter(attributes schemaFieldAttributes, unsigned, nullable bool, arrayIndex int) func(v any, elem reflect.Value) {
	return func(v any, elem reflect.Value) {
		field := getSetterField(elem, attributes, arrayIndex)
		if v == nil {
			field.SetZero()
			return
		}
		if nullable {
			val := reflect.New(field.Type().Elem())
			if unsigned {
				val.Elem().SetUint(v.(uint64))
			} else {
				val.Elem().SetInt(v.(int64))
			}
			field.Set(val)
			return
		}
		if unsigned {
			field.SetUint(v.(uint64))
			return
		}
		field.SetInt(v.(int64))
	}
}

func createReferenceFieldSetter(attributes schemaFieldAttributes, arrayIndex int) func(v any, elem reflect.Value) {
	return func(v any, elem reflect.Value) {
		field := getSetterField(elem, attributes, arrayIndex)
		if v == nil {
			field.SetUint(0)
			return
		}
		field.SetUint(v.(uint64))
	}
}

func createNotSupportedAttrToStringSetter(columnName string) func(any, bool) (string, error) {
	return func(v any, _ bool) (string, error) {
		return "", &BindError{columnName, fmt.Sprintf("type %T is not supported", v)}
	}
}

func createFloatAttrToStringSetter(setter fieldBindSetter) func(any, bool) (string, error) {
	return func(v any, fromBind bool) (string, error) {
		if fromBind {
			return v.(string), nil
		}
		v2, err := setter(v)
		if err != nil {
			return "", err
		}
		return v2.(string), nil
	}
}

func createBoolAttrToStringSetter(setter fieldBindSetter) func(any, bool) (string, error) {
	return func(v any, fromBind bool) (string, error) {
		if fromBind {
			if v.(bool) {
				return "1", nil
			}
			return "0", nil
		}
		v2, err := setter(v)
		if err != nil {
			return "", err
		}
		if v2.(bool) {
			return "1", nil
		}
		return "0", nil
	}
}

func createDateTimeAttrToStringSetter(setter fieldBindSetter) func(any, bool) (string, error) {
	return func(v any, fromBind bool) (string, error) {
		if fromBind {
			return v.(string), nil
		}
		v2, err := setter(v)
		if err != nil {
			return "", err
		}
		return v2.(string), nil
	}
}
