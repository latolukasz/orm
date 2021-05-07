package orm

import (
	"fmt"
	"reflect"

	jsoniter "github.com/json-iterator/go"
)

const cacheNilValue = ""

func loadByID(engine *Engine, id uint64, entity Entity, useCache bool, lazy bool, references ...string) (found bool, schema *tableSchema) {
	orm := initIfNeeded(engine.registry, entity)
	schema = orm.tableSchema
	localCache, hasLocalCache := schema.GetLocalCache(engine)
	redisCache, hasRedis := schema.GetRedisCache(engine)
	if !hasLocalCache && engine.dataLoader != nil {
		e := engine.dataLoader.Load(schema, id)
		if e == nil {
			return false, schema
		}
		fillFromDBRow(id, engine, e, entity, false, lazy)
		if len(references) > 0 {
			warmUpReferences(engine, schema, orm.elem, references, false, lazy)
		}
		return true, schema
	}

	var cacheKey string
	if useCache {
		if !hasLocalCache && engine.hasRequestCache {
			hasLocalCache = true
			localCache = engine.GetLocalCache(requestCacheKey)
		}

		if hasLocalCache {
			cacheKey = schema.getCacheKey(id)
			e, has := localCache.Get(cacheKey)
			if has {
				if e == cacheNilValue {
					return false, schema
				}
				data := e.([]interface{})
				fillFromDBRow(id, engine, data, entity, false, lazy)
				if len(references) > 0 {
					warmUpReferences(engine, schema, orm.value, references, false, lazy)
				}
				return true, schema
			}
		}
		if hasRedis {
			cacheKey = schema.getCacheKey(id)
			row, has := redisCache.Get(cacheKey)
			if has {
				if row == cacheNilValue {
					return false, schema
				}
				decoded := make([]interface{}, len(schema.columnNames))
				_ = jsoniter.ConfigFastest.UnmarshalFromString(row, &decoded)
				convertDataFromJSON(schema.fields, 0, decoded)
				fillFromDBRow(id, engine, decoded, entity, false, lazy)
				if len(references) > 0 {
					warmUpReferences(engine, schema, orm.value, references, false, lazy)
				}
				return true, schema
			}
		}
	}

	found, _, data := searchRow(false, engine, NewWhere("`ID` = ?", id), entity, lazy, nil)
	if !found {
		if localCache != nil {
			localCache.Set(cacheKey, cacheNilValue)
		}
		if redisCache != nil {
			redisCache.Set(cacheKey, cacheNilValue, 60)
		}
		return false, schema
	}
	if useCache {
		if localCache != nil {
			localCache.Set(cacheKey, buildLocalCacheValue(data))
		}
		if redisCache != nil {
			redisCache.Set(cacheKey, buildRedisValue(data), 0)
		}
	}

	if len(references) > 0 {
		warmUpReferences(engine, schema, orm.elem, references, false, lazy)
	} else {
		data[0] = id
	}
	return true, schema
}

func buildRedisValue(data []interface{}) string {
	encoded, _ := jsoniter.ConfigFastest.Marshal(buildLocalCacheValue(data))
	return string(encoded)
}

func buildLocalCacheValue(data []interface{}) []interface{} {
	b := make([]interface{}, len(data))
	copy(b, data)
	return b
}

func initIfNeeded(registry *validatedRegistry, entity Entity) *ORM {
	orm := entity.getORM()
	if !orm.initialised {
		orm.initialised = true
		value := reflect.ValueOf(entity)
		elem := value.Elem()
		t := elem.Type()
		tableSchema := getTableSchema(registry, t)
		if tableSchema == nil {
			panic(fmt.Errorf("entity '%s' is not registered", t.String()))
		}
		orm.tableSchema = tableSchema
		orm.value = value
		orm.elem = elem
		orm.idElem = elem.Field(1)
	}
	return orm
}

func convertDataFromJSON(fields *tableFields, start int, encoded []interface{}) int {
	for i := 0; i < len(fields.uintegers); i++ {
		encoded[start] = uint64(encoded[start].(float64))
		start++
	}
	for i := 0; i < len(fields.uintegersNullable); i++ {
		v := encoded[start]
		if v != nil {
			encoded[start] = uint64(v.(float64))
		}
		start++
	}
	for i := 0; i < len(fields.integers); i++ {
		encoded[start] = int64(encoded[start].(float64))
		start++
	}
	for i := 0; i < len(fields.integersNullable); i++ {
		v := encoded[start]
		if v != nil {
			encoded[start] = int64(v.(float64))
		}
		start++
	}
	start += len(fields.strings) + len(fields.sliceStrings) + len(fields.bytes)
	if fields.fakeDelete > 0 {
		encoded[start] = uint64(encoded[start].(float64))
		start++
	}
	start += len(fields.booleans) + len(fields.booleansNullable) + len(fields.floats) + len(fields.floatsNullable) +
		len(fields.timesNullable) + len(fields.times) + len(fields.jsons)
	for i := 0; i < len(fields.refs); i++ {
		v := encoded[start]
		if v != nil {
			encoded[start] = uint64(v.(float64))
		}
		start++
	}
	start += len(fields.refsMany)
	for _, subFields := range fields.structs {
		start = convertDataFromJSON(subFields, start, encoded)
	}
	return start
}
