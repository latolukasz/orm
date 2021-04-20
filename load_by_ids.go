package orm

import (
	"fmt"
	"reflect"
	"strconv"
	"strings"

	jsoniter "github.com/json-iterator/go"
)

func tryByIDs(engine *Engine, ids []uint64, entities reflect.Value, references []string, lazy bool) (missing bool, schema *tableSchema) {
	lenIDs := len(ids)
	newSlice := reflect.MakeSlice(entities.Type(), lenIDs, lenIDs)
	if lenIDs == 0 {
		return
	}
	t, has, name := getEntityTypeForSlice(engine.registry, entities.Type(), true)
	if !has {
		panic(fmt.Errorf("entity '%s' is not registered", name))
	}

	schema = getTableSchema(engine.registry, t)
	hasLocalCache := schema.hasLocalCache
	hasRedis := schema.hasRedisCache

	if !hasLocalCache && engine.dataLoader != nil {
		data := engine.dataLoader.LoadAll(schema, ids)
		hasValid := false
		for i, row := range data {
			if row == nil {
				missing = true
			} else {
				entity := schema.newEntity()
				fillFromDBRow(ids[i], engine, row, entity, false, lazy)
				newSlice.Index(i).Set(entity.getORM().value)
				hasValid = true
			}
		}
		entities.Set(newSlice)
		if len(references) > 0 && hasValid {
			warmUpReferences(engine, schema, entities, references, true, lazy)
		}
		return
	}

	hasValid := false
	hasCache := hasLocalCache || hasRedis
	var localCache *LocalCache
	var redisCache *RedisCache

	if !hasLocalCache && engine.hasRequestCache {
		hasLocalCache = true
		localCache = engine.GetLocalCache(requestCacheKey)
	}

	cacheKeys := make([]string, 0)
	if hasCache {
		cacheKeys = make([]string, lenIDs)
		for i, id := range ids {
			cacheKeys[i] = schema.getCacheKey(id)
		}
	}
	var cacheMap map[int]int
	var dbMap map[int]int
	var localCacheToSet []interface{}
	var redisCacheToSet []interface{}
	if hasLocalCache {
		if localCache == nil {
			localCache, _ = schema.GetLocalCache(engine)
		}
		inCache := localCache.MGetFast(cacheKeys...)
		j := 0
		for i, val := range inCache {
			if val != nil {
				if val != "nil" {
					e := schema.newEntity()
					newSlice.Index(i).Set(e.getORM().value)
					fillFromDBRow(ids[i], engine, val.([]interface{}), e, false, lazy)
					hasValid = true
				} else {
					missing = true
				}
			} else if hasRedis {
				if dbMap == nil {
					dbMap = make(map[int]int)
				}
				dbMap[j] = i
				cacheKeys[j] = cacheKeys[i]
				if cacheMap == nil {
					cacheMap = make(map[int]int)
				}
				cacheMap[j] = i
				ids[j] = ids[i]
				j++
			} else {
				if dbMap == nil {
					dbMap = make(map[int]int)
				}
				dbMap[j] = i
				cacheKeys[j] = cacheKeys[i]
				ids[j] = ids[i]
				j++
			}
		}
		ids = ids[0:j]
		cacheKeys = cacheKeys[0:j]
	}
	if hasRedis && len(ids) > 0 {
		redisCache, _ = schema.GetRedisCache(engine)
		inCache := redisCache.MGetFast(cacheKeys...)
		j := 0
		for i, val := range inCache {
			if val != nil {
				if val != "nil" {
					k := i
					if hasLocalCache {
						k = cacheMap[k]
					}
					var decoded []interface{}
					_ = jsoniter.ConfigFastest.UnmarshalFromString(val.(string), &decoded)
					convertDataFromJSON(schema.fields, 0, decoded)
					e := schema.newEntity()
					newSlice.Index(k).Set(e.getORM().value)
					fillFromDBRow(ids[k], engine, decoded, e, false, lazy)
					hasValid = true
					if hasLocalCache {
						localCacheToSet = append(localCacheToSet, cacheKeys[i], buildLocalCacheValue(decoded))
					}
				} else {
					missing = true
					if hasLocalCache {
						localCacheToSet = append(localCacheToSet, cacheKeys[i], "nil")
					}
				}
			} else {
				if !hasLocalCache {
					if dbMap == nil {
						dbMap = make(map[int]int)
					}
					dbMap[j] = i
				}
				cacheKeys[j] = cacheKeys[i]
				ids[j] = ids[i]
				j++
			}
		}
		ids = ids[0:j]
		cacheKeys = cacheKeys[0:j]
	}
	if len(ids) > 0 {
		query := "SELECT " + schema.fieldsQuery + " FROM `" + schema.tableName + "` WHERE `ID` IN (" + strconv.FormatUint(ids[0], 10)
		idsMap := map[uint64]int{ids[0]: 0}
		for i, id := range ids[1:] {
			query += "," + strconv.FormatUint(id, 10)
			idsMap[id] = i + 1
		}
		query += ")"
		pool := schema.GetMysql(engine)
		found := 0
		results, def := pool.Query(query)
		defer def()
		for results.Next() {
			pointers := prepareScan(schema)
			results.Scan(pointers...)
			convertScan(schema.fields, 0, pointers)
			id := pointers[0].(uint64)
			k := idsMap[id]
			if dbMap != nil {
				k = dbMap[k]
			}
			e := schema.newEntity()
			newSlice.Index(k).Set(e.getORM().value)
			fillFromDBRow(id, engine, pointers, e, true, lazy)
			if hasCache {
				cacheKey := cacheKeys[idsMap[id]]
				if hasLocalCache {
					localCacheToSet = append(localCacheToSet, cacheKey, buildLocalCacheValue(pointers))
				}
				if hasRedis {
					redisCacheToSet = append(redisCacheToSet, cacheKey, buildRedisValue(pointers))
				}
			}
			hasValid = true
			found++
		}
		if hasCache && found < len(ids) {
			for _, id := range ids {
				k := idsMap[id]
				if dbMap != nil {
					k = dbMap[k]
				}
				if newSlice.Index(k).IsZero() {
					cacheKey := schema.getCacheKey(id)
					if hasLocalCache {
						localCacheToSet = append(localCacheToSet, cacheKey, "nil")
					}
					if hasRedis {
						redisCacheToSet = append(redisCacheToSet, cacheKey, "nil")
					}
				}
			}
		}
		if len(localCacheToSet) > 0 && localCache != nil {
			localCache.MSet(localCacheToSet...)
		}
		if len(redisCacheToSet) > 0 && redisCache != nil {
			redisCache.MSet(redisCacheToSet...)
		}
		if len(ids) != found {
			missing = true
		}
		def()
	}
	entities.Set(newSlice)
	if len(references) > 0 && hasValid {
		warmUpReferences(engine, schema, entities, references, true, lazy)
	}
	return
}

func warmUpReferences(engine *Engine, schema *tableSchema, rows reflect.Value, references []string, many bool, lazy bool) {
	dbMap := make(map[string]map[*tableSchema]map[string][]Entity)
	var localMap map[string]map[string][]Entity
	var redisMap map[string]map[string][]Entity
	l := 1
	if many {
		l = rows.Len()
	}
	if references[0] == "*" {
		references = schema.refOne
	}
	var referencesNextNames map[string][]string
	var referencesNextEntities map[string][]Entity
	for _, ref := range references {
		refName := ref
		pos := strings.Index(refName, "/")
		if pos > 0 {
			if referencesNextNames == nil {
				referencesNextNames = make(map[string][]string)
			}
			if referencesNextEntities == nil {
				referencesNextEntities = make(map[string][]Entity)
			}
			nextRef := refName[pos+1:]
			refName = refName[0:pos]
			referencesNextNames[refName] = append(referencesNextNames[refName], nextRef)
			referencesNextEntities[refName] = nil
		}
		_, has := schema.tags[refName]
		if !has {
			panic(fmt.Errorf("reference %s in %s is not valid", ref, schema.tableName))
		}
		parentRef, has := schema.tags[refName]["ref"]
		manyRef := false
		if !has {
			parentRef, has = schema.tags[refName]["refs"]
			manyRef = true
			if !has {
				panic(fmt.Errorf("reference tag %s is not valid", ref))
			}
		}
		parentSchema := engine.registry.tableSchemas[engine.registry.entities[parentRef]]
		hasLocalCache := parentSchema.hasLocalCache
		if !hasLocalCache && engine.hasRequestCache {
			hasLocalCache = true
		}
		if hasLocalCache && localMap == nil {
			localMap = make(map[string]map[string][]Entity)
		}
		if parentSchema.hasRedisCache && redisMap == nil {
			redisMap = make(map[string]map[string][]Entity)
		}
		for i := 0; i < l; i++ {
			var ref reflect.Value
			var refEntity reflect.Value
			if many {
				refEntity = rows.Index(i)
				if refEntity.IsZero() {
					continue
				}
				ref = reflect.Indirect(refEntity.Elem()).FieldByName(refName)
			} else {
				refEntity = rows
				ref = reflect.Indirect(refEntity).FieldByName(refName)
			}
			if !ref.IsValid() || ref.IsZero() {
				if !lazy {
					continue
				}
				dbData := refEntity.Interface().(Entity).getORM().dBData
				idVal := dbData[schema.columnMapping[refName]]
				if idVal == nil {
					continue
				}
				if manyRef {
					ids := make([]uint64, 0)
					_ = jsoniter.ConfigFastest.UnmarshalFromString(idVal.(string), &ids)
					length := len(ids)
					slice := reflect.MakeSlice(reflect.SliceOf(ref.Type().Elem()), length, length)
					for k, id := range ids {
						n := reflect.New(ref.Type().Elem().Elem())
						orm := initIfNeeded(engine.registry, n.Interface().(Entity))
						orm.idElem.SetUint(id)
						orm.inDB = true
						slice.Index(k).Set(n)
					}
					ref.Set(slice)
				} else {
					id := idVal.(uint64)
					if id == 0 {
						continue
					}
					n := reflect.New(ref.Type().Elem())
					orm := initIfNeeded(engine.registry, n.Interface().(Entity))
					orm.idElem.SetUint(id)
					orm.inDB = true
					ref.Set(n)
				}
			}
			if manyRef {
				length := ref.Len()
				for i := 0; i < length; i++ {
					e := ref.Index(i).Interface().(Entity)
					if !e.IsLoaded() {
						id := e.GetID()
						if id > 0 {
							fillRefMap(engine, id, referencesNextEntities, refName, e, parentSchema, dbMap, localMap, redisMap)
						}
					}
				}
			} else {
				e := ref.Interface().(Entity)
				if !e.IsLoaded() {
					id := e.GetID()
					if id > 0 {
						fillRefMap(engine, id, referencesNextEntities, refName, e, parentSchema, dbMap, localMap, redisMap)
					}
				}
			}
		}
	}
	for k, v := range localMap {
		l := len(v)
		if l == 1 {
			var key string
			for k := range v {
				key = k
				break
			}
			fromCache, has := engine.GetLocalCache(k).Get(key)
			if has && fromCache != "nil" {
				data := fromCache.([]interface{})
				for _, r := range v[key] {
					fillFromDBRow(data[0].(uint64), engine, data, r, false, lazy)
				}
				fillRef(key, localMap, redisMap, dbMap)
			}
		} else if l > 1 {
			keys := make([]string, len(v))
			i := 0
			for k := range v {
				keys[i] = k
				i++
			}
			for key, fromCache := range engine.GetLocalCache(k).MGet(keys...) {
				if fromCache != nil {
					data := fromCache.([]interface{})
					for _, r := range v[key] {
						fillFromDBRow(data[0].(uint64), engine, data, r, false, lazy)
					}
					fillRef(key, localMap, redisMap, dbMap)
				}
			}
		}
	}
	for k, v := range redisMap {
		l := len(v)
		if l == 0 {
			continue
		}
		keys := make([]string, l)
		i := 0
		for k := range v {
			keys[i] = k
			i++
		}
		for key, fromCache := range engine.GetRedis(k).MGet(keys...) {
			if fromCache != nil {
				schema := v[key][0].(Entity).getORM().tableSchema
				decoded := make([]interface{}, len(schema.columnNames))
				_ = jsoniter.ConfigFastest.UnmarshalFromString(fromCache.(string), &decoded)
				convertDataFromJSON(schema.fields, 0, decoded)
				for _, r := range v[key] {
					fillFromDBRow(decoded[0].(uint64), engine, decoded, r, false, lazy)
				}
				fillRef(key, nil, redisMap, dbMap)
			}
		}
	}
	for k, v := range dbMap {
		db := engine.GetMysql(k)
		for schema, v2 := range v {
			if len(v2) == 0 {
				continue
			}
			keys := make([]string, len(v2))
			q := make([]string, len(v2))
			i := 0
			for k2 := range v2 {
				keys[i] = k2[strings.Index(k2, ":")+1:]
				q[i] = keys[i]
				i++
			}
			query := "SELECT " + schema.fieldsQuery + " FROM `" + schema.tableName + "` WHERE `ID` IN (" + strings.Join(q, ",") + ")"
			results, def := db.Query(query)
			for results.Next() {
				pointers := prepareScan(schema)
				results.Scan(pointers...)
				convertScan(schema.fields, 0, pointers)
				id := pointers[0].(uint64)
				for _, r := range v2[schema.getCacheKey(id)] {
					fillFromDBRow(id, engine, pointers, r, false, lazy)
				}
			}
			def()
		}
	}
	for pool, v := range redisMap {
		if len(v) == 0 {
			continue
		}
		values := make([]interface{}, 0)
		for cacheKey, refs := range v {
			e := refs[0].(Entity)
			if e.IsLoaded() {
				values = append(values, cacheKey, buildRedisValue(e.getORM().dBData))
			} else {
				values = append(values, cacheKey, "nil")
			}
		}
		engine.GetRedis(pool).MSet(values...)
	}
	for pool, v := range localMap {
		if len(v) == 0 {
			continue
		}
		values := make([]interface{}, 0)
		for cacheKey, refs := range v {
			e := refs[0].(Entity)
			if e.IsLoaded() {
				values = append(values, cacheKey, buildLocalCacheValue(e.getORM().dBData))
			} else {
				values = append(values, cacheKey, "nil")
			}
		}
		engine.GetLocalCache(pool).MSet(values...)
	}

	for refName, entities := range referencesNextEntities {
		l := len(entities)
		if l == 1 {
			warmUpReferences(engine, entities[0].getORM().tableSchema, reflect.ValueOf(entities[0]),
				referencesNextNames[refName], false, lazy)
		} else if l > 1 {
			warmUpReferences(engine, entities[0].getORM().tableSchema, reflect.ValueOf(entities),
				referencesNextNames[refName], true, lazy)
		}
	}
}

func fillRef(key string, localMap map[string]map[string][]Entity,
	redisMap map[string]map[string][]Entity, dbMap map[string]map[*tableSchema]map[string][]Entity) {
	for _, p := range localMap {
		delete(p, key)
	}
	for _, p := range redisMap {
		delete(p, key)
	}
	for _, p := range dbMap {
		for _, p2 := range p {
			delete(p2, key)
		}
	}
}

func fillRefMap(engine *Engine, id uint64, referencesNextEntities map[string][]Entity, refName string, v Entity, parentSchema *tableSchema,
	dbMap map[string]map[*tableSchema]map[string][]Entity,
	localMap map[string]map[string][]Entity, redisMap map[string]map[string][]Entity) {
	_, has := referencesNextEntities[refName]
	if has {
		referencesNextEntities[refName] = append(referencesNextEntities[refName], v)
	}
	cacheKey := parentSchema.getCacheKey(id)
	if dbMap[parentSchema.mysqlPoolName] == nil {
		dbMap[parentSchema.mysqlPoolName] = make(map[*tableSchema]map[string][]Entity)
	}
	if dbMap[parentSchema.mysqlPoolName][parentSchema] == nil {
		dbMap[parentSchema.mysqlPoolName][parentSchema] = make(map[string][]Entity)
	}
	dbMap[parentSchema.mysqlPoolName][parentSchema][cacheKey] = append(dbMap[parentSchema.mysqlPoolName][parentSchema][cacheKey], v)
	hasLocalCache := parentSchema.hasLocalCache
	localCacheName := parentSchema.localCacheName
	if !hasLocalCache && engine.hasRequestCache {
		hasLocalCache = true
		localCacheName = requestCacheKey
	}
	if hasLocalCache {
		if localMap[localCacheName] == nil {
			localMap[localCacheName] = make(map[string][]Entity)
		}
		localMap[localCacheName][cacheKey] = append(localMap[localCacheName][cacheKey], v)
	}
	if parentSchema.hasRedisCache {
		if redisMap[parentSchema.redisCacheName] == nil {
			redisMap[parentSchema.redisCacheName] = make(map[string][]Entity)
		}
		redisMap[parentSchema.redisCacheName][cacheKey] = append(redisMap[parentSchema.redisCacheName][cacheKey], v)
	}
}
