package orm

import (
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/pkg/errors"
)

type Bind map[string]interface{}

type DuplicatedKeyError struct {
	Message string
	Index   string
}

func (err *DuplicatedKeyError) Error() string {
	return err.Message
}

type ForeignKeyError struct {
	Message    string
	Constraint string
}

func (err *ForeignKeyError) Error() string {
	return err.Message
}

type dataLoaderSets map[*tableSchema]map[uint64][]interface{}

type Flusher interface {
	Track(entity ...Entity) Flusher
	Flush()
	FlushWithCheck() error
	FlushInTransactionWithCheck() error
	FlushWithFullCheck() error
	FlushLazy()
	FlushInTransaction()
	FlushWithLock(lockerPool string, lockName string, ttl time.Duration, waitTimeout time.Duration)
	FlushInTransactionWithLock(lockerPool string, lockName string, ttl time.Duration, waitTimeout time.Duration)
	Clear()
	MarkDirty(entity Entity, queueCode string, ids ...uint64)
	Delete(entity ...Entity) Flusher
	ForceDelete(entity ...Entity) Flusher
}

type flusher struct {
	engine                 *Engine
	trackedEntities        []Entity
	trackedEntitiesCounter int
	mutex                  sync.Mutex
	redisFlusher           *redisFlusher
	updateSQLs             map[string][]string
	deleteBinds            map[reflect.Type]map[uint64]Entity
	lazyMap                map[string]interface{}
	localCacheDeletes      map[string][]string
	localCacheSets         map[string][]interface{}
	dataLoaderSets         map[*tableSchema]map[uint64][]interface{}
}

func (f *flusher) Track(entity ...Entity) Flusher {
	f.mutex.Lock()
	defer f.mutex.Unlock()
	for _, entity := range entity {
		initIfNeeded(f.engine.registry, entity)
		if f.trackedEntities == nil {
			f.trackedEntities = []Entity{entity}
		} else {
			f.trackedEntities = append(f.trackedEntities, entity)
		}
		f.trackedEntitiesCounter++
		if f.trackedEntitiesCounter == 10001 {
			panic(fmt.Errorf("track limit 10000 exceeded"))
		}
	}
	return f
}

func (f *flusher) Delete(entity ...Entity) Flusher {
	for _, e := range entity {
		e.markToDelete()
	}
	f.Track(entity...)
	return f
}

func (f *flusher) ForceDelete(entity ...Entity) Flusher {
	for _, e := range entity {
		e.forceMarkToDelete()
	}
	f.Track(entity...)
	return f
}

func (f *flusher) Flush() {
	f.flushTrackedEntities(false, false)
}

func (f *flusher) FlushWithCheck() error {
	return f.flushWithCheck(false)
}

func (f *flusher) FlushInTransactionWithCheck() error {
	return f.flushWithCheck(true)
}

func (f *flusher) FlushWithFullCheck() error {
	var err error
	func() {
		defer func() {
			if r := recover(); r != nil {
				f.Clear()
				asErr := r.(error)
				err = asErr
			}
		}()
		f.flushTrackedEntities(false, false)
	}()
	return err
}

func (f *flusher) FlushLazy() {
	f.flushTrackedEntities(true, false)
}

func (f *flusher) FlushInTransaction() {
	f.flushTrackedEntities(false, true)
}

func (f *flusher) FlushWithLock(lockerPool string, lockName string, ttl time.Duration, waitTimeout time.Duration) {
	f.flushWithLock(false, lockerPool, lockName, ttl, waitTimeout)
}

func (f *flusher) FlushInTransactionWithLock(lockerPool string, lockName string, ttl time.Duration, waitTimeout time.Duration) {
	f.flushWithLock(true, lockerPool, lockName, ttl, waitTimeout)
}

func (f *flusher) Clear() {
	f.mutex.Lock()
	defer f.mutex.Unlock()
	f.trackedEntities = nil
	f.trackedEntitiesCounter = 0
	f.clear()
}

func (f *flusher) MarkDirty(entity Entity, queueCode string, ids ...uint64) {
	entityName := f.engine.GetRegistry().GetTableSchemaForEntity(entity).GetType().String()
	flusher := f.engine.GetEventBroker().NewFlusher()
	for _, id := range ids {
		flusher.PublishMap(queueCode, EventAsMap{"A": "u", "I": id, "E": entityName})
	}
	flusher.Flush()
}

func (f *flusher) flushTrackedEntities(lazy bool, transaction bool) {
	if f.trackedEntitiesCounter == 0 {
		return
	}
	f.mutex.Lock()
	defer f.mutex.Unlock()
	var dbPools map[string]*DB
	if transaction {
		dbPools = make(map[string]*DB)
		for _, entity := range f.trackedEntities {
			db := entity.getORM().tableSchema.GetMysql(f.engine)
			dbPools[db.code] = db
		}
		for _, db := range dbPools {
			db.Begin()
		}
	}
	defer func() {
		for _, db := range dbPools {
			db.Rollback()
		}
	}()
	f.flush(true, lazy, transaction, f.trackedEntities...)
	if transaction {
		for _, db := range dbPools {
			db.Commit()
		}
	}
	f.clear()
}

func (f *flusher) flushWithCheck(transaction bool) error {
	var err error
	func() {
		defer func() {
			if r := recover(); r != nil {
				f.Clear()
				asErr := r.(error)
				assErr1, is := asErr.(*ForeignKeyError)
				if is {
					err = assErr1
					return
				}
				assErr2, is := asErr.(*DuplicatedKeyError)
				if is {
					err = assErr2
					return
				}
				panic(asErr)
			}
		}()
		f.flushTrackedEntities(false, transaction)
	}()
	return err
}

func (f *flusher) flushWithLock(transaction bool, lockerPool string, lockName string, ttl time.Duration, waitTimeout time.Duration) {
	locker := f.engine.GetLocker(lockerPool)
	lock, has := locker.Obtain(f.engine.context, lockName, ttl, waitTimeout)
	if !has {
		panic(errors.New("lock wait timeout"))
	}
	defer lock.Release()
	f.flushTrackedEntities(false, transaction)
}

func (f *flusher) flush(root bool, lazy bool, transaction bool, entities ...Entity) {
	var insertKeys map[reflect.Type][]string
	insertArguments := make(map[reflect.Type][]interface{})
	insertBinds := make(map[reflect.Type][]map[string]interface{})
	insertReflectValues := make(map[reflect.Type][]Entity)
	isInTransaction := transaction

	var referencesToFlash map[Entity]Entity

	for _, entity := range entities {
		initIfNeeded(f.engine.registry, entity).initDBData()
		if entity.IsLazy() {
			panic(fmt.Errorf("lazy entity and can't be flushed: %v [%d]", entity.getORM().elem.Type().String(), entity.GetID()))
		}
		schema := entity.getORM().tableSchema
		if !isInTransaction && schema.GetMysql(f.engine).inTransaction {
			isInTransaction = true
		}
		for _, refName := range schema.refOne {
			refValue := entity.getORM().elem.FieldByName(refName)
			if refValue.IsValid() && !refValue.IsNil() {
				refEntity := refValue.Interface().(Entity)
				initIfNeeded(f.engine.registry, refEntity).initDBData()
				if refEntity.GetID() == 0 {
					if referencesToFlash == nil {
						referencesToFlash = make(map[Entity]Entity)
					}
					referencesToFlash[refEntity] = refEntity
				}
			}
		}
		for _, refName := range schema.refMany {
			refValue := entity.getORM().elem.FieldByName(refName)
			if refValue.IsValid() && !refValue.IsNil() {
				length := refValue.Len()
				for i := 0; i < length; i++ {
					refEntity := refValue.Index(i).Interface().(Entity)
					initIfNeeded(f.engine.registry, refEntity)
					if refEntity.GetID() == 0 {
						if referencesToFlash == nil {
							referencesToFlash = make(map[Entity]Entity)
						}
						referencesToFlash[refEntity] = refEntity
					}
				}
			}
		}
		if referencesToFlash != nil {
			continue
		}

		orm := entity.getORM()
		dbData := orm.dBData
		bind, updateBind, isDirty := orm.getDirtyBind()
		if !isDirty {
			continue
		}
		bindLength := len(bind)

		t := orm.tableSchema.t
		currentID := entity.GetID()
		if orm.fakeDelete && !orm.tableSchema.hasFakeDelete {
			orm.delete = true
		}
		if orm.delete {
			if f.deleteBinds == nil {
				f.deleteBinds = make(map[reflect.Type]map[uint64]Entity)
			}
			if f.deleteBinds[t] == nil {
				f.deleteBinds[t] = make(map[uint64]Entity)
			}
			f.deleteBinds[t][currentID] = entity
		} else if !orm.inDB {
			onUpdate := entity.getORM().onDuplicateKeyUpdate
			if onUpdate != nil {
				if lazy {
					panic(fmt.Errorf("lazy flush on duplicate key is not supported"))
				}
				if currentID > 0 {
					bind["ID"] = currentID
					bindLength++
				}
				values := make([]string, bindLength)
				columns := make([]string, bindLength)
				bindRow := make([]interface{}, bindLength)
				i := 0
				for key, val := range bind {
					columns[i] = "`" + key + "`"
					values[i] = "?"
					bindRow[i] = val
					i++
				}
				/* #nosec */
				sql := "INSERT INTO " + schema.tableName + "(" + strings.Join(columns, ",") + ") VALUES (" + strings.Join(values, ",") + ")"
				sql += " ON DUPLICATE KEY UPDATE "
				first := true
				for k, v := range onUpdate {
					if !first {
						sql += ", "
					}
					sql += "`" + k + "` = ?"
					bindRow = append(bindRow, v)
					first = false
				}
				if len(onUpdate) == 0 {
					sql += "`Id` = `Id`"
				}
				db := schema.GetMysql(f.engine)
				result := db.Exec(sql, bindRow...)
				affected := result.RowsAffected()
				if affected > 0 {
					lastID := result.LastInsertId()
					f.injectBind(entity, bind)
					orm := entity.getORM()
					orm.idElem.SetUint(lastID)
					orm.dBData[0] = lastID
					if affected == 1 {
						f.updateCacheForInserted(entity, lazy, lastID, bind)
					} else {
						for k, v := range onUpdate {
							err := entity.SetField(k, v)
							checkError(err)
						}
						bind, _ := orm.GetDirtyBind()
						_, _ = loadByID(f.engine, lastID, entity, false, lazy)
						f.updateCacheAfterUpdate(dbData, entity, bind, schema, lastID, false)
					}
				} else {
				OUTER:
					for _, index := range schema.uniqueIndices {
						fields := make([]string, 0)
						binds := make([]interface{}, 0)
						for _, column := range index {
							if bind[column] == nil {
								continue OUTER
							}
							fields = append(fields, "`"+column+"` = ?")
							binds = append(binds, bind[column])
						}
						findWhere := NewWhere(strings.Join(fields, " AND "), binds)
						f.engine.SearchOne(findWhere, entity)
						break
					}
				}
				continue
			}
			if currentID > 0 {
				bind["ID"] = currentID
				bindLength++
			}
			if insertKeys == nil {
				insertKeys = make(map[reflect.Type][]string)
			}
			if insertKeys[t] == nil {
				fields := make([]string, bindLength)
				i := 0
				for key := range bind {
					fields[i] = key
					i++
				}
				insertKeys[t] = fields
			}
			_, has := insertBinds[t]
			if !has {
				insertBinds[t] = make([]map[string]interface{}, 0)
			}
			for _, key := range insertKeys[t] {
				insertArguments[t] = append(insertArguments[t], bind[key])
			}
			insertReflectValues[t] = append(insertReflectValues[t], entity)
			insertBinds[t] = append(insertBinds[t], bind)
		} else {
			if !entity.IsLoaded() {
				panic(fmt.Errorf("entity is not loaded and can't be updated: %v [%d]", entity.getORM().elem.Type().String(), currentID))
			}
			/* #nosec */
			sql := "UPDATE " + schema.GetTableName() + " SET "
			first := true
			for key, value := range updateBind {
				if !first {
					sql += ","
				}
				first = false
				sql += "`" + key + "`=" + value
			}
			sql += " WHERE `ID` = " + strconv.FormatUint(currentID, 10)
			db := schema.GetMysql(f.engine)
			if lazy {
				var logEvents []*LogQueueValue
				var dirtyEvents []*dirtyQueueValue
				logEvent, dirtyEvent := f.updateCacheAfterUpdate(dbData, entity, bind, schema, currentID, true)
				if logEvent != nil {
					logEvents = append(logEvents, logEvent)
				}
				if dirtyEvent != nil {
					dirtyEvents = append(dirtyEvents, dirtyEvent)
				}
				f.fillLazyQuery(db.GetPoolCode(), sql, nil, logEvents, dirtyEvents)
			} else {
				if f.updateSQLs == nil {
					f.updateSQLs = make(map[string][]string)
				}
				f.updateSQLs[schema.mysqlPoolName] = append(f.updateSQLs[schema.mysqlPoolName], sql)
				f.updateCacheAfterUpdate(dbData, entity, bind, schema, currentID, false)
			}
		}
	}

	if referencesToFlash != nil {
		if lazy {
			panic(fmt.Errorf("lazy flush for unsaved references is not supported"))
		}
		toFlush := make([]Entity, len(referencesToFlash))
		i := 0
		for _, v := range referencesToFlash {
			toFlush[i] = v
			i++
		}
		f.flush(false, false, transaction, toFlush...)
		rest := make([]Entity, 0)
		for _, v := range entities {
			_, has := referencesToFlash[v]
			if !has {
				rest = append(rest, v)
			}
		}
		f.flush(true, false, transaction, rest...)
		return
	}
	for typeOf, values := range insertKeys {
		schema := getTableSchema(f.engine.registry, typeOf)
		/* #nosec */
		sql := "INSERT INTO " + schema.tableName
		l := len(values)
		if l > 0 {
			sql += "("
		}
		first := true
		for _, val := range values {
			if !first {
				sql += ","
			}
			first = false
			sql += "`" + val + "`"
		}
		if l > 0 {
			sql += ")"
		}
		sql += " VALUES "
		bindPart := "("
		if l > 0 {
			bindPart += "?"
		}
		for i := 1; i < l; i++ {
			bindPart += ",?"
		}
		bindPart += ")"
		l = len(insertBinds[typeOf])
		for i := 0; i < l; i++ {
			if i > 0 {
				sql += ","
			}
			sql += bindPart
		}
		db := schema.GetMysql(f.engine)
		if lazy {
			var logEvents []*LogQueueValue
			var dirtyEvents []*dirtyQueueValue
			for key, entity := range insertReflectValues[typeOf] {
				logEvent, dirtyEvent := f.updateCacheForInserted(entity, lazy, 0, insertBinds[typeOf][key])
				if logEvent != nil {
					logEvents = append(logEvents, logEvent)
				}
				if dirtyEvent != nil {
					dirtyEvents = append(dirtyEvents, dirtyEvent)
				}
			}
			f.fillLazyQuery(db.GetPoolCode(), sql, insertArguments[typeOf], logEvents, dirtyEvents)
		} else {
			res := db.Exec(sql, insertArguments[typeOf]...)
			id := res.LastInsertId()
			for key, entity := range insertReflectValues[typeOf] {
				bind := insertBinds[typeOf][key]
				f.injectBind(entity, bind)
				insertedID := entity.GetID()
				if insertedID == 0 {
					orm := entity.getORM()
					orm.idElem.SetUint(id)
					orm.dBData[0] = id
					insertedID = id
					id = id + db.autoincrement
				}
				f.updateCacheForInserted(entity, lazy, insertedID, bind)
			}
		}
	}
	if root {
		for pool, queries := range f.updateSQLs {
			db := f.engine.GetMysql(pool)
			if len(queries) == 1 {
				db.Exec(queries[0])
				continue
			}
			_, def := db.Query(strings.Join(queries, ";") + ";")
			def()
		}
		for typeOf, deleteBinds := range f.deleteBinds {
			schema := getTableSchema(f.engine.registry, typeOf)
			ids := make([]interface{}, len(deleteBinds))
			var logEvents []*LogQueueValue
			var dirtyEvents []*dirtyQueueValue
			i := 0
			for id, entity := range deleteBinds {
				ids[i] = id
				i++
				if lazy {
					orm := entity.getORM()
					bind := f.convertDBDataToMap(schema, orm.dBData)
					logEvent := f.addToLogQueue(schema, id, bind, nil, orm.logMeta, lazy)
					if logEvent != nil {
						logEvents = append(logEvents, logEvent)
					}
					dirtyEvent := f.addDirtyQueues(bind, schema, id, "d", lazy)
					if dirtyEvent != nil {
						dirtyEvents = append(dirtyEvents, dirtyEvent)
					}
				}
			}
			/* #nosec */
			sql := "DELETE FROM `" + schema.tableName + "` WHERE " + NewWhere("`ID` IN ?", ids).String()
			db := schema.GetMysql(f.engine)
			if lazy {
				f.fillLazyQuery(db.GetPoolCode(), sql, ids, logEvents, dirtyEvents)
			} else {
				usage := schema.GetUsage(f.engine.registry)
				if len(usage) > 0 {
					for refT, refColumns := range usage {
						for _, refColumn := range refColumns {
							refSchema := getTableSchema(f.engine.registry, refT)
							_, isCascade := refSchema.tags[refColumn]["cascade"]
							if isCascade {
								subValue := reflect.New(reflect.SliceOf(reflect.PtrTo(refT)))
								subElem := subValue.Elem()
								sub := subValue.Interface()
								pager := NewPager(1, 1000)
								where := NewWhere("`"+refColumn+"` IN ?", ids)
								for {
									f.engine.Search(where, pager, sub)
									total := subElem.Len()
									if total == 0 {
										break
									}
									toDeleteAll := make([]Entity, total)
									for i := 0; i < total; i++ {
										toDeleteValue := subElem.Index(i).Interface().(Entity)
										toDeleteValue.markToDelete()
										toDeleteAll[i] = toDeleteValue
									}
									f.flush(true, transaction, lazy, toDeleteAll...)
								}
							}
						}
					}
				}
				_ = db.Exec(sql, ids...)
			}

			localCache, hasLocalCache := schema.GetLocalCache(f.engine)
			redisCache, hasRedis := schema.GetRedisCache(f.engine)
			if !hasLocalCache && f.engine.hasRequestCache {
				hasLocalCache = true
				localCache = f.engine.GetLocalCache(requestCacheKey)
			}
			for id, entity := range deleteBinds {
				dbData := entity.getORM().dBData
				bind := f.convertDBDataToMap(schema, dbData)
				if !lazy {
					f.addDirtyQueues(bind, schema, id, "d", lazy)
					f.addToLogQueue(schema, id, bind, nil, entity.getORM().logMeta, lazy)
				}
				if hasLocalCache {
					f.addLocalCacheSet(localCache.code, schema.getCacheKey(id), "nil")
					keys := f.getCacheQueriesKeys(schema, bind, dbData, true)
					f.addLocalCacheDeletes(localCache.code, keys...)
				} else if f.engine.dataLoader != nil {
					f.addToDataLoader(schema, id, nil)
				}
				if hasRedis {
					f.getRedisFlusher().Del(redisCache.code, schema.getCacheKey(id))
					keys := f.getCacheQueriesKeys(schema, bind, dbData, true)
					f.getRedisFlusher().Del(redisCache.code, keys...)
				}
				if schema.hasSearchCache {
					key := schema.redisSearchPrefix + strconv.FormatUint(id, 10)
					f.getRedisFlusher().Del(schema.searchCacheName, key)
				}
			}
		}
		for cacheCode, keys := range f.localCacheSets {
			cache := f.engine.GetLocalCache(cacheCode)
			if !isInTransaction {
				cache.MSet(keys...)
			} else {
				if f.engine.afterCommitLocalCacheSets == nil {
					f.engine.afterCommitLocalCacheSets = make(map[string][]interface{})
				}
				f.engine.afterCommitLocalCacheSets[cacheCode] = append(f.engine.afterCommitLocalCacheSets[cacheCode], keys...)
			}
		}
	}
	if lazy {
		lazyMap := f.getLazyMap()
		deletesRedisCache, has := lazyMap["cr"].(map[string][]string)
		if !has {
			deletesRedisCache = make(map[string][]string)
			lazyMap["cr"] = deletesRedisCache
		}
		for cacheCode, commands := range f.getRedisFlusher().pipelines {
			if commands.deletes != nil {
				deletesRedisCache[cacheCode] = commands.deletes
			}
		}
		if f.localCacheDeletes != nil {
			deletesLocalCache, hasLocal := lazyMap["cl"].(map[string][]string)
			if !hasLocal {
				deletesLocalCache = make(map[string][]string)
				lazyMap["cl"] = deletesLocalCache
			}
			for cacheCode, keys := range f.localCacheDeletes {
				deletesLocalCache[cacheCode] = append(deletesLocalCache[cacheCode], keys...)
			}
		}
	} else if isInTransaction {
		f.engine.afterCommitRedisFlusher = f.getRedisFlusher()
	}
	for cacheCode, allKeys := range f.localCacheDeletes {
		f.engine.GetLocalCache(cacheCode).Remove(allKeys...)
	}
	for schema, rows := range f.dataLoaderSets {
		if !isInTransaction {
			for id, value := range rows {
				f.engine.dataLoader.Prime(schema, id, value)
			}
		} else {
			if f.engine.afterCommitDataLoaderSets == nil {
				f.engine.afterCommitDataLoaderSets = make(map[*tableSchema]map[uint64][]interface{})
			}
			if f.engine.afterCommitDataLoaderSets[schema] == nil {
				f.engine.afterCommitDataLoaderSets[schema] = make(map[uint64][]interface{})
			}
			for id, value := range rows {
				f.engine.afterCommitDataLoaderSets[schema][id] = value
			}
		}
	}
	if f.lazyMap != nil {
		f.getRedisFlusher().Publish(lazyChannelName, f.lazyMap)
		f.lazyMap = nil
	}
	if f.redisFlusher != nil && !isInTransaction && root {
		f.redisFlusher.Flush()
	}
}

func (f *flusher) updateCacheForInserted(entity Entity, lazy bool, id uint64, bind map[string]interface{}) (*LogQueueValue, *dirtyQueueValue) {
	schema := entity.getORM().tableSchema
	localCache, hasLocalCache := schema.GetLocalCache(f.engine)
	if !hasLocalCache && f.engine.hasRequestCache {
		hasLocalCache = true
		localCache = f.engine.GetLocalCache(requestCacheKey)
	}
	if hasLocalCache {
		if !lazy {
			f.addLocalCacheSet(localCache.code, schema.getCacheKey(id), buildLocalCacheValue(entity.getORM().dBData))
		} else {
			f.addLocalCacheDeletes(localCache.code, schema.getCacheKey(id))
		}
		keys := f.getCacheQueriesKeys(schema, bind, entity.getORM().dBData, true)
		f.addLocalCacheDeletes(localCache.code, keys...)
	} else if !lazy && f.engine.dataLoader != nil {
		f.addToDataLoader(schema, id, buildLocalCacheValue(entity.getORM().dBData))
	}
	redisCache, hasRedis := schema.GetRedisCache(f.engine)
	if hasRedis {
		f.getRedisFlusher().Del(redisCache.code, schema.getCacheKey(id))
		keys := f.getCacheQueriesKeys(schema, bind, entity.getORM().dBData, true)
		f.getRedisFlusher().Del(redisCache.code, keys...)
	}
	f.fillRedisSearchFromBind(schema, bind, id)
	return f.addToLogQueue(schema, id, nil, bind, entity.getORM().logMeta, lazy), f.addDirtyQueues(bind, schema, id, "i", lazy)
}

func (f *flusher) getRedisFlusher() *redisFlusher {
	if f.redisFlusher == nil {
		f.redisFlusher = f.engine.afterCommitRedisFlusher
		if f.redisFlusher == nil {
			f.redisFlusher = &redisFlusher{engine: f.engine}
		}
	}
	return f.redisFlusher
}

func (f *flusher) getLazyMap() map[string]interface{} {
	if f.lazyMap == nil {
		f.lazyMap = make(map[string]interface{})
	}
	return f.lazyMap
}

func (f *flusher) updateCacheAfterUpdate(dbData []interface{}, entity Entity, bind Bind, schema *tableSchema, currentID uint64, lazy bool) (*LogQueueValue, *dirtyQueueValue) {
	var old []interface{}
	localCache, hasLocalCache := schema.GetLocalCache(f.engine)
	redisCache, hasRedis := schema.GetRedisCache(f.engine)
	if hasLocalCache || hasRedis || schema.hasLog {
		old = make([]interface{}, len(dbData))
		copy(old, dbData)
	}
	f.injectBind(entity, bind)
	if !hasLocalCache && f.engine.hasRequestCache {
		hasLocalCache = true
		localCache = f.engine.GetLocalCache(requestCacheKey)
	}
	if hasLocalCache {
		cacheKey := schema.getCacheKey(currentID)
		f.addLocalCacheSet(localCache.code, cacheKey, buildLocalCacheValue(entity.getORM().dBData))
		keys := f.getCacheQueriesKeys(schema, bind, dbData, false)
		f.addLocalCacheDeletes(localCache.code, keys...)
		keys = f.getCacheQueriesKeys(schema, bind, old, false)
		f.addLocalCacheDeletes(localCache.code, keys...)
	} else if f.engine.dataLoader != nil {
		f.addToDataLoader(schema, currentID, buildLocalCacheValue(entity.getORM().dBData))
	}
	if hasRedis {
		redisFlusher := f.getRedisFlusher()
		redisFlusher.Del(redisCache.code, schema.getCacheKey(currentID))
		keys := f.getCacheQueriesKeys(schema, bind, dbData, false)
		redisFlusher.Del(redisCache.code, keys...)
		keys = f.getCacheQueriesKeys(schema, bind, old, false)
		redisFlusher.Del(redisCache.code, keys...)
	}
	f.fillRedisSearchFromBind(schema, bind, entity.GetID())
	dirtyValue := f.addDirtyQueues(bind, schema, currentID, "u", lazy)
	if schema.hasLog {
		return f.addToLogQueue(schema, currentID, f.convertDBDataToMap(schema, old), bind, entity.getORM().logMeta, lazy), dirtyValue
	}
	return nil, dirtyValue
}

func (f *flusher) addDirtyQueues(bind map[string]interface{}, schema *tableSchema, id uint64, action string, lazy bool) *dirtyQueueValue {
	var key EventAsMap
	var allStreams []string
	for column, tags := range schema.tags {
		queues, has := tags["dirty"]
		if !has {
			continue
		}
		isDirty := column == "ORM"
		if !isDirty {
			_, isDirty = bind[column]
		}
		if !isDirty {
			continue
		}
		queueNames := strings.Split(queues, ",")
		if key == nil {
			key = EventAsMap{"E": schema.t.String(), "I": id, "A": action}
		}

		for _, queueName := range queueNames {
			if !lazy {
				f.getRedisFlusher().PublishMap(queueName, key)
			} else {
				allStreams = append(allStreams, queueName)
			}
		}
	}
	if key == nil {
		return nil
	}
	return &dirtyQueueValue{Event: key, Streams: allStreams}
}

func (f *flusher) addToLogQueue(tableSchema *tableSchema, id uint64, before Bind, changes Bind, entityMeta Bind, lazy bool) *LogQueueValue {
	if !tableSchema.hasLog {
		return nil
	}
	if changes != nil && len(tableSchema.skipLogs) > 0 {
		skipped := 0
		for _, skip := range tableSchema.skipLogs {
			_, has := changes[skip]
			if has {
				skipped++
			}
		}
		if skipped == len(changes) {
			return nil
		}
	}
	val := &LogQueueValue{TableName: tableSchema.logTableName, ID: id,
		PoolName: tableSchema.logPoolName, Before: before,
		Changes: changes, Updated: time.Now(), Meta: entityMeta}
	if val.Meta == nil {
		val.Meta = f.engine.logMetaData
	} else {
		for k, v := range f.engine.logMetaData {
			val.Meta[k] = v
		}
	}
	if !lazy {
		f.getRedisFlusher().Publish(logChannelName, val)
	}
	return val
}

func (f *flusher) fillRedisSearchFromBind(schema *tableSchema, bind map[string]interface{}, id uint64) {
	if schema.hasSearchCache {
		if schema.hasFakeDelete {
			val, has := bind["FakeDelete"]
			if has && val.(uint64) > 0 {
				f.getRedisFlusher().Del(schema.searchCacheName, schema.redisSearchPrefix+strconv.FormatUint(id, 10))
			}
		}
		values := make([]interface{}, 0)
		idMap, has := schema.mapBindToRedisSearch["ID"]
		if has {
			values = append(values, "ID", idMap(id))
		}
		hasChangedField := false
		for k, f := range schema.mapBindToRedisSearch {
			v, has := bind[k]
			if has {
				values = append(values, k, f(v))
				hasChangedField = true
			}
		}
		if hasChangedField {
			f.getRedisFlusher().HSet(schema.searchCacheName, schema.redisSearchPrefix+strconv.FormatUint(id, 10), values...)
		}
	}
}

func (f *flusher) convertDBDataToMap(schema *tableSchema, data []interface{}) map[string]interface{} {
	m := make(map[string]interface{})
	for _, name := range schema.columnNames[1:] {
		m[name] = data[schema.columnMapping[name]]
	}
	return m
}

func (f *flusher) injectBind(entity Entity, bind map[string]interface{}) {
	orm := entity.getORM()
	mapping := orm.tableSchema.columnMapping
	orm.initDBData()
	for key, value := range bind {
		orm.dBData[mapping[key]] = value
	}
	orm.loaded = true
	orm.inDB = true
}

func (f *flusher) getCacheQueriesKeys(schema *tableSchema, bind map[string]interface{}, data []interface{}, addedDeleted bool) (keys []string) {
	keys = make([]string, 0)

	for indexName, definition := range schema.cachedIndexesAll {
		if !addedDeleted && schema.hasFakeDelete {
			_, addedDeleted = bind["FakeDelete"]
		}
		if addedDeleted && len(definition.TrackedFields) == 0 {
			keys = append(keys, getCacheKeySearch(schema, indexName))
		}
		for _, trackedField := range definition.TrackedFields {
			_, has := bind[trackedField]
			if has {
				attributes := make([]interface{}, 0)
				for _, trackedFieldSub := range definition.QueryFields {
					val := data[schema.columnMapping[trackedFieldSub]]
					if !schema.hasFakeDelete || trackedFieldSub != "FakeDelete" {
						attributes = append(attributes, val)
					}
				}
				keys = append(keys, getCacheKeySearch(schema, indexName, attributes...))
				break
			}
		}
	}
	return
}

func (f *flusher) addLocalCacheSet(cacheCode string, keys ...interface{}) {
	if f.localCacheSets == nil {
		f.localCacheSets = make(map[string][]interface{})
	}
	f.localCacheSets[cacheCode] = append(f.localCacheSets[cacheCode], keys...)
}

func (f *flusher) addToDataLoader(schema *tableSchema, id uint64, value []interface{}) {
	if f.dataLoaderSets == nil {
		f.dataLoaderSets = make(map[*tableSchema]map[uint64][]interface{})
	}
	if f.dataLoaderSets[schema] == nil {
		f.dataLoaderSets[schema] = make(map[uint64][]interface{})
	}
	f.dataLoaderSets[schema][id] = value
}

func (f *flusher) addLocalCacheDeletes(cacheCode string, keys ...string) {
	if len(keys) == 0 {
		return
	}
	if f.localCacheDeletes == nil {
		f.localCacheDeletes = make(map[string][]string)
	}
	f.localCacheDeletes[cacheCode] = append(f.localCacheDeletes[cacheCode], keys...)
}

func (f *flusher) fillLazyQuery(dbCode string, sql string, values []interface{}, logEvent []*LogQueueValue, dirtyData []*dirtyQueueValue) {
	lazyMap := f.getLazyMap()
	updatesMap := lazyMap["q"]
	if updatesMap == nil {
		updatesMap = make([]interface{}, 0)
		lazyMap["q"] = updatesMap
	}
	lazyValue := make([]interface{}, 3)
	lazyValue[0] = dbCode
	lazyValue[1] = sql
	lazyValue[2] = values
	lazyMap["q"] = append(updatesMap.([]interface{}), lazyValue)
	if len(logEvent) > 0 {
		lazyMap["l"] = logEvent
	}
	if len(dirtyData) > 0 {
		lazyMap["d"] = dirtyData
	}
}

func (f *flusher) clear() {
	f.updateSQLs = nil
	f.deleteBinds = nil
	f.localCacheDeletes = nil
	f.localCacheSets = nil
	f.dataLoaderSets = nil
}
