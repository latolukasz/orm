package orm

import (
	"context"
	"fmt"
	"os"
	"reflect"
	"sync"

	"github.com/golang/groupcache/lru"

	logApex "github.com/apex/log"

	levelHandler "github.com/apex/log/handlers/level"
	"github.com/apex/log/handlers/text"
)

var defaultQueryDebug = text.New(os.Stdout)

type Engine struct {
	mutex                     sync.Mutex
	registry                  *validatedRegistry
	context                   context.Context
	dbs                       map[string]*DB
	dbsMutex                  sync.Mutex
	clickHouseDbs             map[string]*ClickHouse
	clickHouseMutex           sync.Mutex
	localCache                map[string]*LocalCache
	localCacheMutex           sync.Mutex
	redis                     map[string]*RedisCache
	redisMutex                sync.Mutex
	redisSearch               map[string]*RedisSearch
	redisSearchMutex          sync.Mutex
	elastic                   map[string]*Elastic
	elasticMutex              sync.Mutex
	logMetaData               map[string]interface{}
	logMetaDataMutex          sync.RWMutex
	dataLoader                *dataLoader
	hasRequestCache           bool
	queryLoggers              map[QueryLoggerSource]*logger
	hasRedisLogger            bool
	hasStreamsLogger          bool
	hasDBLogger               bool
	hasClickHouseLogger       bool
	hasElasticLogger          bool
	hasLocalCacheLogger       bool
	log                       *log
	logOnce                   sync.Once
	logMutex                  sync.Mutex
	logDebugOnce              sync.Once
	afterCommitLocalCacheSets map[string][]interface{}
	afterCommitRedisFlusher   *redisFlusher
	afterCommitDataLoaderSets dataLoaderSets
	eventBroker               *eventBroker
}

func (e *Engine) Log() Log {
	e.logOnce.Do(func() {
		e.log = newLog(e)
	})
	return e.log
}

func (e *Engine) EnableRequestCache(goroutines bool) {
	e.mutex.Lock()
	defer e.mutex.Unlock()
	if goroutines {
		e.dataLoader = &dataLoader{engine: e, maxBatchSize: dataLoaderMaxPatch}
		e.hasRequestCache = false
	} else {
		e.hasRequestCache = true
		e.dataLoader = nil
	}
}

func (e *Engine) EnableLogger(level logApex.Level, handlers ...logApex.Handler) {
	if len(handlers) == 0 {
		handlers = []logApex.Handler{&jsonHandler{}}
	}
	l := e.Log()
	e.logMutex.Lock()
	defer e.logMutex.Unlock()
	for _, handler := range handlers {
		l.(*log).logger.handler.Handlers = append(e.log.logger.handler.Handlers, levelHandler.New(handler, level))
	}
}

func (e *Engine) EnableDebug() {
	l := e.Log()
	e.logDebugOnce.Do(func() {
		l.(*log).logger.handler.Handlers = append(e.log.logger.handler.Handlers, levelHandler.New(text.New(os.Stderr), logApex.DebugLevel))
	})
}

func (e *Engine) AddQueryLogger(handler logApex.Handler, level logApex.Level, source ...QueryLoggerSource) {
	if len(source) == 0 {
		source = []QueryLoggerSource{QueryLoggerSourceDB, QueryLoggerSourceRedis, QueryLoggerSourceElastic,
			QueryLoggerSourceClickHouse, QueryLoggerSourceStreams}
	}
	e.logMutex.Lock()
	defer e.logMutex.Unlock()
	if e.queryLoggers == nil {
		e.queryLoggers = make(map[QueryLoggerSource]*logger)
	}
	newHandler := levelHandler.New(handler, level)
MAIN:
	for _, source := range source {
		l, has := e.queryLoggers[source]
		if has {
			for _, v := range l.handler.Handlers {
				asLevel, is := v.(*levelHandler.Handler)
				if is && asLevel.Handler == handler {
					continue MAIN
				}
			}
			l.handler.Handlers = append(l.handler.Handlers, newHandler)
		} else {
			e.queryLoggers[source] = e.newLogger(newHandler, level)
			switch source {
			case QueryLoggerSourceRedis:
				e.hasRedisLogger = true
			case QueryLoggerSourceStreams:
				e.hasStreamsLogger = true
			case QueryLoggerSourceDB:
				e.hasDBLogger = true
			case QueryLoggerSourceClickHouse:
				e.hasClickHouseLogger = true
			case QueryLoggerSourceElastic:
				e.hasElasticLogger = true
			case QueryLoggerSourceLocalCache:
				e.hasLocalCacheLogger = true
			}
		}
	}
}

func (e *Engine) EnableQueryDebug(source ...QueryLoggerSource) {
	e.AddQueryLogger(defaultQueryDebug, logApex.DebugLevel, source...)
}

func (e *Engine) SetLogMetaData(key string, value interface{}) {
	e.logMetaDataMutex.Lock()
	defer e.logMetaDataMutex.Unlock()
	if e.logMetaData == nil {
		e.logMetaData = make(map[string]interface{})
	}
	e.logMetaData[key] = value
}

func (e *Engine) GetMysql(code ...string) *DB {
	dbCode := "default"
	if len(code) > 0 {
		dbCode = code[0]
	}
	e.dbsMutex.Lock()
	defer e.dbsMutex.Unlock()
	db, has := e.dbs[dbCode]
	if !has {
		val, has := e.registry.sqlClients[dbCode]
		if !has {
			panic(fmt.Errorf("unregistered mysql pool '%s'", dbCode))
		}
		db = &DB{engine: e, code: val.code, databaseName: val.databaseName,
			client: &standardSQLClient{db: val.db}, autoincrement: val.autoincrement, version: val.version}
		if e.dbs == nil {
			e.dbs = map[string]*DB{dbCode: db}
		} else {
			e.dbs[dbCode] = db
		}
	}
	return db
}

func (e *Engine) GetLocalCache(code ...string) *LocalCache {
	dbCode := "default"
	if len(code) > 0 {
		dbCode = code[0]
	}
	e.localCacheMutex.Lock()
	defer e.localCacheMutex.Unlock()
	cache, has := e.localCache[dbCode]
	if !has {
		val, has := e.registry.localCacheContainers[dbCode]
		if !has {
			if dbCode == requestCacheKey {
				cache = &LocalCache{code: dbCode, engine: e, m: &sync.Mutex{}, lru: lru.New(5000)}
				if e.localCache == nil {
					e.localCache = map[string]*LocalCache{dbCode: cache}
				} else {
					e.localCache[dbCode] = cache
				}
				return cache
			}
			panic(fmt.Errorf("unregistered local cache pool '%s'", dbCode))
		}
		cache = &LocalCache{engine: e, code: val.code, lru: val.lru, m: &val.m}
		if e.localCache == nil {
			e.localCache = map[string]*LocalCache{dbCode: cache}
		} else {
			e.localCache[dbCode] = cache
		}
	}
	return cache
}

func (e *Engine) GetRedis(code ...string) *RedisCache {
	dbCode := "default"
	if len(code) > 0 {
		dbCode = code[0]
	}
	e.redisMutex.Lock()
	defer e.redisMutex.Unlock()
	cache, has := e.redis[dbCode]
	if !has {
		config, has := e.registry.redisServers[dbCode]
		if !has {
			panic(fmt.Errorf("unregistered redis cache pool '%s'", dbCode))
		}
		client := config.getClient()
		if client != nil {
			client = client.WithContext(e.context)
		}
		cache = &RedisCache{engine: e, config: config, client: client, ctx: context.Background()}
		if e.redis == nil {
			e.redis = map[string]*RedisCache{dbCode: cache}
		} else {
			e.redis[dbCode] = cache
		}
	}
	return cache
}

func (e *Engine) GetRedisSearch(code ...string) *RedisSearch {
	dbCode := "default"
	if len(code) > 0 {
		dbCode = code[0]
	}
	e.redisSearchMutex.Lock()
	defer e.redisSearchMutex.Unlock()
	cache, has := e.redisSearch[dbCode]
	if !has {
		config, has := e.registry.redisServers[dbCode]
		if !has {
			panic(fmt.Errorf("unregistered redis cache pool '%s'", dbCode))
		}
		client := config.getClient()
		if client != nil {
			client = client.WithContext(e.context)
		}
		redisClient := &RedisCache{engine: e, config: config, client: client, ctx: context.Background()}
		cache = &RedisSearch{engine: e, redis: redisClient, ctx: context.Background()}
		if e.redisSearch == nil {
			e.redisSearch = map[string]*RedisSearch{dbCode: cache}
		} else {
			e.redisSearch[dbCode] = cache
		}
	}
	return cache
}

func (e *Engine) GetClickHouse(code ...string) *ClickHouse {
	dbCode := "default"
	if len(code) > 0 {
		dbCode = code[0]
	}
	e.clickHouseMutex.Lock()
	defer e.clickHouseMutex.Unlock()
	ch, has := e.clickHouseDbs[dbCode]
	if !has {
		val, has := e.registry.clickHouseClients[dbCode]
		if !has {
			panic(fmt.Errorf("unregistered clickhouse pool '%s'", dbCode))
		}
		ch = &ClickHouse{engine: e, code: val.code, client: val.db}
		if e.clickHouseDbs == nil {
			e.clickHouseDbs = map[string]*ClickHouse{dbCode: ch}
		} else {
			e.clickHouseDbs[dbCode] = ch
		}
	}
	return ch
}

func (e *Engine) GetElastic(code ...string) *Elastic {
	dbCode := "default"
	if len(code) > 0 {
		dbCode = code[0]
	}
	e.elasticMutex.Lock()
	defer e.elasticMutex.Unlock()
	elastic, has := e.elastic[dbCode]
	if !has {
		val, has := e.registry.elasticServers[dbCode]
		if !has {
			panic(fmt.Errorf("unregistered elastic pool '%s'", dbCode))
		}
		elastic = &Elastic{engine: e, code: val.code, client: val.client}
		if e.elastic == nil {
			e.elastic = map[string]*Elastic{dbCode: elastic}
		} else {
			e.elastic[dbCode] = elastic
		}
	}
	return elastic
}

func (e *Engine) NewFlusher() Flusher {
	return &flusher{engine: e}
}

func (e *Engine) NewRedisFlusher() RedisFlusher {
	return &redisFlusher{engine: e}
}

func (e *Engine) Flush(entity Entity) {
	e.FlushMany(entity)
}

func (e *Engine) FlushLazy(entity Entity) {
	e.FlushLazyMany(entity)
}

func (e *Engine) FlushMany(entities ...Entity) {
	e.NewFlusher().Track(entities...).Flush()
}

func (e *Engine) FlushLazyMany(entities ...Entity) {
	e.NewFlusher().Track(entities...).FlushLazy()
}

func (e *Engine) FlushWithCheck(entity ...Entity) error {
	return e.NewFlusher().Track(entity...).FlushWithCheck()
}

func (e *Engine) Delete(entity Entity) {
	entity.markToDelete()
	e.Flush(entity)
}

func (e *Engine) ForceDelete(entity Entity) {
	entity.forceMarkToDelete()
	e.Flush(entity)
}

func (e *Engine) DeleteMany(entities ...Entity) {
	for _, entity := range entities {
		entity.markToDelete()
	}
	e.FlushMany(entities...)
}

func (e *Engine) GetRegistry() ValidatedRegistry {
	return e.registry
}

func (e *Engine) SearchWithCount(where *Where, pager *Pager, entities interface{}, references ...string) (totalRows int) {
	return search(true, e, where, pager, true, false, true, reflect.ValueOf(entities).Elem(), references...)
}

func (e *Engine) SearchWithCountLAzy(where *Where, pager *Pager, entities interface{}, references ...string) (totalRows int) {
	return search(true, e, where, pager, true, true, true, reflect.ValueOf(entities).Elem(), references...)
}

func (e *Engine) Search(where *Where, pager *Pager, entities interface{}, references ...string) {
	search(true, e, where, pager, false, false, true, reflect.ValueOf(entities).Elem(), references...)
}

func (e *Engine) SearchLazy(where *Where, pager *Pager, entities interface{}, references ...string) {
	search(true, e, where, pager, false, true, true, reflect.ValueOf(entities).Elem(), references...)
}

func (e *Engine) SearchIDsWithCount(where *Where, pager *Pager, entity Entity) (results []uint64, totalRows int) {
	return searchIDsWithCount(true, e, where, pager, reflect.TypeOf(entity).Elem())
}

func (e *Engine) SearchIDs(where *Where, pager *Pager, entity Entity) []uint64 {
	results, _ := searchIDs(true, e, where, pager, false, reflect.TypeOf(entity).Elem())
	return results
}

func (e *Engine) SearchOne(where *Where, entity Entity, references ...string) (found bool) {
	found, _, _ = searchOne(true, e, where, entity, false, references)
	return found
}

func (e *Engine) SearchOneLazy(where *Where, entity Entity, references ...string) (found bool) {
	found, _, _ = searchOne(true, e, where, entity, true, references)
	return found
}

func (e *Engine) CachedSearchOne(entity Entity, indexName string, arguments ...interface{}) (found bool) {
	found, _ = cachedSearchOne(e, entity, indexName, true, false, arguments, nil)
	return found
}

func (e *Engine) CachedSearchOneLazy(entity Entity, indexName string, arguments ...interface{}) (found bool) {
	found, _ = cachedSearchOne(e, entity, indexName, true, true, arguments, nil)
	return found
}

func (e *Engine) CachedSearchOneWithReferences(entity Entity, indexName string, arguments []interface{}, references []string) (found bool) {
	found, _ = cachedSearchOne(e, entity, indexName, true, false, arguments, references)
	return found
}

func (e *Engine) CachedSearchOneWithReferencesLazy(entity Entity, indexName string, arguments []interface{}, references []string) (found bool) {
	found, _ = cachedSearchOne(e, entity, indexName, true, true, arguments, references)
	return found
}

func (e *Engine) CachedSearch(entities interface{}, indexName string, pager *Pager, arguments ...interface{}) (totalRows int) {
	total, _ := cachedSearch(e, entities, indexName, pager, arguments, false, true, nil)
	return total
}

func (e *Engine) CachedSearchLazy(entities interface{}, indexName string, pager *Pager, arguments ...interface{}) (totalRows int) {
	total, _ := cachedSearch(e, entities, indexName, pager, arguments, true, true, nil)
	return total
}

func (e *Engine) CachedSearchIDs(entity Entity, indexName string, pager *Pager, arguments ...interface{}) (totalRows int, ids []uint64) {
	return cachedSearch(e, entity, indexName, pager, arguments, false, false, nil)
}

func (e *Engine) CachedSearchCount(entity Entity, indexName string, arguments ...interface{}) int {
	total, _ := cachedSearch(e, entity, indexName, NewPager(1, 1), arguments, false, false, nil)
	return total
}

func (e *Engine) CachedSearchWithReferences(entities interface{}, indexName string, pager *Pager,
	arguments []interface{}, references []string) (totalRows int) {
	total, _ := cachedSearch(e, entities, indexName, pager, arguments, false, true, references)
	return total
}

func (e *Engine) CachedSearchWithReferencesLazy(entities interface{}, indexName string, pager *Pager,
	arguments []interface{}, references []string) (totalRows int) {
	total, _ := cachedSearch(e, entities, indexName, pager, arguments, true, true, references)
	return total
}

func (e *Engine) ClearByIDs(entity Entity, ids ...uint64) {
	clearByIDs(e, entity, ids...)
}

func (e *Engine) LoadByID(id uint64, entity Entity, references ...string) (found bool) {
	found, _ = loadByID(e, id, entity, true, false, references...)
	return found
}

func (e *Engine) LoadByIDLazy(id uint64, entity Entity, references ...string) (found bool) {
	found, _ = loadByID(e, id, entity, true, true, references...)
	return found
}

func (e *Engine) Load(entity Entity, references ...string) {
	e.load(entity, false, references...)
}

func (e *Engine) LoadLazy(entity Entity, references ...string) {
	e.load(entity, true, references...)
}

func (e *Engine) load(entity Entity, lazy bool, references ...string) {
	if entity.IsLoaded() {
		if len(references) > 0 {
			orm := entity.getORM()
			warmUpReferences(e, orm.tableSchema, orm.elem, references, false, lazy)
		}
		return
	}
	orm := initIfNeeded(e.registry, entity)
	id := orm.GetID()
	if id > 0 {
		loadByID(e, id, entity, true, lazy, references...)
	}
}

func (e *Engine) LoadByIDs(ids []uint64, entities interface{}, references ...string) (missing bool) {
	missing, _ = tryByIDs(e, ids, reflect.ValueOf(entities).Elem(), references, false)
	return missing
}

func (e *Engine) LoadByIDsLazy(ids []uint64, entities interface{}, references ...string) (missing bool) {
	missing, _ = tryByIDs(e, ids, reflect.ValueOf(entities).Elem(), references, true)
	return missing
}

func (e *Engine) GetAlters() (alters []Alter) {
	return getAlters(e)
}

func (e *Engine) GetRedisSearchIndexAlters() (alters []RedisSearchIndexAlter) {
	return getRedisSearchAlters(e)
}

func (e *Engine) GetElasticIndexAlters() (alters []ElasticIndexAlter) {
	return getElasticIndexAlters(e)
}
