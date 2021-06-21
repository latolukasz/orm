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
	registry                  *validatedRegistry
	context                   context.Context
	dbs                       map[string]*DB
	clickHouseDbs             map[string]*ClickHouse
	localCache                map[string]*LocalCache
	redis                     map[string]*RedisCache
	redisSearch               map[string]*RedisSearch
	elastic                   map[string]*Elastic
	logMetaData               Bind
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
	logDebugOnce              sync.Once
	afterCommitLocalCacheSets map[string][]interface{}
	afterCommitRedisFlusher   *redisFlusher
	afterCommitDataLoaderSets dataLoaderSets
	eventBroker               *eventBroker
	serializer                *serializer
}

func (e *Engine) GetContext() context.Context {
	return e.context
}

func (e *Engine) Clone() *Engine {
	newEngine := &Engine{}
	newEngine.registry = e.registry
	newEngine.context = e.context
	newEngine.dataLoader = e.dataLoader
	return newEngine
}

func (e *Engine) Log() Log {
	if e.log == nil {
		e.log = newLog(e)
	}
	return e.log
}

func (e *Engine) EnableRequestCache(goroutines bool) {
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
	if e.logMetaData == nil {
		e.logMetaData = make(Bind)
	}
	e.logMetaData[key] = value
}

func (e *Engine) GetMysql(code ...string) *DB {
	dbCode := "default"
	if len(code) > 0 {
		dbCode = code[0]
	}
	db, has := e.dbs[dbCode]
	if !has {
		config, has := e.registry.mySQLServers[dbCode]
		if !has {
			panic(fmt.Errorf("unregistered mysql pool '%s'", dbCode))
		}
		db = &DB{engine: e, config: config, client: &standardSQLClient{db: config.getClient()}}
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
	cache, has := e.localCache[dbCode]
	if !has {
		config, has := e.registry.localCacheServers[dbCode]
		if !has {
			if dbCode == requestCacheKey {
				cache = &LocalCache{config: &localCachePoolConfig{code: dbCode, limit: 5000}, engine: e, lru: lru.New(5000)}
				if e.localCache == nil {
					e.localCache = map[string]*LocalCache{dbCode: cache}
				} else {
					e.localCache[dbCode] = cache
				}
				return cache
			}
			panic(fmt.Errorf("unregistered local cache pool '%s'", dbCode))
		}
		cache = &LocalCache{engine: e, config: config.(*localCachePoolConfig), lru: lru.New(config.GetLimit())}
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

func (e *Engine) DeleteLazy(entity Entity) {
	entity.markToDelete()
	e.FlushLazy(entity)
}

func (e *Engine) ForceDelete(entity Entity) {
	entity.forceMarkToDelete()
	e.Flush(entity)
}

func (e *Engine) ForceDeleteMany(entities ...Entity) {
	for _, entity := range entities {
		entity.forceMarkToDelete()
	}
	e.FlushMany(entities...)
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

func (e *Engine) SearchWithCountLazy(where *Where, pager *Pager, entities interface{}, references ...string) (totalRows int) {
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

func (e *Engine) Load(entity Entity, references ...string) (found bool) {
	return e.load(entity, false, references...)
}

func (e *Engine) LoadLazy(entity Entity, references ...string) {
	e.load(entity, true, references...)
}

func (e *Engine) load(entity Entity, lazy bool, references ...string) bool {
	if entity.IsLoaded() {
		if len(references) > 0 {
			orm := entity.getORM()
			warmUpReferences(e, orm.tableSchema, orm.elem, references, false, lazy)
		}
		return true
	}
	orm := initIfNeeded(e.registry, entity)
	id := orm.GetID()
	found := false
	if id > 0 {
		found, _ = loadByID(e, id, entity, true, lazy, references...)
	}
	return found
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

func (e *Engine) getSerializer() *serializer {
	if e.serializer == nil {
		e.serializer = newSerializer()
	}
	return e.serializer
}
