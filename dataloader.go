package orm

import (
	"strconv"
	"strings"
	"sync"
	"time"
)

const dataLoaderMaxPatch = 200
const dataLoaderWait = time.Millisecond

type dataLoader struct {
	engine       *Engine
	cache        map[string][]byte
	batch        *dataLoaderBatch
	maxBatchSize int
	mu           sync.Mutex
}

type dataLoaderBatch struct {
	keys    []string
	data    [][]byte
	closing bool
	done    chan struct{}
}

func (l *dataLoader) Load(schema TableSchema, id uint64) []byte {
	return l.loadThunk(l.key(schema, id))()
}

func (l *dataLoader) LoadAll(schema TableSchema, ids []uint64) [][]byte {
	results := make([]func() []byte, len(ids))

	for i, id := range ids {
		results[i] = l.loadThunk(l.key(schema, id))
	}

	data := make([][]byte, len(ids))
	for i, thunk := range results {
		data[i] = thunk()
	}
	return data
}

func (l *dataLoader) Prime(schema TableSchema, id uint64, value []byte) {
	key := l.key(schema, id)
	l.mu.Lock()
	l.unsafeSet(key, value)
	l.mu.Unlock()
}

func (l *dataLoader) Clear() {
	l.mu.Lock()
	l.cache = nil
	l.batch = nil
	l.mu.Unlock()
}

func (l *dataLoader) key(schema TableSchema, id uint64) string {
	return schema.GetType().String() + ":" + strconv.FormatUint(id, 10)
}

func (l *dataLoader) loadThunk(key string) func() []byte {
	l.mu.Lock()
	if it, ok := l.cache[key]; ok {
		l.mu.Unlock()
		return func() []byte {
			return it
		}
	}
	if l.batch == nil {
		l.batch = &dataLoaderBatch{done: make(chan struct{})}
	}
	batch := l.batch
	pos := batch.keyIndex(l, key)
	l.mu.Unlock()

	return func() []byte {
		<-batch.done

		var data []byte
		if pos < len(batch.data) {
			data = batch.data[pos]
		}

		l.mu.Lock()
		l.unsafeSet(key, data)
		l.mu.Unlock()

		return data
	}
}

func (l *dataLoader) unsafeSet(key string, value []byte) {
	if l.cache == nil {
		l.cache = map[string][]byte{}
	}
	l.cache[key] = value
}

func (b *dataLoaderBatch) keyIndex(l *dataLoader, key string) int {
	for i, existingKey := range b.keys {
		if key == existingKey {
			return i
		}
	}

	pos := len(b.keys)
	b.keys = append(b.keys, key)
	if pos == 0 {
		go b.startTimer(l)
	}

	if pos >= l.maxBatchSize-1 {
		if !b.closing {
			b.closing = true
			l.batch = nil
			go b.end(l)
		}
	}

	return pos
}

func (b *dataLoaderBatch) startTimer(l *dataLoader) {
	time.Sleep(dataLoaderWait)
	l.mu.Lock()

	if b.closing {
		l.mu.Unlock()
		return
	}

	l.batch = nil
	l.mu.Unlock()

	b.end(l)
}

func (b *dataLoaderBatch) end(l *dataLoader) {
	m := make(map[string][]uint64)
	for _, key := range b.keys {
		parts := strings.Split(key, ":")
		if m[parts[0]] == nil {
			m[parts[0]] = make([]uint64, 0)
		}
		id, _ := strconv.ParseUint(parts[1], 10, 64)
		m[parts[0]] = append(m[parts[0]], id)
	}
	results := make(map[string][]byte)
	for entityName, ids := range m {
		lenIDs := len(ids)
		schema := l.engine.registry.GetTableSchema(entityName).(*tableSchema)
		var redisCacheKeys []string
		resultsKeys := make(map[string][]byte, lenIDs)
		keysMapping := make(map[string]uint64, lenIDs)
		redisCache, hasRedis := schema.GetRedisCache(l.engine)
		cacheKeys := make([]string, lenIDs)
		for index, id := range ids {
			cacheKey := schema.getCacheKey(id)
			keysMapping[cacheKey] = id
			cacheKeys[index] = cacheKey
		}
		if hasRedis {
			cacheKeys = b.getKeysForNils(l, schema, redisCache.MGet(cacheKeys...), keysMapping, resultsKeys, results)
			redisCacheKeys = cacheKeys
		}
		ids = make([]uint64, len(cacheKeys))
		for k, v := range cacheKeys {
			ids[k] = keysMapping[v]
		}
		lIds := len(ids)
		if lIds > 0 {
			for id, v := range b.search(schema, l.engine, ids) {
				resultsKeys[schema.getCacheKey(id)] = v
				results[l.key(schema, id)] = v
			}
		}
		if hasRedis {
			lIds = len(redisCacheKeys)
			if lIds > 0 {
				pairs := make([]interface{}, lIds*2)
				i := 0
				for _, key := range redisCacheKeys {
					pairs[i] = key
					val := resultsKeys[key]
					var toSet interface{}
					if val == nil {
						toSet = cacheNilValue
					} else {
						toSet = val
					}
					pairs[i+1] = toSet
					i += 2
				}
				redisCache.MSet(pairs...)
			}
		}
	}
	i := 0
	b.data = make([][]byte, len(b.keys))
	for _, key := range b.keys {
		b.data[i] = results[key]
		i++
	}
	close(b.done)
}

func (b *dataLoaderBatch) getKeysForNils(l *dataLoader, schema *tableSchema, rows map[string]interface{}, keyMapping map[string]uint64,
	resultsKeys map[string][]byte, results map[string][]byte) []string {
	keys := make([]string, 0)
	for k, v := range rows {
		if v == nil {
			keys = append(keys, k)
		} else {
			if v == cacheNilValue {
				resultsKeys[k] = nil
			} else {
				binary := []byte(v.(string))
				resultsKeys[k] = binary
				results[l.key(schema, keyMapping[k])] = binary
			}
		}
	}
	return keys
}

func (b *dataLoaderBatch) search(schema *tableSchema, engine *Engine, ids []uint64) map[uint64][]byte {
	where := NewWhere("`ID` IN ?", ids)
	result := make(map[uint64][]byte)
	/* #nosec */
	query := "SELECT " + schema.fieldsQuery + " FROM `" + schema.tableName + "` WHERE" + where.String()
	pool := schema.GetMysql(engine)
	results, def := pool.Query(query, where.GetParameters()...)
	defer def()

	i := 0
	for results.Next() {
		pointers := prepareScan(schema)
		results.Scan(pointers...)
		id := *pointers[schema.idIndex].(*uint64)
		serializer := engine.getSerializer()
		serializer.buffer.Reset()
		deserializeStructFromDB(engine, serializer, 0, schema.fields, pointers)
		result[id] = serializer.Serialize()
		i++
	}
	def()
	return result
}
