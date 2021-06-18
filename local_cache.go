package orm

import (
	"sync"
	"time"

	log2 "github.com/apex/log"

	"github.com/golang/groupcache/lru"
)

const requestCacheKey = "_request"

type LocalCachePoolConfig interface {
	GetCode() string
	GetLimit() int
}

type localCachePoolConfig struct {
	code  string
	limit int
	m     sync.Mutex
}

func (p *localCachePoolConfig) GetCode() string {
	return p.code
}

func (p *localCachePoolConfig) GetLimit() int {
	return p.limit
}

type LocalCache struct {
	engine *Engine
	config *localCachePoolConfig
	lru    *lru.Cache
}

type ttlValue struct {
	value interface{}
	time  int64
}

func (c *LocalCache) GetPoolConfig() LocalCachePoolConfig {
	return c.config
}

func (c *LocalCache) GetSet(key string, ttlSeconds int, provider GetSetProvider) interface{} {
	val, has := c.Get(key)
	if has {
		ttlVal := val.(ttlValue)
		if time.Now().Unix()-ttlVal.time <= int64(ttlSeconds) {
			return ttlVal.value
		}
	}
	userVal := provider()
	val = ttlValue{value: userVal, time: time.Now().Unix()}
	c.Set(key, val)
	return userVal
}

func (c *LocalCache) Get(key string) (value interface{}, ok bool) {
	c.config.m.Lock()
	defer c.config.m.Unlock()

	value, ok = c.lru.Get(key)
	if c.engine.hasLocalCacheLogger {
		misses := 0
		if !ok {
			misses = 1
		}
		c.fillLogFields("[ORM][LOCAL][GET]", "get", misses, map[string]interface{}{"Key": key})
	}
	return
}

func (c *LocalCache) MGet(keys ...string) []interface{} {
	c.config.m.Lock()
	defer c.config.m.Unlock()

	results := make([]interface{}, len(keys))
	misses := 0
	for i, key := range keys {
		value, ok := c.lru.Get(key)
		if !ok {
			misses++
			value = nil
		}
		results[i] = value
	}
	if c.engine.hasLocalCacheLogger {
		c.fillLogFields("[ORM][LOCAL][MGET]", "mget", misses, map[string]interface{}{"Keys": keys})
	}
	return results
}

func (c *LocalCache) Set(key string, value interface{}) {
	c.config.m.Lock()
	defer c.config.m.Unlock()
	c.lru.Add(key, value)
	if c.engine.hasLocalCacheLogger {
		c.fillLogFields("[ORM][LOCAL][MGET]", "set", -1, map[string]interface{}{"Key": key, "value": value})
	}
}

func (c *LocalCache) MSet(pairs ...interface{}) {
	max := len(pairs)
	c.config.m.Lock()
	defer c.config.m.Unlock()
	for i := 0; i < max; i += 2 {
		c.lru.Add(pairs[i], pairs[i+1])
	}
	if c.engine.hasLocalCacheLogger {
		c.fillLogFields("[ORM][LOCAL][MSET]", "mset", -1, map[string]interface{}{"Keys": pairs})
	}
}

func (c *LocalCache) HMget(key string, fields ...string) map[string]interface{} {
	c.config.m.Lock()
	defer c.config.m.Unlock()

	l := len(fields)
	results := make(map[string]interface{}, l)
	value, ok := c.lru.Get(key)
	misses := 0
	for _, field := range fields {
		if !ok {
			results[field] = nil
			misses++
		} else {
			val, has := value.(map[string]interface{})[field]
			if !has {
				results[field] = nil
				misses++
			} else {
				results[field] = val
			}
		}
	}
	if c.engine.hasLocalCacheLogger {
		c.fillLogFields("[ORM][LOCAL][HMGET]", "hmget", misses, map[string]interface{}{"Key": key, "fields": fields})
	}
	return results
}

func (c *LocalCache) HMset(key string, fields map[string]interface{}) {
	c.config.m.Lock()
	defer c.config.m.Unlock()

	m, has := c.lru.Get(key)
	if !has {
		m = make(map[string]interface{})
		c.lru.Add(key, m)
	}
	for k, v := range fields {
		m.(map[string]interface{})[k] = v
	}
	if c.engine.hasLocalCacheLogger {
		c.fillLogFields("[ORM][LOCAL][HMSET]", "hmset", -1, map[string]interface{}{"Key": key, "fields": fields})
	}
}

func (c *LocalCache) Remove(keys ...string) {
	c.config.m.Lock()
	defer c.config.m.Unlock()
	for _, v := range keys {
		c.lru.Remove(v)
	}
	if c.engine.hasLocalCacheLogger {
		c.fillLogFields("[ORM][LOCAL][REMOVE]", "remove", -1, map[string]interface{}{"Keys": keys})
	}
}

func (c *LocalCache) GetObjectsCount() int {
	c.config.m.Lock()
	defer c.config.m.Unlock()

	return c.lru.Len()
}

func (c *LocalCache) Clear() {
	c.config.m.Lock()
	defer c.config.m.Unlock()
	c.lru.Clear()
	if c.engine.hasLocalCacheLogger {
		c.fillLogFields("[ORM][LOCAL][CLEAR]", "clear", -1, nil)
	}
}

func (c *LocalCache) fillLogFields(message string, operation string, misses int, fields log2.Fields) {
	e := c.engine.queryLoggers[QueryLoggerSourceLocalCache].log.WithFields(log2.Fields{
		"operation": operation,
		"pool":      c.config.GetCode(),
		"target":    "local_cache",
		"misses":    misses,
	})
	if fields != nil {
		e = e.WithFields(fields)
	}
	e.Info(message)
}
