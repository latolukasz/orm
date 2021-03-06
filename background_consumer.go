package orm

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/pkg/errors"

	jsoniter "github.com/json-iterator/go"
)

const lazyChannelName = "orm-lazy-channel"
const logChannelName = "orm-log-channel"
const redisSearchIndexerChannelName = "orm-redis-search-channel"
const asyncConsumerGroupName = "orm-async-consumer"

type LogQueueValue struct {
	PoolName  string
	TableName string
	ID        uint64
	LogID     uint64
	Meta      map[string]interface{}
	Before    map[string]interface{}
	Changes   map[string]interface{}
	Updated   time.Time
}

type dirtyQueueValue struct {
	Event   EventAsMap
	Streams []string
}

type BackgroundConsumer struct {
	eventConsumerBase
	engine       *Engine
	logLogger    func(log *LogQueueValue)
	redisFlusher RedisFlusher
}

func NewBackgroundConsumer(engine *Engine) *BackgroundConsumer {
	c := &BackgroundConsumer{engine: engine, redisFlusher: engine.NewRedisFlusher()}
	c.loop = true
	c.limit = 1
	c.blockTime = time.Second * 30
	return c
}

func (r *BackgroundConsumer) SetLogLogger(logger func(log *LogQueueValue)) {
	r.logLogger = logger
}

func (r *BackgroundConsumer) Digest(ctx context.Context) {
	consumer := r.engine.GetEventBroker().Consumer("default-consumer", asyncConsumerGroupName).(*eventsConsumer)
	consumer.eventConsumerBase = r.eventConsumerBase
	consumer.Consume(ctx, 100, true, func(events []Event) {
		for _, event := range events {
			switch event.Stream() {
			case lazyChannelName:
				r.handleLazy(event)
			case logChannelName:
				r.handleLogEvent(event)
			case redisSearchIndexerChannelName:
				r.handleRedisIndexerEvent(event)
			}
		}
	})
}

func (r *BackgroundConsumer) handleLogEvent(event Event) {
	var value LogQueueValue
	err := event.Unserialize(&value)
	if err != nil {
		event.Ack()
		return
	}
	r.handleLog(&value)
	event.Ack()
}

func (r *BackgroundConsumer) handleLog(value *LogQueueValue) {
	poolDB := r.engine.GetMysql(value.PoolName)
	/* #nosec */
	query := "INSERT INTO `" + value.TableName + "`(`entity_id`, `added_at`, `meta`, `before`, `changes`) VALUES(?, ?, ?, ?, ?)"
	var meta, before, changes interface{}
	if value.Meta != nil {
		meta, _ = jsoniter.ConfigFastest.Marshal(value.Meta)
	}
	if value.Before != nil {
		before, _ = jsoniter.ConfigFastest.Marshal(value.Before)
	}
	if value.Changes != nil {
		changes, _ = jsoniter.ConfigFastest.Marshal(value.Changes)
	}
	func() {
		if r.logLogger != nil {
			poolDB.Begin()
		}
		defer poolDB.Rollback()
		res := poolDB.Exec(query, value.ID, value.Updated.Format("2006-01-02 15:04:05"), meta, before, changes)
		if r.logLogger != nil {
			value.LogID = res.LastInsertId()
			r.logLogger(value)
			poolDB.Commit()
		}
	}()
}

func (r *BackgroundConsumer) handleLazy(event Event) {
	var data map[string]interface{}
	err := event.Unserialize(&data)
	if err != nil {
		event.Ack()
		return
	}
	ids := r.handleQueries(r.engine, data)
	r.handleCache(data, ids)
	event.Ack()
}

func (r *BackgroundConsumer) handleQueries(engine *Engine, validMap map[string]interface{}) []uint64 {
	queries := validMap["q"]
	if queries == nil {
		return nil
	}
	validQueries := queries.([]interface{})
	ids := make([]uint64, len(validQueries))
	for i, query := range validQueries {
		validInsert := query.([]interface{})
		code := validInsert[0].(string)
		db := engine.GetMysql(code)
		sql := validInsert[1].(string)
		attributes := validInsert[2]
		var res ExecResult
		if attributes == nil {
			res = db.Exec(sql)
		} else {
			res = db.Exec(sql, attributes.([]interface{})...)
		}
		if sql[0:11] == "INSERT INTO" {
			id := res.LastInsertId()
			ids[i] = res.LastInsertId()
			logEvents, has := validMap["l"]
			if has {
				for _, row := range logEvents.([]interface{}) {
					row.(map[string]interface{})["ID"] = id
					id += db.GetPoolConfig().getAutoincrement()
				}
			}
			dirtyEvents, has := validMap["d"]
			if has {
				for _, row := range dirtyEvents.([]interface{}) {
					row.(map[string]interface{})["Event"].(map[string]interface{})["I"] = id
					id += db.GetPoolConfig().getAutoincrement()
				}
			}
		} else {
			ids[i] = 0
		}
	}
	logEvents, has := validMap["l"]
	if has {
		for _, row := range logEvents.([]interface{}) {
			logEvent := &LogQueueValue{}
			asMap := row.(map[string]interface{})
			logEvent.ID, _ = strconv.ParseUint(fmt.Sprintf("%v", asMap["ID"]), 10, 64)
			logEvent.PoolName = asMap["PoolName"].(string)
			logEvent.TableName = asMap["TableName"].(string)
			logEvent.Updated = time.Now()
			if asMap["Meta"] != nil {
				logEvent.Meta = asMap["Meta"].(map[string]interface{})
			}
			if asMap["Before"] != nil {
				logEvent.Before = asMap["Before"].(map[string]interface{})
			}
			if asMap["Changes"] != nil {
				logEvent.Changes = asMap["Changes"].(map[string]interface{})
			}
			r.handleLog(logEvent)
		}
	}
	dirtyEvents, has := validMap["d"]
	if has {
		for _, row := range dirtyEvents.([]interface{}) {
			asMap := row.(map[string]interface{})
			event := asMap["Event"].(map[string]interface{})
			for _, stream := range asMap["Streams"].([]interface{}) {
				r.redisFlusher.PublishMap(stream.(string), event)
			}
		}
		r.redisFlusher.Flush()
	}
	return ids
}

func (r *BackgroundConsumer) handleCache(validMap map[string]interface{}, ids []uint64) {
	keys, has := validMap["cr"]
	if has {
		idKey := 0
		validKeys := keys.(map[string]interface{})
		for cacheCode, allKeys := range validKeys {
			validAllKeys := allKeys.([]interface{})
			stringKeys := make([]string, len(validAllKeys))
			for i, v := range validAllKeys {
				parts := strings.Split(v.(string), ":")
				l := len(parts)
				if l > 1 {
					if parts[l-1] == "0" {
						parts[l-1] = strconv.FormatUint(ids[idKey], 10)
					}
					idKey++
				}
				stringKeys[i] = strings.Join(parts, ":")
			}
			cache := r.engine.GetRedis(cacheCode)
			cache.Del(stringKeys...)
		}
	}
	localCache, has := validMap["cl"]
	if has {
		validKeys := localCache.(map[string]interface{})
		for cacheCode, allKeys := range validKeys {
			validAllKeys := allKeys.([]interface{})
			stringKeys := make([]string, len(validAllKeys))
			for i, v := range validAllKeys {
				stringKeys[i] = v.(string)
			}
			r.engine.GetLocalCache(cacheCode).Remove(stringKeys...)
		}
	}
}

func (r *BackgroundConsumer) handleRedisIndexerEvent(event Event) {
	indexEvent := &redisIndexerEvent{}
	err := event.Unserialize(indexEvent)
	if err != nil {
		event.Ack()
		return
	}
	var indexDefinition *RedisSearchIndex
	redisPool := ""
	for pool, list := range r.engine.registry.redisSearchIndexes {
		val, has := list[indexEvent.Index]
		if has {
			indexDefinition = val
			redisPool = pool
			break
		}
	}
	if indexDefinition == nil {
		event.Ack()
		return
	}
	search := r.engine.GetRedisSearch(redisPool)
	pusher := &redisSearchIndexPusher{pipeline: search.redis.PipeLine()}
	id := uint64(0)
	idRedisKey := redisSearchForceIndexLastIDKeyPrefix + indexEvent.Index + strconv.FormatUint(indexEvent.IndexID, 10)
	idInRedis, has := search.redis.Get(idRedisKey)
	if has {
		id, _ = strconv.ParseUint(idInRedis, 10, 64)
	}
	for {
		hasMore := false
		nextID := uint64(0)
		if indexDefinition.Indexer != nil {
			newID, hasNext := indexDefinition.Indexer(r.engine, id, pusher)
			hasMore = hasNext
			nextID = newID
			if pusher.pipeline.commands > 0 {
				pusher.Flush()
			}
			if hasMore {
				search.redis.Set(idRedisKey, strconv.FormatUint(nextID, 10), 86400)
			}
		}

		if !hasMore {
			search.redis.Del(idRedisKey)
			for _, oldName := range search.ListIndices() {
				if strings.HasPrefix(oldName, indexDefinition.Name+":") {
					parts := strings.Split(oldName, ":")
					oldID, _ := strconv.ParseUint(parts[1], 10, 64)
					if oldID < indexEvent.IndexID {
						search.dropIndex(oldName, false)
					}
				}
			}
			break
		}
		if nextID <= id {
			panic(errors.Errorf("loop detected in indxer for index %s in pool %s", indexDefinition.Name, redisPool))
		}
		id = nextID
	}
}
