package orm

import (
	"context"
	"fmt"
	"time"

	log2 "github.com/apex/log"

	"github.com/go-redis/redis/v8"
)

type RedisPipeLine struct {
	engine   *Engine
	pool     string
	pipeLine redis.Pipeliner
	ctx      context.Context
	executed bool
	commands int
	log      []string
}

func (rp *RedisPipeLine) Del(key ...string) *PipeLineInt {
	rp.commands++
	if rp.engine.hasRedisLogger {
		rp.log = append(rp.log, "DEL")
		rp.log = append(rp.log, key...)
	}
	return &PipeLineInt{p: rp, cmd: rp.pipeLine.Del(rp.ctx, key...)}
}

func (rp *RedisPipeLine) Get(key string) *PipeLineGet {
	rp.commands++
	if rp.engine.hasRedisLogger {
		rp.log = append(rp.log, "GET", key)
	}
	return &PipeLineGet{p: rp, cmd: rp.pipeLine.Get(rp.ctx, key)}
}

func (rp *RedisPipeLine) Set(key string, value interface{}, expiration time.Duration) *PipeLineStatus {
	rp.commands++
	if rp.engine.hasRedisLogger {
		rp.log = append(rp.log, "SET", key)
	}
	return &PipeLineStatus{p: rp, cmd: rp.pipeLine.Set(rp.ctx, key, value, expiration)}
}

func (rp *RedisPipeLine) Expire(key string, expiration time.Duration) *PipeLineBool {
	rp.commands++
	if rp.engine.hasRedisLogger {
		rp.log = append(rp.log, "EXPIRE", key)
	}
	return &PipeLineBool{p: rp, cmd: rp.pipeLine.Expire(rp.ctx, key, expiration)}
}

func (rp *RedisPipeLine) HIncrBy(key, field string, incr int64) *PipeLineInt {
	rp.commands++
	if rp.engine.hasRedisLogger {
		rp.log = append(rp.log, "HIncrBy", key)
	}
	return &PipeLineInt{p: rp, cmd: rp.pipeLine.HIncrBy(rp.ctx, key, field, incr)}
}

func (rp *RedisPipeLine) HSet(key string, values ...interface{}) *PipeLineInt {
	rp.commands++
	if rp.engine.hasRedisLogger {
		rp.log = append(rp.log, "HSet", key)
	}
	return &PipeLineInt{p: rp, cmd: rp.pipeLine.HSet(rp.ctx, key, values...)}
}

func (rp *RedisPipeLine) HDel(key string, values ...string) *PipeLineInt {
	rp.commands++
	if rp.engine.hasRedisLogger {
		rp.log = append(rp.log, "HDel", key)
	}
	return &PipeLineInt{p: rp, cmd: rp.pipeLine.HDel(rp.ctx, key, values...)}
}

func (rp *RedisPipeLine) XAdd(stream string, values interface{}) *PipeLineString {
	rp.commands++
	if rp.engine.hasRedisLogger {
		rp.log = append(rp.log, "XAdd", stream)
	}
	return &PipeLineString{p: rp, cmd: rp.pipeLine.XAdd(rp.ctx, &redis.XAddArgs{Stream: stream, Values: values})}
}

func (rp *RedisPipeLine) Exec() {
	if rp.executed {
		panic(fmt.Errorf("pipeline is already executed"))
	}
	start := time.Now()
	_, err := rp.pipeLine.Exec(rp.ctx)
	rp.executed = true
	if err != nil && err == redis.Nil {
		err = nil
	}
	if rp.engine.hasRedisLogger {
		rp.fillLogFields(start, err)
	}
	checkError(err)
}

func (rp *RedisPipeLine) Executed() bool {
	return rp.executed
}

type PipeLineGet struct {
	p   *RedisPipeLine
	cmd *redis.StringCmd
}

func (c *PipeLineGet) Result() (value string, has bool, err error) {
	checkExecuted(c.p)
	val, err := c.cmd.Result()
	if err == redis.Nil {
		return val, false, nil
	}
	return val, true, err
}

type PipeLineString struct {
	p   *RedisPipeLine
	cmd *redis.StringCmd
}

func (c *PipeLineString) Result() (string, error) {
	checkExecuted(c.p)
	return c.cmd.Result()
}

type PipeLineInt struct {
	p   *RedisPipeLine
	cmd *redis.IntCmd
}

func (c *PipeLineInt) Result() (int64, error) {
	checkExecuted(c.p)
	return c.cmd.Result()
}

type PipeLineBool struct {
	p   *RedisPipeLine
	cmd *redis.BoolCmd
}

func (c *PipeLineBool) Result() (bool, error) {
	checkExecuted(c.p)
	return c.cmd.Result()
}

type PipeLineStatus struct {
	p   *RedisPipeLine
	cmd *redis.StatusCmd
}

func (c *PipeLineStatus) Result() error {
	checkExecuted(c.p)
	_, err := c.cmd.Result()
	return err
}

func (rp *RedisPipeLine) fillLogFields(start time.Time, err error) {
	if rp.engine.hasRedisLogger {
		message := "[ORM][REDIS][EXEC]"
		now := time.Now()
		stop := time.Since(start).Microseconds()
		e := rp.engine.queryLoggers[QueryLoggerSourceRedis].log.WithFields(log2.Fields{
			"microseconds": stop,
			"operation":    "exec",
			"commands":     rp.log,
			"pool":         rp.pool,
			"target":       "redis",
			"started":      start.UnixNano(),
			"finished":     now.UnixNano(),
		})
		if err != nil {
			injectLogError(err, e).Error(message)
		} else {
			e.Info(message)
		}
	}
}

func checkExecuted(p *RedisPipeLine) {
	if !p.Executed() {
		panic(fmt.Errorf("pipeline must be executed first"))
	}
}
