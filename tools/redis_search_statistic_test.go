package tools

import (
	jsoniter "github.com/json-iterator/go"
	"testing"

	"github.com/latolukasz/orm"
	"github.com/stretchr/testify/assert"
)

func TestRedisSearchStatistics(t *testing.T) {
	registry := &orm.Registry{}
	registry.RegisterRedis("localhost:6382", 0)
	registry.RegisterRedisSearchIndex(&orm.RedisSearchIndex{Name: "test", RedisPool: "default"})
	validatedRegistry, err := registry.Validate()
	assert.NoError(t, err)
	engine := validatedRegistry.CreateEngine()
	engine.GetRedis().FlushDB()
	for _, alter := range engine.GetRedisSearchIndexAlters() {
		alter.Execute()
	}
	stats := GetRedisSearchStatistics(engine)
	assert.Len(t, stats, 1)
	assert.Equal(t, "test", stats[0].Index.Name)
	assert.Len(t, stats[0].Versions, 1)
	asJson, err := jsoniter.ConfigFastest.Marshal(stats)
	assert.NoError(t, err)
	assert.NotEmpty(t, asJson)
}
