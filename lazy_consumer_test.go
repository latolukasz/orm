package orm

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

type lazyReceiverEntity struct {
	ORM          `orm:"localCache;redisCache;asyncRedisLazyFlush=default"`
	ID           uint
	Name         string `orm:"unique=name"`
	Age          uint64
	EnumNullable string `orm:"enum=orm.TestEnum"`
	RefOne       *lazyReceiverReference
	IndexAll     *CachedQuery `query:""`
}

type lazyReceiverReference struct {
	ORM
	ID   uint
	Name string
}

func TestLazyReceiver(t *testing.T) {
	var entity *lazyReceiverEntity
	var ref *lazyReceiverReference

	registry := &Registry{}
	registry.RegisterEnum("orm.TestEnum", "a", "b", "c")
	engine := PrepareTables(t, registry, 5, entity, ref)
	engine.GetRedis().FlushDB()

	receiver := NewAsyncConsumer(engine)
	receiver.DisableLoop()
	receiver.blockTime = time.Millisecond

	e := &lazyReceiverEntity{Name: "John", Age: 18}
	engine.FlushLazy(e)

	e = &lazyReceiverEntity{}
	loaded := engine.LoadByID(1, e)
	assert.False(t, loaded)

	validHeartBeat := false
	receiver.SetHeartBeat(time.Minute, func() {
		validHeartBeat = true
	})
	receiver.Digest(context.Background())
	assert.True(t, validHeartBeat)

	engine.GetLocalCache().Clear()
	loaded = engine.LoadByID(1, e)
	assert.True(t, loaded)
	assert.Equal(t, "John", e.Name)
	assert.Equal(t, uint64(18), e.Age)

	e.Name = "Tom"
	engine.FlushLazy(e)

	e = &lazyReceiverEntity{}
	loaded = engine.LoadByID(1, e)
	assert.True(t, loaded)
	assert.Equal(t, "Tom", e.Name)

	engine.GetLocalCache().Clear()
	e = &lazyReceiverEntity{}
	loaded = engine.LoadByID(1, e)
	assert.True(t, loaded)
	assert.Equal(t, "John", e.Name)

	validHeartBeat = false
	receiver.SetHeartBeat(time.Minute, func() {
		validHeartBeat = true
	})
	receiver.Digest(context.Background())
	assert.True(t, validHeartBeat)

	e = &lazyReceiverEntity{}
	loaded = engine.LoadByID(1, e)
	assert.True(t, loaded)
	assert.Equal(t, "John", e.Name)

	e = &lazyReceiverEntity{Name: "Tom"}
	e.SetOnDuplicateKeyUpdate(map[string]interface{}{"Age": 38})
	assert.PanicsWithError(t, "lazy flush on duplicate key is not supported", func() {
		engine.FlushLazy(e)
	})

	e = &lazyReceiverEntity{Name: "Adam", RefOne: &lazyReceiverReference{Name: "Test"}}
	assert.PanicsWithError(t, "lazy flush for unsaved references is not supported", func() {
		engine.FlushLazy(e)
	})

	e = &lazyReceiverEntity{}
	engine.LoadByID(1, e)
	engine.NewFlusher().Delete(e).FlushLazy()
	receiver.Digest(context.Background())
	loaded = engine.LoadByID(1, e)
	assert.False(t, loaded)
}
