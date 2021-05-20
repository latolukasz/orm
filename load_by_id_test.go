package orm

import (
	"testing"

	jsoniter "github.com/json-iterator/go"
	"github.com/shamaton/msgpack"

	apexLog "github.com/apex/log"
	"github.com/apex/log/handlers/memory"
	"github.com/stretchr/testify/assert"
)

type loadByIDEntity struct {
	ORM             `orm:"localCache;redisCache"`
	ID              uint
	Name            string `orm:"max=100"`
	ReferenceOne    *loadByIDReference
	ReferenceSecond *loadByIDReference
	ReferenceMany   []*loadByIDReference
}

type loadByIDRedisEntity struct {
	ORM `orm:"redisCache"`
	ID  uint
}

type loadByIDNoCacheEntity struct {
	ORM
	ID   uint
	Name string
}

type loadByIDReference struct {
	ORM            `orm:"localCache;redisCache"`
	ID             uint
	Name           string
	ReferenceTwo   *loadByIDSubReference
	ReferenceThree *loadByIDSubReference2
}

type loadByIDSubReference struct {
	ORM  `orm:"localCache;redisCache"`
	ID   uint
	Name string
}

type loadByIDSubReference2 struct {
	ORM          `orm:"localCache"`
	ID           uint
	Name         string
	ReferenceTwo *loadByIDSubReference
}

type loadByIDBenchmarkEntity struct {
	ORM
	ID      uint
	Name    string
	Int     int
	Bool    bool
	Float   float64
	Decimal float32 `orm:"decimal=10,2"`
}

func TestLoadById(t *testing.T) {
	var entity *loadByIDEntity
	var entityRedis *loadByIDRedisEntity
	var entityNoCache *loadByIDNoCacheEntity
	var reference *loadByIDReference
	var subReference *loadByIDSubReference
	var subReference2 *loadByIDSubReference2
	engine := PrepareTables(t, &Registry{}, 5, entity, entityRedis, entityNoCache, reference, subReference, subReference2)

	//s := engine.getSerializer()
	luna := &loadByIDEntity{Name: "lunka"}
	engine.Flush(luna)
	//initIfNeeded(engine.registry, luna)
	//luna.serialize(s)
	//
	//fmt.Printf("%v\n", luna.getORM().tableSchema.columnMapping)
	//fmt.Printf("%v\n", string(luna.getORM().binary))
	//s.Reset(luna.getORM().binary)
	//fmt.Printf("a: %v\n", s.GetUInt32())
	//fmt.Printf("b: %v\n", s.GetUInt32())
	//fmt.Printf("c: %v\n", s.GetUInt32())
	//fmt.Printf("d: %v\n", s.GetString())
	//
	return

	engine.EnableQueryDebug()
	e := &loadByIDEntity{Name: "a", ReferenceOne: &loadByIDReference{Name: "r1", ReferenceTwo: &loadByIDSubReference{Name: "s1"}}}
	e.ReferenceSecond = &loadByIDReference{Name: "r11", ReferenceTwo: &loadByIDSubReference{Name: "s1"},
		ReferenceThree: &loadByIDSubReference2{Name: "s11", ReferenceTwo: &loadByIDSubReference{Name: "hello"}}}
	engine.FlushMany(e,
		&loadByIDEntity{Name: "b", ReferenceOne: &loadByIDReference{Name: "r2", ReferenceTwo: &loadByIDSubReference{Name: "s2"}}},
		&loadByIDEntity{Name: "c"}, &loadByIDNoCacheEntity{Name: "a"})
	return

	engine.FlushMany(&loadByIDReference{Name: "rm1", ID: 100}, &loadByIDReference{Name: "rm2", ID: 101}, &loadByIDReference{Name: "rm3", ID: 102})
	engine.FlushMany(&loadByIDEntity{Name: "eMany", ID: 200, ReferenceMany: []*loadByIDReference{{ID: 100}, {ID: 101}, {ID: 102}}})

	entity = &loadByIDEntity{}
	localLogger := memory.New()
	engine.AddQueryLogger(localLogger, apexLog.InfoLevel, QueryLoggerSourceLocalCache)
	found := engine.LoadByID(1, entity, "ReferenceOne/ReferenceTwo",
		"ReferenceSecond/ReferenceTwo", "ReferenceSecond/ReferenceThree/ReferenceTwo")
	assert.True(t, found)
	assert.Len(t, localLogger.Entries, 5)
	assert.True(t, entity.IsLoaded())
	assert.False(t, entity.IsLazy())
	assert.True(t, entity.ReferenceOne.IsLoaded())
	assert.False(t, entity.ReferenceOne.IsLazy())
	assert.True(t, entity.ReferenceOne.ReferenceTwo.IsLoaded())
	assert.False(t, entity.ReferenceOne.ReferenceTwo.IsLazy())
	assert.True(t, entity.ReferenceSecond.IsLoaded())
	assert.False(t, entity.ReferenceSecond.IsLazy())
	assert.True(t, entity.ReferenceSecond.ReferenceTwo.IsLoaded())
	assert.False(t, entity.ReferenceSecond.ReferenceTwo.IsLazy())
	assert.True(t, entity.ReferenceSecond.ReferenceThree.IsLoaded())
	assert.False(t, entity.ReferenceSecond.ReferenceThree.IsLazy())
	assert.True(t, entity.ReferenceSecond.ReferenceThree.ReferenceTwo.IsLoaded())
	assert.False(t, entity.ReferenceSecond.ReferenceThree.ReferenceTwo.IsLazy())

	entity = &loadByIDEntity{}
	found = engine.LoadByIDLazy(1, entity, "ReferenceOne/ReferenceTwo",
		"ReferenceSecond/ReferenceTwo", "ReferenceSecond/ReferenceThree/ReferenceTwo")
	assert.True(t, found)
	assert.True(t, entity.IsLoaded())
	assert.True(t, entity.IsLazy())
	assert.Equal(t, "", entity.Name)
	assert.Equal(t, "a", entity.GetFieldLazy(engine, "Name"))
	entity.Fill(engine)
	assert.Equal(t, "a", entity.Name)
	assert.IsType(t, reference, entity.ReferenceOne)
	assert.True(t, entity.ReferenceOne.IsLoaded())
	assert.True(t, entity.ReferenceOne.IsLazy())
	assert.Equal(t, "r1", entity.ReferenceOne.GetFieldLazy(engine, "Name"))
	assert.True(t, entity.ReferenceOne.ReferenceTwo.IsLoaded())
	assert.True(t, entity.ReferenceSecond.IsLoaded())
	assert.True(t, entity.ReferenceSecond.IsLoaded())
	assert.Equal(t, "r11", entity.ReferenceSecond.GetFieldLazy(engine, "Name"))
	assert.True(t, entity.ReferenceSecond.ReferenceTwo.IsLoaded())
	assert.True(t, entity.ReferenceSecond.ReferenceTwo.IsLazy())
	assert.Equal(t, "s1", entity.ReferenceSecond.ReferenceTwo.GetFieldLazy(engine, "Name"))
	assert.True(t, entity.ReferenceSecond.ReferenceThree.IsLoaded())
	assert.True(t, entity.ReferenceSecond.ReferenceThree.IsLazy())
	assert.Equal(t, "s11", entity.ReferenceSecond.ReferenceThree.GetFieldLazy(engine, "Name"))
	assert.True(t, entity.ReferenceSecond.ReferenceThree.ReferenceTwo.IsLoaded())
	assert.True(t, entity.ReferenceSecond.ReferenceThree.ReferenceTwo.IsLazy())
	assert.Equal(t, "hello", entity.ReferenceSecond.ReferenceThree.ReferenceTwo.GetFieldLazy(engine, "Name"))

	entity = &loadByIDEntity{}
	found = engine.LoadByID(1, entity, "ReferenceOne/ReferenceTwo")
	assert.True(t, found)
	assert.Equal(t, uint(1), entity.ID)
	assert.Equal(t, "a", entity.Name)
	assert.Equal(t, "r1", entity.ReferenceOne.Name)
	assert.True(t, entity.ReferenceOne.IsLoaded())
	assert.Equal(t, "s1", entity.ReferenceOne.ReferenceTwo.Name)
	assert.True(t, entity.ReferenceOne.ReferenceTwo.IsLoaded())

	entity = &loadByIDEntity{ID: 1}
	engine.Load(entity, "ReferenceOne/ReferenceTwo")
	assert.Equal(t, "a", entity.Name)
	assert.False(t, entity.IsLazy())
	assert.Equal(t, "r1", entity.ReferenceOne.Name)
	assert.False(t, entity.ReferenceOne.IsLazy())
	assert.True(t, entity.ReferenceOne.IsLoaded())
	assert.Equal(t, "s1", entity.ReferenceOne.ReferenceTwo.Name)
	assert.True(t, entity.ReferenceOne.ReferenceTwo.IsLoaded())

	entity = &loadByIDEntity{ID: 1}
	engine.LoadLazy(entity, "ReferenceOne/ReferenceTwo")
	assert.Equal(t, "", entity.Name)
	assert.Equal(t, "a", entity.GetFieldLazy(engine, "Name"))
	assert.True(t, entity.IsLazy())
	assert.Equal(t, "r1", entity.ReferenceOne.GetFieldLazy(engine, "Name"))
	assert.True(t, entity.ReferenceOne.IsLazy())
	assert.True(t, entity.ReferenceOne.IsLoaded())
	assert.Equal(t, "s1", entity.ReferenceOne.ReferenceTwo.GetFieldLazy(engine, "Name"))
	assert.True(t, entity.ReferenceOne.ReferenceTwo.IsLoaded())
	assert.True(t, entity.ReferenceOne.ReferenceTwo.IsLazy())

	entityNoCache = &loadByIDNoCacheEntity{}
	found = engine.LoadByID(1, entityNoCache, "*")
	assert.True(t, found)
	assert.Equal(t, uint(1), entityNoCache.ID)
	assert.Equal(t, "a", entityNoCache.Name)

	found = engine.LoadByID(100, entity, "*")
	assert.False(t, found)
	found = engine.LoadByID(100, entity, "*")
	assert.False(t, found)
	entityRedis = &loadByIDRedisEntity{}
	found = engine.LoadByID(100, entityRedis, "*")
	assert.False(t, found)
	found = engine.LoadByID(100, entityRedis, "*")
	assert.False(t, found)

	entity = &loadByIDEntity{}
	found = engine.LoadByID(200, entity, "ReferenceMany")
	assert.True(t, found)
	assert.Len(t, entity.ReferenceMany, 3)
	assert.Equal(t, uint(100), entity.ReferenceMany[0].ID)
	assert.Equal(t, uint(101), entity.ReferenceMany[1].ID)
	assert.Equal(t, uint(102), entity.ReferenceMany[2].ID)
	assert.Equal(t, "rm1", entity.ReferenceMany[0].Name)
	assert.Equal(t, "rm2", entity.ReferenceMany[1].Name)
	assert.Equal(t, "rm3", entity.ReferenceMany[2].Name)
	assert.True(t, entity.ReferenceMany[0].IsLoaded())
	assert.True(t, entity.ReferenceMany[1].IsLoaded())
	assert.True(t, entity.ReferenceMany[2].IsLoaded())

	entity = &loadByIDEntity{}
	found = engine.LoadByIDLazy(200, entity, "ReferenceMany")
	assert.True(t, found)
	assert.Len(t, entity.ReferenceMany, 3)
	assert.Equal(t, uint(100), entity.ReferenceMany[0].ID)
	assert.Equal(t, uint(101), entity.ReferenceMany[1].ID)
	assert.Equal(t, uint(102), entity.ReferenceMany[2].ID)
	assert.Equal(t, "", entity.ReferenceMany[0].Name)
	assert.Equal(t, "", entity.ReferenceMany[1].Name)
	assert.Equal(t, "", entity.ReferenceMany[2].Name)
	assert.True(t, entity.ReferenceMany[0].IsLazy())
	assert.True(t, entity.ReferenceMany[1].IsLazy())
	assert.True(t, entity.ReferenceMany[2].IsLazy())
	assert.True(t, entity.ReferenceMany[0].IsLoaded())
	assert.True(t, entity.ReferenceMany[1].IsLoaded())
	assert.True(t, entity.ReferenceMany[2].IsLoaded())
	assert.Equal(t, "rm1", entity.ReferenceMany[0].GetFieldLazy(engine, "Name"))
	assert.Equal(t, "rm2", entity.ReferenceMany[1].GetFieldLazy(engine, "Name"))
	assert.Equal(t, "rm3", entity.ReferenceMany[2].GetFieldLazy(engine, "Name"))
	entity.ReferenceMany[0].Fill(engine)
	assert.Equal(t, "rm1", entity.ReferenceMany[0].Name)
	entity.ReferenceMany[0].Fill(engine)
	assert.False(t, entity.ReferenceMany[0].IsLazy())

	engine = PrepareTables(t, &Registry{}, 5)
	entity = &loadByIDEntity{}
	assert.PanicsWithError(t, "entity 'orm.loadByIDEntity' is not registered", func() {
		engine.LoadByID(1, entity)
	})
}

// BenchmarkLoadByIDdLocalCache-12    	 5869000	       203.3 ns/op	       8 B/op	       1 allocs/op
func BenchmarkLoadByIDdLocalCache(b *testing.B) {
	benchmarkLoadByIDLocalCache(b, false, true, false)
}

// BenchmarkLoadByIDLocalCacheLazy-12    	 9291440	       132.9 ns/op	       8 B/op	       1 allocs/op
func BenchmarkLoadByIDLocalCacheLazy(b *testing.B) {
	benchmarkLoadByIDLocalCache(b, true, true, false)
}

func BenchmarkLoadByIDRedisCacheLazy(b *testing.B) {
	benchmarkLoadByIDLocalCache(b, true, false, true)
}

var serializeData = []interface{}{17458, 179388, uint8(2), uint16(17), 982, "Name 2", "Name", false, true, float32(1.338832), 12324.23}

// LEN: 68 BenchmarkSerializeJSON-12    	 1156762	       930.9 ns/op	     496 B/op	      15 allocs/op
func BenchmarkSerializeJSON(b *testing.B) {
	val := serializeData
	encoded, _ := jsoniter.ConfigFastest.Marshal(val)
	asString := string(encoded)
	b.ResetTimer()
	b.ReportAllocs()
	for n := 0; n < b.N; n++ {
		decoded := make([]interface{}, 6)
		_ = jsoniter.ConfigFastest.Unmarshal([]byte(asString), &decoded)
	}
}

//LEN 42 BenchmarkSerializeMessagePack2-12    	 1046724	      1133 ns/op	     496 B/op	      13 allocs/op
func BenchmarkSerializeMessagePack(b *testing.B) {
	val := serializeData
	encoded, _ := msgpack.Marshal(val)
	asString := string(encoded)
	b.ResetTimer()
	b.ReportAllocs()
	for n := 0; n < b.N; n++ {
		decoded := make([]interface{}, 6)
		_ = msgpack.Unmarshal([]byte(asString), &decoded)
	}
}

func benchmarkLoadByIDLocalCache(b *testing.B, lazy, local, redis bool) {
	entity := &loadByIDBenchmarkEntity{}
	registry := &Registry{}
	registry.RegisterEnumStruct("orm.TestEnum", TestEnum)
	registry.RegisterLocalCache(10000)
	engine := PrepareTables(nil, registry, 5, entity)
	schema := engine.GetRegistry().GetTableSchemaForEntity(entity).(*tableSchema)
	if local {
		schema.localCacheName = "default"
		schema.hasLocalCache = true
	} else {
		schema.localCacheName = ""
		schema.hasLocalCache = false
	}
	if redis {
		schema.redisCacheName = "default"
		schema.hasRedisCache = true
	} else {
		schema.redisCacheName = ""
		schema.hasRedisCache = false
	}

	entity.Name = "Name"
	entity.Int = 1
	entity.Float = 1.3
	entity.Decimal = 12.23
	engine.Flush(entity)
	_ = engine.LoadByID(1, entity)
	b.ResetTimer()
	b.ReportAllocs()
	for n := 0; n < b.N; n++ {
		if lazy {
			_ = engine.LoadByIDLazy(1, entity)
		} else {
			_ = engine.LoadByID(1, entity)
		}
	}
}
