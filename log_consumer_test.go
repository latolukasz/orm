package orm

import (
	"context"
	"database/sql"
	"testing"
	"time"

	apexLog "github.com/apex/log"
	"github.com/apex/log/handlers/memory"

	"github.com/stretchr/testify/assert"
)

type logReceiverEntity1 struct {
	ORM      `orm:"log;asyncRedisLogs=default;redisCache"`
	ID       uint
	Name     string
	LastName string
	Country  string `orm:"skip-log"`
}

type logReceiverEntity2 struct {
	ORM  `orm:"redisCache;log"`
	ID   uint
	Name string
	Age  uint64
}

func TestLogReceiver(t *testing.T) {
	var entity1 *logReceiverEntity1
	var entity2 *logReceiverEntity2
	registry := &Registry{}
	engine := PrepareTables(t, registry, 5, entity1, entity2)
	engine.GetMysql().Exec("TRUNCATE TABLE `_log_default_logReceiverEntity1`")
	engine.GetMysql().Exec("TRUNCATE TABLE `_log_default_logReceiverEntity2`")
	engine.GetRedis().FlushDB()

	consumer := NewAsyncConsumer(engine)
	consumer.DisableLoop()
	consumer.blockTime = time.Millisecond

	e1 := &logReceiverEntity1{Name: "John", LastName: "Smith", Country: "Poland"}
	engine.Flush(e1)
	e2 := &logReceiverEntity2{Name: "Tom", Age: 18}
	engine.Flush(e2)

	valid := false
	validHeartBeat := false
	consumer.SetLogLogger(func(log *LogQueueValue) {
		valid = true
	})
	consumer.SetHeartBeat(time.Minute, func() {
		validHeartBeat = true
	})
	consumer.Digest(context.Background())
	assert.True(t, valid)
	assert.True(t, validHeartBeat)

	var entityID int
	var meta sql.NullString
	var before sql.NullString
	var changes string
	where1 := NewWhere("SELECT `entity_id`, `meta`, `before`, `changes` FROM `_log_default_logReceiverEntity1` WHERE `ID` = 1")
	engine.GetMysql().QueryRow(where1, &entityID, &meta, &before, &changes)
	assert.Equal(t, 1, entityID)
	assert.False(t, meta.Valid)
	assert.False(t, before.Valid)
	assert.Equal(t, "{\"Name\": \"John\", \"Country\": \"Poland\", \"LastName\": \"Smith\"}", changes)

	where2 := NewWhere("SELECT `entity_id`, `meta`, `before`, `changes` FROM `_log_default_logReceiverEntity2` WHERE `ID` = 1")
	engine.GetMysql().QueryRow(where2, &entityID, &meta, &before, &changes)
	assert.Equal(t, 1, entityID)
	assert.False(t, meta.Valid)
	assert.False(t, before.Valid)
	assert.Equal(t, "{\"Age\": 18, \"Name\": \"Tom\"}", changes)

	engine.SetLogMetaData("user_id", 12)
	flusher := engine.NewFlusher()
	e1 = &logReceiverEntity1{Name: "John2"}
	flusher.Track(e1)
	e2 = &logReceiverEntity2{Name: "Tom2", Age: 18}
	e2.SetEntityLogMeta("admin_id", "10")
	flusher.Track(e2)
	flusher.Flush()

	consumer.Digest(context.Background())

	where1 = NewWhere("SELECT `entity_id`, `meta`, `before`, `changes` FROM `_log_default_logReceiverEntity1` WHERE `ID` = 2")
	engine.GetMysql().QueryRow(where1, &entityID, &meta, &before, &changes)
	assert.Equal(t, 2, entityID)
	assert.Equal(t, "{\"Name\": \"John2\", \"Country\": null, \"LastName\": null}", changes)
	assert.False(t, before.Valid)
	assert.Equal(t, "{\"user_id\": 12}", meta.String)

	where2 = NewWhere("SELECT `entity_id`, `meta`, `before`, `changes` FROM `_log_default_logReceiverEntity2` WHERE `ID` = 2")
	engine.GetMysql().QueryRow(where2, &entityID, &meta, &before, &changes)
	assert.Equal(t, 2, entityID)
	assert.Equal(t, "{\"Age\": 18, \"Name\": \"Tom2\"}", changes)
	assert.False(t, before.Valid)
	assert.Equal(t, "{\"user_id\": 12, \"admin_id\": \"10\"}", meta.String)

	e1.Country = "Germany"
	engine.Flush(e1)
	consumer.Digest(context.Background())
	where1 = NewWhere("SELECT `entity_id`, `meta`, `before`, `changes` FROM `_log_default_logReceiverEntity1` WHERE `ID` = 3")
	found := engine.GetMysql().QueryRow(where1, &entityID, &meta, &before, &changes)
	assert.False(t, found)

	e1.LastName = "Summer"
	engine.Flush(e1)
	consumer.Digest(context.Background())
	where1 = NewWhere("SELECT `entity_id`, `meta`, `before`, `changes` FROM `_log_default_logReceiverEntity1` WHERE `ID` = 3")
	found = engine.GetMysql().QueryRow(where1, &entityID, &meta, &before, &changes)
	assert.True(t, found)
	assert.Equal(t, 2, entityID)
	assert.Equal(t, "{\"LastName\": \"Summer\"}", changes)
	assert.Equal(t, "{\"Name\": \"John2\", \"Country\": \"Germany\", \"LastName\": null}", before.String)
	assert.Equal(t, "{\"user_id\": 12}", meta.String)

	engine.Delete(e1)
	consumer.Digest(context.Background())
	where1 = NewWhere("SELECT `entity_id`, `meta`, `before`, `changes` FROM `_log_default_logReceiverEntity1` WHERE `ID` = 4")
	var changesNullable sql.NullString
	found = engine.GetMysql().QueryRow(where1, &entityID, &meta, &before, &changesNullable)
	assert.True(t, found)
	assert.Equal(t, 2, entityID)
	assert.False(t, changesNullable.Valid)
	assert.Equal(t, "{\"Name\": \"John2\", \"Country\": \"Germany\", \"LastName\": \"Summer\"}", before.String)
	assert.Equal(t, "{\"user_id\": 12}", meta.String)

	e3 := &logReceiverEntity1{Name: "Adam", LastName: "Pol", Country: "Brazil"}
	engine.FlushLazy(e3)
	receiver := NewAsyncConsumer(engine)
	receiver.DisableLoop()
	receiver.blockTime = time.Millisecond
	receiver.Digest(context.Background())
	where1 = NewWhere("SELECT `entity_id`, `meta`, `before`, `changes` FROM `_log_default_logReceiverEntity1` WHERE `ID` = 5")
	found = engine.GetMysql().QueryRow(where1, &entityID, &meta, &before, &changes)
	assert.True(t, found)
	assert.Equal(t, 3, entityID)
	assert.False(t, before.Valid)
	assert.Equal(t, "{\"Name\": \"Adam\", \"Country\": \"Brazil\", \"LastName\": \"Pol\"}", changes)
	assert.Equal(t, "{\"user_id\": 12}", meta.String)

	engine.LoadByID(3, e3)
	e3.Name = "Eva"
	engine.FlushLazy(e3)
	receiver.Digest(context.Background())
	where1 = NewWhere("SELECT `entity_id`, `meta`, `before`, `changes` FROM `_log_default_logReceiverEntity1` WHERE `ID` = 6")
	found = engine.GetMysql().QueryRow(where1, &entityID, &meta, &before, &changes)
	assert.True(t, found)
	assert.Equal(t, 3, entityID)
	assert.True(t, before.Valid)
	assert.Equal(t, "{\"Name\": \"Eva\"}", changes)
	assert.Equal(t, "{\"Name\": \"Adam\", \"Country\": \"Brazil\", \"LastName\": \"Pol\"}", before.String)
	assert.Equal(t, "{\"user_id\": 12}", meta.String)

	engine.LoadByID(3, e3)
	flusher = engine.NewFlusher()
	flusher.Delete(e3)
	flusher.FlushLazy()
	receiver.Digest(context.Background())
	where1 = NewWhere("SELECT `entity_id`, `meta`, `before`, `changes` FROM `_log_default_logReceiverEntity1` WHERE `ID` = 7")
	var changesNull sql.NullString
	found = engine.GetMysql().QueryRow(where1, &entityID, &meta, &before, &changesNull)
	assert.True(t, found)
	assert.Equal(t, 3, entityID)
	assert.False(t, changesNull.Valid)
	assert.Equal(t, "{\"Name\": \"Eva\", \"Country\": \"Brazil\", \"LastName\": \"Pol\"}", before.String)
	assert.Equal(t, "{\"user_id\": 12}", meta.String)

	e4 := &logReceiverEntity2{}
	e5 := &logReceiverEntity2{}
	engine.LoadByID(1, e4)
	engine.LoadByID(2, e5)
	flusher = engine.NewFlusher()
	engine.GetMysql().Begin()
	e4.Age = 34
	flusher.Track(e4)
	_ = flusher.FlushWithCheck()
	_ = flusher.FlushWithCheck()
	e5.Name = "Lucas"
	flusher.Track(e5)
	_ = flusher.FlushWithCheck()
	logger := memory.New()
	engine.AddQueryLogger(logger, apexLog.InfoLevel)
	engine.GetMysql().Commit()
	assert.Len(t, logger.Entries, 2)
	assert.Equal(t, "[ORM][MYSQL][COMMIT]", logger.Entries[0].Message)
	assert.Equal(t, "[ORM][REDIS][EXEC]", logger.Entries[1].Message)
	commands := logger.Entries[1].Fields["commands"].([]string)
	assert.Len(t, commands, 7)
	assert.Equal(t, "DEL", commands[0])
	assert.Equal(t, "d06b9:1", commands[1])
	assert.Equal(t, "d06b9:2", commands[2])
	assert.Equal(t, "XAdd", commands[3])
	assert.Equal(t, "orm-log-channel", commands[4])
	assert.Equal(t, "XAdd", commands[5])
	assert.Equal(t, "orm-log-channel", commands[6])

	engine.GetMysql().Begin()
	flusher = engine.NewFlusher()
	flusher.Flush()
	e4.Age = 100
	flusher.Track(e4)
	flusher.Flush()
	flusher = engine.NewFlusher()
	flusher.Track(e4)
	flusher.Flush()
	logger.Entries = make([]*apexLog.Entry, 0)
	engine.GetMysql().Commit()
	assert.Len(t, logger.Entries, 2)
	assert.Equal(t, "[ORM][MYSQL][COMMIT]", logger.Entries[0].Message)
	assert.Equal(t, "[ORM][REDIS][EXEC]", logger.Entries[1].Message)
	commands = logger.Entries[1].Fields["commands"].([]string)
	assert.Len(t, commands, 4)
	assert.Equal(t, "DEL", commands[0])
	assert.Equal(t, "d06b9:1", commands[1])
	assert.Equal(t, "XAdd", commands[2])
	assert.Equal(t, "orm-log-channel", commands[3])
}
