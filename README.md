# orm

![Check & test](https://github.com/latolukasz/orm/workflows/Check%20&%20test/badge.svg)
[![codecov](https://codecov.io/gh/latolukasz/orm/branch/main/graph/badge.svg?token=90GL4IJRWI)](https://codecov.io/gh/latolukasz/orm)
[![Go Report Card](https://goreportcard.com/badge/github.com/latolukasz/orm)](https://goreportcard.com/report/github.com/latolukasz/orm)
[![MIT license](https://img.shields.io/badge/license-MIT-brightgreen.svg)](https://opensource.org/licenses/MIT)


ORM that delivers support for full stack data access:

 * MySQL - for relational data
 * Redis - for NoSQL in memory shared cache
 * Elastic Search - for full text search
 * Redis Search - for full text and in-memory search
 * Local Cache - in memory local (not shared) cache
 * ClickHouse - time series database
 
Menu:

 * [Configuration](https://github.com/latolukasz/orm#configuration) 
 * [Defining entities](https://github.com/latolukasz/orm#defining-entities) 
 * [Creating registry](https://github.com/latolukasz/orm#validated-registry) 
 * [Creating engine](https://github.com/latolukasz/orm#creating-engine) 
 * [Checking and updating table schema](https://github.com/latolukasz/orm#checking-and-updating-table-schema) 
 * [Adding, editing, deleting entities](https://github.com/latolukasz/orm#adding-editing-deleting-entities) 
 * [Transactions](https://github.com/latolukasz/orm#transactions) 
 * [Loading entities using primary key](https://github.com/latolukasz/orm#loading-entities-using-primary-key) 
 * [Loading entities using search](https://github.com/latolukasz/orm#loading-entities-using-search) 
 * [Lazy loading](https://github.com/latolukasz/orm#lazy-loading)
 * [Reference one to one](https://github.com/latolukasz/orm#reference-one-to-one) 
 * [Cached queries](https://github.com/latolukasz/orm#cached-queries) 
 * [Lazy flush](https://github.com/latolukasz/orm#lazy-flush)
 * [Data loader](https://github.com/latolukasz/orm#data-loader) 
 * [Log entity changes](https://github.com/latolukasz/orm#log-entity-changes) 
 * [Dirty stream](https://github.com/latolukasz/orm#dirty-stream) 
 * [Fake delete](https://github.com/latolukasz/orm#fake-delete) 
 * [Working with Redis](https://github.com/latolukasz/orm#working-with-redis) 
 * [Working with local cache](https://github.com/latolukasz/orm#working-with-local-cache) 
 * [Working with mysql](https://github.com/latolukasz/orm#working-with-mysql) 
 * [Working with elastic search](https://github.com/latolukasz/orm#working-with-elastic-search)  
 * [Working with ClickHouse](https://github.com/latolukasz/orm#working-with-clickhouse)  
 * [Working with Locker](https://github.com/latolukasz/orm#working-with-locker) 
 * [Query logging](https://github.com/latolukasz/orm#query-logging) 
 * [Logger](https://github.com/latolukasz/orm#logger) 
 * [Event broker](https://github.com/latolukasz/orm#event-broker)
 * [Redis search](https://github.com/latolukasz/orm#redis-search)
 * [Tools](https://github.com/latolukasz/orm#tools)
 * [Health check](https://github.com/latolukasz/orm#health-check)

## Configuration

First you need to define Registry object and register all connection pools to MySQL, Redis and local cache.
Use this object to register queues, and entities. You should create this object once when application
starts.

```go
package main

import "github.com/latolukasz/orm"

func main() {

    registry := &Registry{}

    /*MySQL */
    registry.RegisterMySQLPool("root:root@tcp(localhost:3306)/database_name?limit_connections=10") // you should define max connections, default 100
    //optionally you can define pool name as second argument
    registry.RegisterMySQLPool("root:root@tcp(localhost:3307)/database_name", "second_pool")
    registry.DefaultEncoding("utf8") //optional, default is utf8mb4

    /* Redis */
    registry.RegisterRedis("localhost:6379", 0)
    //optionally you can define pool name as second argument
    registry.RegisterRedis("localhost:6379", 1, "second_pool")

    /* Redis sentinel */
    registry.RegisterRedisSentinel("mymaster", 0, []string{":26379", "192.23.12.33:26379", "192.23.12.35:26379"})
    // redis database number set to 2
    registry.RegisterRedisSentinel("mymaster", 2, []string{":26379", "192.23.12.11:26379", "192.23.12.12:26379"}, "second_pool") 

    /* Local cache (in memory) */
    registry.RegisterLocalCache(1000) //you need to define cache size
    //optionally you can define pool name as second argument
    registry.RegisterLocalCache(100, "second_pool")

    /* Redis used to handle locks (explained later) */
    registry.RegisterRedis("localhost:6379", 4, "lockers_pool")

    /* ElasticSearch */
    registry.RegisterElastic("http://127.0.0.1:9200")
    //optionally you can define pool name as second argument
    registry.RegisterElastic("http://127.0.0.1:9200", "second_pool")
    // you can enable trace log
    registry.RegisterElasticWithTraceLog("http://127.0.0.1:9200", "second_pool")

    /* ClickHouse */
    registry.RegisterClickHouse("http://127.0.0.1:9000")
    //optionally you can define pool name as second argument
    registry.RegisterClickHouse("http://127.0.0.1:9000", "second_pool")
}

```

##### You can also create registry using yaml configuration file:

```.yaml
default:
    mysql: root:root@tcp(localhost:3310)/db
    mysqlEncoding: utf8 //optional, default is utf8mb4
    redis: localhost:6379:0
    streams:
      stream-1:
        - test-group-1
        - test-group-2
    elastic: http://127.0.0.1:9200
    elastic_trace: http://127.0.0.1:9201 //with trace log
    clickhouse: http://127.0.0.1:9000
    local_cache: 1000
second_pool:
    mysql: root:root@tcp(localhost:3311)/db2
      sentinel:
        master:1:
          - :26379
          - 192.156.23.11:26379
          - 192.156.23.12:26379
```

```go
package main

import (
    "github.com/latolukasz/orm"
    "gopkg.in/yaml.v2"
    "io/ioutil"
)

func main() {

    yamlFileData, err := ioutil.ReadFile("./yaml")
    if err != nil {
        //...
    }
    
    var parsedYaml map[string]interface{}
    err = yaml.Unmarshal(yamlFileData, &parsedYaml)
    registry := InitByYaml(parsedYaml)
}

```

## Defining entities

```go
package main

import (
	"github.com/latolukasz/orm"
	"time"
)

func main() {

    type AddressSchema struct {
        Street   string
        Building uint16
    }
    
    type colors struct {
        Red    string
        Green  string
        Blue   string
        Yellow string
        Purple string
    }
    var Colors = &colors{
        orm.EnumModel,
    	Red:    "Red",
    	Green:  "Green",
    	Blue:   "Blue",
    	Yellow: "Yellow",
    	Purple: "Purple",
    }

    type testEntitySchema struct {
        orm.ORM
        ID                   uint
        Name                 string `orm:"length=100;index=FirstIndex"`
        NameNullable         string `orm:"length=100;index=FirstIndex"`
        BigName              string `orm:"length=max;required"`
        Uint8                uint8  `orm:"unique=SecondIndex:2,ThirdIndex"`
        Uint24               uint32 `orm:"mediumint=true"`
        Uint32               uint32
        Uint32Nullable       *uint32
        Uint64               uint64 `orm:"unique=SecondIndex"`
        Int8                 int8
        Int16                int16
        Int32                int32
        Int64                int64
        Rune                 rune
        Int                  int
        IntNullable          *int
        Bool                 bool
        BoolNullable         *bool
        Float32              float32
        Float64              float64
        Float64Nullable      *float64
        Float32Decimal       float32  `orm:"decimal=8,2"`
        Float64DecimalSigned float64  `orm:"decimal=8,2;unsigned=false"`
        Enum                 string   `orm:"enum=orm.colorEnum"`
        EnumNotNull          string   `orm:"enum=orm.colorEnum;required"`
        Set                  []string `orm:"set=orm.colorEnum"`
        YearNullable         *uint16   `orm:"year=true"`
        YearNotNull          uint16   `orm:"year=true"`
        Date                 *time.Time
        DateNotNull          time.Time
        DateTime             *time.Time `orm:"time=true"`
        DateTimeNotNull      time.Time  `orm:"time=true"`
        Address              AddressSchema
        Json                 interface{}
        ReferenceOne         *testEntitySchemaRef
        ReferenceOneCascade  *testEntitySchemaRef `orm:"cascade"`
        ReferenceMany        []*testEntitySchemaRef
        IgnoreField          []time.Time       `orm:"ignore"`
        Blob                 []byte
        MediumBlob           []byte `orm:"mediumblob=true"`
        LongBlob             []byte `orm:"longblob=true"`
        FieldAsJson          map[string]string
    }
    
    type testEntitySchemaRef struct {
        orm.ORM
        ID   uint
        Name string
    }
    type testEntitySecondPool struct {
    	orm.ORM `orm:"mysql=second_pool"`
    	ID                   uint
    }

    registry := &Registry{}
    var testEntitySchema testEntitySchema
    var testEntitySchemaRef testEntitySchemaRef
    var testEntitySecondPool testEntitySecondPool
    registry.RegisterEntity(testEntitySchema, testEntitySchemaRef, testEntitySecondPool)
    registry.RegisterEnumStruct("color", Colors)

    // now u can use:
    Colors.GetDefault() // "Red" (first field)
    Colors.GetFields() // ["Red", "Blue" ...]
    Colors.GetMapping() // map[string]string{"Red": "Red", "Blue": "Blue"}
    Colors.Has("Red") //true
    Colors.Has("Orange") //false
    
    //or register enum from slice
    registry.RegisterEnumSlice("color", []string{"Red", "Blue"})
    validatedRegistry.GetEnum("color").GetFields()
    validatedRegistry.GetEnum("color").Has("Red")
    
    //or register enum from map
    registry.RegisterEnumMap("color", map[string]string{"red": "Red", "blue": "Blue"}, "red")
}
```

There are only two golden rules you need to remember defining entity struct: 

 * first field must be type of "ORM"
 * second argument must have name "ID" and must be type of one of uint, uint16, uint32, uint24, uint64, rune
 
 
 By default entity is not cached in local cache or redis, to change that simply use key "redisCache" or "localCache"
 in "orm" tag for "ORM" field:
 
 ```go
 package main
 
 import (
 	"github.com/latolukasz/orm"
 	"time"
 )
 
 func main() {
 
     type testEntityLocalCache struct {
     	orm.ORM `orm:"localCache"` //default pool
        //...
     }
    
    type testEntityLocalCacheSecondPool struct {
     	orm.ORM `orm:"localCache=second_pool"`
        //...
     }
    
    type testEntityRedisCache struct {
     	orm.ORM `orm:"redisCache"` //default pool
        //...
     }
    
    type testEntityRedisCacheSecondPool struct {
     	orm.ORM `orm:"redisCache=second_pool"`
        //...
     }

    type testEntityLocalAndRedisCache struct {
     	orm.ORM `orm:"localCache;redisCache"`
        //...
     }
 }
 ```

## Validated registry

Once you created your registry and registered all pools and entities you should validate it.
You should also run it once when your application starts.

 ```go
 package main
 
 import "github.com/latolukasz/orm"
 
 func main() {
    registry := &Registry{}
    //register pools and entities
    validatedRegistry, err := registry.Validate()
 }
 
 ```
 
 ## Creating engine
 
 You need to crete engine to start working with entities (searching, saving).
 You must create engine for each http request and thread.
 
  ```go
  package main
  
  import "github.com/latolukasz/orm"
  
  func main() {
     registry := &Registry{}
     //register pools and entities
     validatedRegistry, err := registry.Validate()
     engine := validatedRegistry.CreateEngine()
  }
  
  ```
 
 ## Checking and updating table schema
 
 ORM provides useful object that describes entity structrure called TabelSchema:
 
 ```go
 package main
 
 import "github.com/latolukasz/orm"
 
 func main() {
    
    registry := &Registry{}
    // register
    validatedRegistry, err := registry.Validate() 
    engine := validatatedRegistry.CreateEngine()
    alters := engine.GetAlters()
    
    /*optionally you can execute alters for each model*/
    var userEntity UserEntity
    tableSchema := engine.GetRegistry().GetTableSchemaForEntity(userEntity)
    //or
    tableSchema := validatedRegistry.GetTableSchemaForEntity(userEntity)

    /*checking table structure*/
    tableSchema.UpdateSchema(engine) //it will create or alter table if needed
    tableSchema.DropTable(engine) //it will drop table if exist
    tableSchema.TruncateTable(engine)
    tableSchema.UpdateSchemaAndTruncateTable(engine)
    has, alters := tableSchema.GetSchemaChanges(engine)

    /*getting table structure*/
    db := tableSchema.GetMysql(engine)
    localCache, has := tableSchema.GetLocalCache(engine) 
    redisCache, has := tableSchema.GetRedisCache(engine)
    columns := tableSchema.GetColumns()
    tableSchema.GetTableName()
 }
 
 ```


## Adding, editing, deleting entities

```go
package main

import "github.com/latolukasz/orm"

func main() {

     /* adding */

    entity := testEntity{Name: "Name 1"}
    engine.Flush(&entity)

    entity2 := testEntity{Name: "Name 1"}
    entity2.SetOnDuplicateKeyUpdate(orm.Bind{"Counter": 2}, entity2)
    engine.Flush(&entity2)

    entity2 = testEntity{Name: "Name 1"}
    engine.SetOnDuplicateKeyUpdate(orm.Bind{}, entity2) //it will change nothing un row
    engine.Flush(&entity)

    /*if you need to add more than one entity*/
    entity = testEntity{Name: "Name 2"}
    entity2 := testEntity{Name: "Name 3"}
    flusher := engine.NewFlusher()
    flusher.Track(&entity, &entity2)
    //it will execute only one query in MySQL adding two rows at once (atomic)
    flusher.Flush()
 
    /* editing */

    flusher := engine.NewFlusher().Track(&entity, &entity2)
    entity.Name = "New name 2"
    //you can also use (but it's slower):
    entity.SetField("Name", "New name 2")
    entity.IsDirty() //returns true
    entity2.IsDirty() //returns false
    flusher.Flush() //it will save data in DB for all dirty tracked entities
    engine.IsDirty(entity) //returns false
    
    /* deleting */
    entity2.Delete()
    //or
    flusher.Delete(&entity, &entity2).Flush()

    /* flush will panic if there is any error. You can catch 2 special errors using this method  */
    err := flusher.FlushWithCheck()
    //or
    err := flusher.FlushInTransactionWithCheck()
    orm.DuplicatedKeyError{} //when unique index is broken
    orm.ForeignKeyError{} //when foreign key is broken
    
    /* You can catch all errors using this method  */
    err := flusher.FlushWithFullCheck()
}
```

## Transactions

```go
package main

import "github.com/latolukasz/orm"

func main() {
	
    entity = testEntity{Name: "Name 2"}
    entity2 := testEntity{Name: "Name 3"}
    flusher := engine.NewFlusher().Track(&entity, &entity2)

    // DB transcation
    flusher.FlushInTransaction()
    // or redis lock
    flusher.FlushWithLock("default", "lock_name", 10 * time.Second, 10 * time.Second)
    // or DB transcation nad redis lock
    flusher.FlushInTransactionWithLock("default", "lock_name", 10 * time.Second, 10 * time.Second)
 
    //manual transaction
    db := engine.GetMysql()
    db.Begin()
    defer db.Rollback()
    //run queries
    db.Commit()
```

## Loading entities using primary key

```go
package main

import "github.com/latolukasz/orm"

func main() {

    var entity testEntity
    has := engine.LoadByID(1, &entity)
    
    var entities []*testEntity
    missing := engine.LoadByIDs([]uint64{1, 3, 4}, &entities) //missing contains IDs that are missing in database

}

```

## Loading entities using search

```go
package main

import "github.com/latolukasz/orm"

func main() {

    var entities []*testEntity
    pager := orm.NewPager(1, 1000)
    where := orm.NewWhere("`ID` > ? AND `ID` < ?", 1, 8)
    engine.Search(where, pager, &entities)
    
    //or if you need number of total rows
    totalRows := engine.SearchWithCount(where, pager, &entities)
    
    //or if you need only one row
    where := onm.NewWhere("`Name` = ?", "Hello")
    var entity testEntity
    found := engine.SearchOne(where, &entity)
    
    //or if you need only primary keys
    ids := engine.SearchIDs(where, pager, entity)
    
    //or if you need only primary keys and total rows
    ids, totalRows = engine.SearchIDsWithCount(where, pager, entity)
}

```

## Lazy loading

Loading entities is very fast. But if you need to load many entities you can make it even faster
using lazy loading. Simply use methods that ends with "Lazy". Such methods are many times faster and
use less memory and allocations because struct fields are not filled with data. See example below

```go
func main() {

 var entities []*testEntity
 // load 1000 entities
 total := 0
 engine.SearchLazy(orm.NewWhere("1"), orm.NewPager(1, 1000), &entities)
 for _, e := range entities {
  e.IsLazy() // true
  e.Balance // always zero, it's not filled with DB data
  total += e.GetFieldLazy("Balance").(uint64) // returns data from DB
 }
 fmt.Printf("Amount: %d\n", total)

}
```

Field type mapping:

| Field type  | Lazy type | 
|---|---|
| *uint,*uint8,*uint16,*uint32,*uint64    | uint64,nil  |
| *int,*int8,*int16,*int32,*int64    | int64,nil  |
| uint,uint8,uint16,uint32,uint64    | uint64  |
| int,int8,int16,int32,int64    | int64  |
| string    | string,nil  |
| []string    | []string,nil  |
| FakeDelete    | bool  |
| bool    | bool  |
| *bool    | bool,nil  |
| *float32,*float64    | float64,nil  |
| float32,float64    | float64  |
| *time.Time    | time.Time,nil  |
| time.Time   | time.Time  |
| struct   | string (encoded json),nil  |
| *orm.Entity   | uint64,nil  |
| []*orm.Entity   | string (encoded json of []uint64),nil  |


Lazy entity can't be edited and flushed. But you can use *Fill* method to manage that:

```go
func main() {

 var entities []*testEntity
 // load 1000 entities
 total := 0
 engine.SearchLazy(orm.NewWhere("1"), orm.NewPager(1, 1000), &entities)
 for _, e := range entities {
    e.IsLazy() // true
    if e.GetFieldLazy("Old").bool {
        e.Fill(engine)
        e.IsLazy() // false
        e.Balance // holds data from DB now
        e.Old = false
        engine.Flush(e)
    }           
 }

}
```

## Reference one to one

```go
package main

import "github.com/latolukasz/orm"

func main() {

    type UserEntity struct {
        ORM
        ID                   uint64
        Name                 string
        School               *SchoolEntity `orm:"required"` // key is "on delete restrict" by default not not nullable
        SecondarySchool      *SchoolEntity // key is nullable
    }
    
    type SchoolEntity struct {
        ORM
        ID                   uint64
        Name                 string
    }

    type UserHouse struct {
        ORM
        ID                   uint64
        User                 *UserEntity  `orm:"cascade;required"` // on delete cascade and is not nullable
    }
    
    // saving in DB:

    user := UserEntity{Name: "John"}
    school := SchoolEntity{Name: "Name of school"}
    house := UserHouse{Name: "Name of school"}
    engine.Track(&user, &school, &house)
    user.School = school
    house.User = user
    engine.Flush()

    // loading references: 

    _ = engine.LoadById(1, &user)
    user.School != nil //returns true, School has ID: 1 but other fields are nof filled
    user.School.ID == 1 //true
    user.School.Loaded() //false
    user.Name == "" //true
    user.School.Load(engine) //it will load school from db
    user.School.Loaded() //now it's true, you can access school fields like user.School.Name
    user.Name == "Name of school" //true
    
    //If you want to set reference and you have only ID:
    user.School = &SchoolEntity{ID: 1}

    // detaching reference
    user.School = nil

    // preloading references
    engine.LoadByID(1, &user, "*") //all references
    engine.LoadByID(1, &user, "School") //only School
    engine.LoadByID(1, &user, "School", "SecondarySchool") //only School and SecondarySchool
    engine.LoadByID(1, &userHouse, "User/School", "User/SecondarySchool") //User, School and SecondarySchool in each User
    engine.LoadByID(1, &userHouse, "User/*") // User, all references in User
    engine.LoadByID(1, &userHouse, "User/*/*") // User, all references in User and all references in User subreferences
    //You can have as many levels you want: User/School/AnotherReference/EvenMore/
    
    //You can preload referenes in all search and load methods:
    engine.LoadByIDs()
    engine.Search()
    engine.SearchOne()
    engine.CachedSearch()
    ...
}

```

## Cached queries

```go
package main

import "github.com/latolukasz/orm"

func main() {

    //Fields that needs to be tracked for changes should start with ":"

    type UserEntity struct {
        ORM
        ID                   uint64
        Name                 string
        Age                  uint16
        IndexAge             *CachedQuery `query:":Age = ? ORDER BY :ID"`
        IndexAll             *CachedQuery `query:""` //cache all rows
        IndexName            *CachedQuery `queryOne:":Name = ?"`
    }

    pager := orm.NewPager(1, 1000)
    var users []*UserEntity
    var user  UserEntity
    totalRows := engine.CachedSearch(&users, "IndexAge", pager, 18)
    totalRows = engine.CachedSearch(&users, "IndexAll", pager)
    has := engine.CachedSearchOne(&user, "IndexName", "John")

}

```

## Lazy flush

Sometimes you want to flush changes in database, but it's ok if data is flushed after some time. 
For example when you want to save some logs in database.

```go
package main

import "github.com/latolukasz/orm"

func main() {
    
    // you need to register redis  
    registry.RegisterRedis("localhost:6379", 0)
    registry.RegisterRedis("localhost:6380", 0, "another_redis")
    
    // .. create engine

    type User struct {
       ORM  `orm:"log"`
       ID   uint
       Name string
       Age  int `orm:"skip-log"` //Don't track this field
    }
   
    // optionally you can set optional redis pool used to queue all events
    type Dog struct {
       ORM  `orm:"asyncRedisLazyFlush=another_redis"`
       ID   uint
       Name string
    }
       
    // now in code you can use FlushLazy() methods instead of Flush().
    // it will send changes to queue (database and cached is not updated yet)
    engine.FlushLazy()
    
    // you need to run code that will read data from queue and execute changes
    // run in separate goroutine (cron script)
    consumer := NewAsyncConsumer(engine, "my-consumer", 1) // you can run maximum one consumer
    consumer.Digest() //It will wait for new messages in a loop, run receiver.DisableLoop() to run loop once

    consumerAnotherPool := NewAsyncConsumer(engine,  "my-consumer", 5) // you can run up to 5 consumers at the same time
    consumerAnotherPool.Digets()
}

```

## Request cache

It's a good practice to cache entities in one short request (e.g. http request) to reduce number of requests to databases.

If you are using more than one goroutine (for example in GraphQL backend implementation) you can enable Data loader in engine to group many queries into one and reduce
number of queries. You can read more about idea behind it [here](https://gqlgen.com/reference/dataloaders/).

```go
package main

import "github.com/latolukasz/orm"

func main() {
    engine.EnableDataLoader(true)
}
```

Otherwise set false and all entities will be cached in a simple temporary cache:

```go
package main

import "github.com/latolukasz/orm"

func main() {
    engine.EnableDataLoader(false) 
}

```

## Log entity changes

ORM can store in database every change of entity in special log table.

```go
package main

import "github.com/latolukasz/orm"

func main() {

    //it's recommended to keep logs in separated DB
    registry.RegisterMySQLPool("root:root@tcp(localhost:3306)/log_database", "log_db_pool")
    // you need to register default Redis   
    registry.RegisterRedis("localhost:6379", 0)
    registry.RegisterRedis("localhost:6380", 0, "another_redis")

    //next you need to define in Entity that you want to log changes. Just add "log" tag or define mysql pool name
    type User struct {
        ORM  `orm:"log"`
        ID   uint
        Name string
        Age  int `orm:"skip-log"` //Don't track this field
    }
    
    // optionally you can set optional redis pool used to queue all events
     type Dog struct {
        ORM  `orm:"log=log_db_pool;asyncRedisLogs=another_redis"`
        ID   uint
        Name string
     }

    // Now every change of User will be saved in log table
    
    // You can add extra data to log, simply use this methods before Flush():
    engine.SetLogMetaData("logged_user_id", 12) 
    engine.SetLogMetaData("ip", request.GetUserIP())
    // you can set meta only in specific entity
    engine.SetEntityLogMeta("user_name", "john", entity)
    
    consumer := NewAsyncConsumer(engine, "my-consumer",  1)
    consumer.Digets() //it will wait for new messages in queue

    consumerAnotherPool := NewAsyncConsumer(engine, "my-consumer", 1)
    consumerAnotherPool.Digets()
}

```

## Dirty stream

You can send event to event broker if any specific data in entity was changed.

```go
package main

import "github.com/latolukasz/orm"

func main() {

	//define at least one redis pool
    registry.RegisterRedis("localhost:6379", 0, "event-broker-pool")
    //define stream with consumer groups for events
    registry.RegisterRedisStream("user_changed", "event-broker-pool", []string{"my-consumer-group"})
    registry.RegisterRedisStream("age_name_changed", "event-broker-pool", []string{"my-consumer-group"})
    registry.RegisterRedisStream("age_changed", "event-broker-pool", []string{"my-consumer-group"})

    // create engine
    
    // next you need to define in Entity that you want to log changes. Just add "dirty" tag
    type User struct {
        orm.ORM  `orm:"dirty=user_changed"` //define dirty here to track all changes
        ID       uint
        Name     string `orm:"dirty=age_name_changed"` //event will be send to age_name_changed if Name or Age changed
        Age      int `orm:"dirty=age_name_changed,age_changed"` //event will be send to age_changed if Age changed
    }
 
    consumer := engine.GetEventBroker().Consume("my-consumer", "my-consumer-group", 1)

    consumer.Consume(context.Background(), 100, func(events []orm.Event) {
        for _, event := range events {
           dirty := orm.EventDirtyEntity(event) // created wrapper around event to easily access data
           if dirty.Added() {
           	 fmt.Printf("Entity %s with ID %d was added", dirty.TableSchema().GetType().String(), dirty.ID())
           } else if dirty.Updated() {
           	 fmt.Printf("Entity %s with ID %d was updated", dirty.TableSchema().GetType().String(), dirty.ID())
           } else if dirty.Deleted() {
             fmt.Printf("Entity %s with ID %d was deleted", dirty.TableSchema().GetType().String(), dirty.ID())
           }
           event.Ack()
        }
    })
}


```

## Fake delete

If you want to keep deleted entity in database but ny default this entity should be excluded
from all engine.Search() and engine.CacheSearch() queries you can use FakeDelete column. Simply create
field bool with name "FakeDelete".

```go
func main() {

    type UserEntity struct {
        ORM
        ID                   uint64
        Name                 string
        FakeDelete           bool
    }

    //you can delete in two ways:
    engine.Delete(user) -> will set user.FakeDelete = true
    //or:
    user.FakeDelete = true

    engine.Flush(user) //it will save entity id in Column `FakeDelete`.

    //will return all rows where `FakeDelete` = 0
    total, err = engine.SearchWithCount(NewWhere("1"), nil, &rows)

    //To force delete (remove row from DB):
    engine.ForceDelete(user)
}


```

## Working with Redis

```go
package main

import "github.com/latolukasz/orm"

func main() {

    config.RegisterRedis("localhost:6379", 0)
    
    //storing data in cached for x seconds
    val := engine.GetRedis().GetSet("key", 1, func() interface{} {
		return "hello"
	})
    
    //standard redis api
    keys := engine.GetRedis().LRange("key", 1, 2)
    engine.GetRedis().LPush("key", "a", "b")
    //...

    //rete limiter
    valid := engine.GetRedis().RateLimit("resource_name", redis_rate.PerMinute(10))
}

```


## Working with local cache

```go
package main

import "github.com/latolukasz/orm"

func main() {
    
    registry.RegisterLocalCache(1000)
    
    //storing data in cached for x seconds
    val := engine.GetLocalCache().GetSet("key", 1, func() interface{} {
        return "hello"
    })
    
    //getting value
    value, has := engine.GetLocalCache().Get("key")
    
    //getting many values
    values := engine.GetLocalCache().MGet("key1", "key2")
    
    //setting value
    engine.GetLocalCache().Set("key", "value")
    
    //setting values
    engine.GetLocalCache().MSet("key1", "value1", "key2", "value2" /*...*/)
    
    //getting values from hash set (like redis HMGET)
    values = engine.GetLocalCache().HMget("key")
    
    //setting values in hash set
    engine.GetLocalCache().HMset("key", map[string]interface{}{"key1" : "value1", "key2": "value 2"})

    //deleting value
    engine.GetLocalCache().Remove("key1", "key2" /*...*/)
    
    //clearing cache
    engine.GetLocalCache().Clear()

}

```

## Working with mysql

```go
package main

import (
    "database/sql"
    "github.com/latolukasz/orm"
)

func main() {
    
    // register mysql pool
    registry.RegisterMySQLPool("root:root@tcp(localhost:3306)/database_name")

    res := engine.GetMysql().Exec("UPDATE `table_name` SET `Name` = ? WHERE `ID` = ?", "Hello", 2)

    var row string
    found := engine.GetMysql().QueryRow(orm.NewWhere("SELECT * FROM `table_name` WHERE  `ID` = ?", 1), &row)
    
    results, def := engine.GetMysql().Query("SELECT * FROM `table_name` WHERE  `ID` > ? LIMIT 100", 1)
    defer def()
    for results.Next() {
    	var row string
        results.Scan(&row)
    }
    def() //if it's not last line in this method
}

```

## Working with elastic search

```go
package main

import (
    "github.com/latolukasz/orm"
)

func main() {

    type TestIndex struct {
    }
    
    func (i *TestIndex) GetName() string {
    	return "test_index"
    }
    
    func (i *TestIndex) GetDefinition() map[string]interface{} {
        return map[string]interface{}{
            "settings": map[string]interface{}{
                "number_of_replicas": "1",
                "number_of_shards":   "1",
            },
            "mappings": map[string]interface{}{
                "properties": map[string]interface{}{
                    "Name": map[string]interface{}{
                        "type":       "keyword",
                        "normalizer": "case_insensitive",
                    },
                },
            },
        }
    }

    
    // register elastic search pool and index
    registry.RegisterElastic("http://127.0.0.1:9200")
    registry.RegisterElasticIndex(&TestIndex{})


    e := engine.GetElastic()

    // create indices
	for _, alter := range engine.GetElasticIndexAlters() {
        // alter.Safe is true if index does not exists or is not empty
		engine.GetElastic(alter.Pool).CreateIndex(alter.Index)
	}

    query := elastic.NewBoolQuery()
	query.Must(elastic.NewTermQuery("user_id", 12))
    options := &orm.SearchOptions{}
    options.AddSort("created_at", true).AddSort("name", false)
	results := e.Search("users", query, orm.NewPager(1, 10), options)
}

```

## Working with ClickHouse

```go
package main

import (
    "github.com/latolukasz/orm"
)

func main() {
    
    // register elastic search pool
    registry.RegisterClickHouse("http://127.0.0.1:9000")

    ch := engine.GetClickHouse()

    ch.Exec("INSERT INTO `table` (name) VALUES (?)", "hello")

    statement, def := ch.Prepare("INSERT INTO `table` (name) VALUES (?)")
    defer def()
    statement.Exec("hello")
    statement.Exec("hello 2")

    rows, def := ch.Queryx("SELECT FROM `table` WHERE x = ? AND y = ?", 1, "john")
    defer def()
    for rows.Next() {
    	m := &MyStruct{}
        err := rows.StructScan(m)
    }

    ch.Begin()
    defer ch.Rollback()
    // run queries
    defer ch.Commit()
}

```

## Working with Locker

Shared cached that is using redis

```go
package main

import "github.com/latolukasz/orm"

func main() {

    // register redis
    registry.RegisterRedis("localhost:6379", 0)
    
    locker, _ := engine.GetLocker()
    lock := locker.Obtain("my_lock", 5 * Time.Second, 1 * Time.Second)

    defer lock.Release()
    
    // do smth
    
    ttl := lock.TTL()
    if ttl == 0 {
        panic("lock lost")
    }
}

```
## Query logging

You can log all queries:
 
 * queries to MySQL database (insert, select, update)
 * requests to Redis
 * requests to Elastic Search 
 * queries to CickHouse 

```go
package main

import "github.com/latolukasz/orm"

func main() {
	
    //enable human friendly console log
    engine.EnableQueryDebug() //MySQL, redis, Elastic Search, ClickHouse queries (local cache in excluded bt default)
    engine.EnableQueryDebug(orm.QueryLoggerSourceRedis, orm.QueryLoggerSourceLocalCache)

    //adding custom logger example:
    engine.AddQueryLogger(json.New(os.Stdout), log.LevelWarn) //MySQL, redis warnings and above
    engine.AddQueryLogger(es.New(os.Stdout), log.LevelError, orm.QueryLoggerSourceRedis)
}    
```

## Logger

```go
package main

import "github.com/latolukasz/orm"

func main() {
	
    //enable json logger with proper level
    engine.EnableLogger(log.InfoLevel)
    //or enable human friendly console logger
    engine.EnableDebug()
    
    //you can add special fields to all logs
    engine.Log().AddFields(log.Fields{"user_id": 12, "session_id": "skfjhfhhs1221"})

    //printing logs
    engine.Log().Warn("message", nil)
    engine.Log().Debug("message", log.Fields{"user_id": 12})
    engine.Log().Error(err, nil)
    engine.Log().ErrorMessage("ups, that is strange", nil)


    //handling recovery
    if err := recover(); err != nil {
    	engine.Log().Error(err, nil)
    }

    //filling log with data from http.Request
    engine.Log().AddFieldsFromHTTPRequest(request, "197.21.34.22")

}    
```

## Event broker

ORM provides easy way to use event broker.

First yuo need to define streams and consumer groups:

```yaml
#YAML config file
default:
  redis: localhost:6381:0 // redis is required
  streams:
    stream-1:
      - test-group-1
      - test-group-2
    stream-2:
      - test-group-1
    stream-3:
      - test-group-3
```
or using go:

```go
package main

import "github.com/latolukasz/orm"

func main() {
 registry := &orm.Registry{}
 registry.RegisterRedisStream("stream-1", "default", []string{"test-group-1", "test-group-2"})
 registry.RegisterRedisStream("stream-2", "default", []string{"test-group-1"})
 registry.RegisterRedisStream("stream-3", "default", []string{"test-group-3"})
}    
```


Publishing and receiving events
:

```go
package main

import (
 "context"
 "github.com/latolukasz/orm"
)

func main() {

 // .. create engine

 type Person struct {
  Name string
  Age  int
 }

 // fastest, no serialization
 engine.GetEventBroker().PublishMap("stream-1", orm.EventAsMap{"key": "value", "anotherKey": "value 2"})
 
 // using serialization
 engine.GetEventBroker().Publish("stream-3", Person{Name: "Adam", Age: 18})

 // publishing many at once, recommended because it's much faster than one by one
 flusher :=  engine.GetEventBroker().NewFlusher()
 flusher.PublishMap("stream-1", orm.EventAsMap{"key": "value", "anotherKey": "value 2"})
 flusher.Publish("stream-1", Person{Name: "Adam", Age: 18})
 flusher.Flush()
 
 // reading from "stream-1" and "stream-2" streams, you can run max one consumer at once
 consumerTestGroup1 := engine.GetEventBroker().Consumer("my-consumer", "test-group-1", 1)
 
 // reading max 100 events in one loop, this line stop execution, waiting for new events
 consumerTestGroup1.Consumer(context.Background(), 100, func(events []orm.Event) {
 	for _, event := range events {
            values := event.RawData() // map[string]interface{}{"key": "value", "anotherKey": "value 2"}
            //do some work
            event.Ack() // this line is acknowledging event
 	}
 })

 // auto acknowledging
 consumerTestGroup1.Consumer(context.Background(), 100, func(events []orm.Event) { 
 	//do some work, for example put all events at once to DB
 	// in this example all events will be acknowledge when this method is finished 
 })

 // skipping some events
 consumerTestGroup1.Consumer(context.Background(), 100, func(events []orm.Event) {
  for _, event := range events {
        if someCondition {
             event.Ack()
        } else {
             event.Skip() //this event will be consumed again later
        }
    }
 })

 // reading from "stream-3" stream, you can run max to two consumers at once
 consumerTestGroup3 := engine.GetEventBroker().Consumer("my-consumer", "test-group-2", 2)
 consumerTestGroup3.DisableLoop() // all events will be consumed once withour waiting for new events   

 consumerTestGroup3.Consumer(context.Background(), 100, func(events []orm.Event) {
    var person Person
 	for _, event := range events {
        err := event.Unserialize(&person)
        if err != nil {
        	// ...
        }
        //do some work
        event.Ack() // this line is acknowledging event
    }
 })

}    
```

You can register special function that will handle panics:
```go
 consumer := broker.Consumer("test-consumer", "test-group")
 consumer.SetErrorHandler(func(err interface{}, event Event) error {
   // log error somewhere
   // optionally you can remove this event from stream
   event.Ack()
   return nil // do not panic, error is handled above, you can return err if you want consumer to stop with panic
 })

```


Setting another redis pool for AsyncConsumer:
```yaml
another_pool:
  redis: localhost:6381:0
  streams:
   orm-lazy-channel: # FlushLazy()
      - default-group # group name for AsyncConsumer
    orm-log-channel: # adding changes to logs
      - default-group # group name for AsyncConsumer
      - test-group-2 # you can register another consumers to read logs  
```

## Redis search

Be sure your redis has [Redis Search module](https://github.com/RediSearch/RediSearch) 2.X installed.

### Indexer

You need to run special script that is indexing data in redis search for you.
Be sure that this script is running in your code:

```go
package main

import "github.com/latolukasz/orm"
func main() {
 indexer := orm.NewRedisSearchIndexer(engine)
 indexer.Run(context.Background())
}
```

### Redis search index alters

Every time you run your code be sure you are checking if redis search indices are valid:

```go
altersSearch := engine.GetRedisSearchIndexAlters()
for _, alter := range altersSearch {
	// show them or execute: 
	alter.Execute()
}
```

### Entity redis search

It's very easy to search ofr entities. Simply use special
orm entity tag close to fields that should be used in search:

```go
package main

import "github.com/latolukasz/orm"

type MyEntity struct {
 orm.ORM             `orm:"redisSearch=search"`
 ID              uint
 Name            string             `orm:"searchable"`
 Age             uint64             `orm:"searchable;sortable"`
 Balance         int64              `orm:"sortable"`
 Weight          float64            `orm:"searchable"`
 Enum            string             `orm:"enum=orm.TestEnum;required;searchable"`
}
  
```

That's it:)
All you need to do from now is to add/edit/delete entities as always using Flush() and FlushLazy().

Tags:
 * **searchable** - you can search for this entity using where condition for this field
 * **sortable** - you can sort results using this field

Now it's time to search some entities:


```go
package main

import "github.com/latolukasz/orm"

func main() {

  // engine := ...	 
  	
 query := &orm.RedisSearchQuery{}
 query.Query("adam").Sort("Age", false).FilterIntMinMax("Age", 6, 8).FilterTag("Enum", "active", "blocked")
 
 // getting ids
 ids, total := engine.RedisSearchIds(entity, query, orm.NewPager(1, 50))
 // getting entities
 total = engine.RedisSearch(&entities, query, NewPager(1, 10))
 // getting entity
 found := engine.RedisSearchOne(entity, query)
 
 // some another examples of query
 query.FilterInt("Age", 12)
 query.FilterIntNull("Age")
 query.FilterIntGreaterEqual("Age", 12)
 query.FilterDateGreaterEqual("Age", time.Now())
 query.FilterBool("Active", true)
 query.Query("@y:foo (-@x:foo) (-@x:bar)")
}    
```
Read more about redis search query syntax [here](https://oss.redislabs.com/redisearch/Query_Syntax/).

### Building your own redis search index

If you need to build more advanced redis search simply register an index manually. Below example:

```go
package main

import "github.com/latolukasz/orm/tools"

func main() {
  registry := &orm.Registry{}
  registry.RegisterRedis("localhost:6383", 0, "search")
  
  // register an index:
  myIndex := &orm.RedisSearchIndex{}
  myIndex.Name = "my-index"
  myIndex.RedisPool = "search"
  myIndex.Prefixes = []string{"my-index:"}
  myIndex.AddTextField("name", 1, true, false, false)
  myIndex.AddNumericField("age", true, false)
  myIndex.AddGeoField("location", false, false)
  myIndex.Indexer = func(engine *orm.Engine, lastID uint64, pusher orm.RedisSearchIndexPusher) (newID uint64, hasMore bool) {
     rows := // select from database WHERE ID > lastID ORDER BY ID LIMIT 1000
     for _, row := range rows {
      pusher.NewDocument("my-index:" + row.ID)
      pusher.SetField("name", row.Name)
      pusher.SetField("age", row.Age)
      pusher.SetField("location", row.location)
      pusher.PushDocument()
      lastID = row.id
     }
     return lastID, len(rows) < 1000 // return last used id and false if there ar eno more rows ro index
  }
  registry.RegisterRedisSearchIndex(myIndex)
  validatedRegistry, _ := registry.Validate()
  engine := validatedRegistry.CreateEngine()
  
  // adding, updating documents:
  pusher := engine.NewRedisSearchIndexPusher("search")
  pusher.NewDocument("my-index:33")
  pusher.SetField("name", "Tom")
  pusher.SetField("age", 12)
  pusher.SetField("location", "52.2982648,17.0103596")
  pusher.PushDocument()
  pusher.NewDocument("my-index:34")
  pusher.SetField("name", "Adam")
  pusher.SetField("age", 18)
  pusher.PushDocument()
  pusher.DeletewDocuments("my-index:35", "my-index:36")
  pusher.Flush()
 
  // searching
  search := engine.GetRedisSearch("search")
  query := &orm.RedisSearchQuery{}
  query.Query("tom").Verbatim().NoStopWords().Sort("age", true)
  
  // return data from index
  total, rowsRaw := search.Search("my-index", query, orm.NewPager(1, 2))
  // return just keys
  total, keys := search.SearchKeys("my-index", query, orm.NewPager(1, 2))
  
  // return only name and age
 query.Return("name", "age")
 total, rowsRaw := search.Search("my-index", query, orm.NewPager(1, 2))
}    
```

## Tools

```go
package main

import "github.com/latolukasz/orm/tools"

func main() {
   // Redis streams statistics	
   tools.GetRedisStreamsStatistics(engine)
   // Redis statistics	
   tools.GetRedisStatistics(engine)
   // Redis search statistics	
   tools.GetRedisSearchStatistics(engine)
}    
```


## Health check

ORM provides special method that can be used to check that all database services are up and running.

```go
package main

import (
 "fmt"
 "github.com/latolukasz/orm/tools"
)

func main() {
 registry := &orm.Registry{}
 registry.RegisterRedis("localhost:6383")
 // register other services
 validatedRegistry, _ := registry.Validate()
 engine := validatedRegistry.CreateEngine()

 errors, warnings, valid := engine.HealthCheck()
 for _, err := range errors {
    fmt.Printf("Error for step %s (%s): %s\n", err.Name, err.Description, err.Message)
 }
 for _, warn := range warnings {
    fmt.Printf("Warning for step %s (%s): %s\n", warn.Name, warn.Description, warn.Message)
 }
 for _, step := range valid {
    fmt.Printf("Warning for step %s (%s) passed\n", step.Name, step.Description)
 }

}    
```