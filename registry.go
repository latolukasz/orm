package orm

import (
	"database/sql"
	"fmt"
	log2 "log"
	"math"
	"os"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/pkg/errors"

	"github.com/go-redis/redis/v8"
	_ "github.com/go-sql-driver/mysql" // force this mysql driver
	"github.com/jmoiron/sqlx"
	"github.com/olivere/elastic/v7"
)

type Registry struct {
	mysqlPools         map[string]MySQLPoolConfig
	clickHouseClients  map[string]*ClickHouseConfig
	localCachePools    map[string]LocalCachePoolConfig
	redisPools         map[string]RedisPoolConfig
	elasticServers     map[string]*ElasticConfig
	entities           map[string]reflect.Type
	redisSearchIndices map[string]map[string]*RedisSearchIndex
	elasticIndices     map[string]map[string]ElasticIndexDefinition
	enums              map[string]Enum
	defaultEncoding    string
	redisStreamGroups  map[string]map[string]map[string]bool
	redisStreamPools   map[string]string
}

func NewRegistry() *Registry {
	return &Registry{}
}

func (r *Registry) Validate() (ValidatedRegistry, error) {
	if r.defaultEncoding == "" {
		r.defaultEncoding = "utf8mb4"
	}
	registry := &validatedRegistry{}
	registry.registry = r
	l := len(r.entities)
	registry.tableSchemas = make(map[reflect.Type]*tableSchema, l)
	registry.entities = make(map[string]reflect.Type)
	if registry.mySQLServers == nil {
		registry.mySQLServers = make(map[string]MySQLPoolConfig)
	}
	for k, v := range r.mysqlPools {
		db, err := sql.Open("mysql", v.GetDataSourceURI())
		if err != nil {
			return nil, err
		}
		var version string
		err = db.QueryRow("SELECT VERSION()").Scan(&version)
		if err != nil {
			return nil, err
		}
		v.(*mySQLPoolConfig).version, _ = strconv.Atoi(strings.Split(version, ".")[0])

		var autoincrement uint64
		var maxConnections int
		var skip string
		err = db.QueryRow("SHOW VARIABLES LIKE 'auto_increment_increment'").Scan(&skip, &autoincrement)
		if err != nil {
			return nil, err
		}
		v.(*mySQLPoolConfig).autoincrement = autoincrement

		err = db.QueryRow("SHOW VARIABLES LIKE 'max_connections'").Scan(&skip, &maxConnections)
		if err != nil {
			return nil, err
		}
		var waitTimeout int
		err = db.QueryRow("SHOW VARIABLES LIKE 'wait_timeout'").Scan(&skip, &waitTimeout)
		if err != nil {
			return nil, err
		}
		maxConnections = int(math.Floor(float64(maxConnections) * 0.9))
		if maxConnections == 0 {
			maxConnections = 1
		}
		maxLimit := v.getMaxConnections()
		if maxLimit == 0 {
			maxLimit = 100
		}
		if maxConnections < maxLimit {
			maxLimit = maxConnections
		}
		if waitTimeout == 0 {
			waitTimeout = 180
		}
		waitTimeout = int(math.Min(float64(waitTimeout), 180))
		db.SetMaxOpenConns(maxLimit)
		db.SetMaxIdleConns(maxLimit)
		db.SetConnMaxLifetime(time.Duration(waitTimeout) * time.Second)
		v.(*mySQLPoolConfig).client = db
		registry.mySQLServers[k] = v
	}
	if registry.clickHouseClients == nil {
		registry.clickHouseClients = make(map[string]*ClickHouseConfig)
	}
	for k, v := range r.clickHouseClients {
		db, err := sqlx.Open("clickhouse", v.url)
		if err != nil {
			return nil, err
		}
		v.db = db
		registry.clickHouseClients[k] = v
	}

	if registry.localCacheServers == nil {
		registry.localCacheServers = make(map[string]LocalCachePoolConfig)
	}
	for k, v := range r.localCachePools {
		registry.localCacheServers[k] = v
	}
	if registry.redisServers == nil {
		registry.redisServers = make(map[string]RedisPoolConfig)
	}
	for k, v := range r.redisPools {
		registry.redisServers[k] = v
	}
	if registry.elasticServers == nil {
		registry.elasticServers = make(map[string]*ElasticConfig)
	}
	for k, v := range r.elasticServers {
		registry.elasticServers[k] = v
	}
	if registry.enums == nil {
		registry.enums = make(map[string]Enum)
	}
	for k, v := range r.enums {
		registry.enums[k] = v
	}
	registry.redisSearchIndexes = make(map[string]map[string]*RedisSearchIndex)
	for k, v := range r.redisSearchIndices {
		registry.redisSearchIndexes[k] = make(map[string]*RedisSearchIndex)
		for k2, v2 := range v {
			registry.redisSearchIndexes[k][k2] = v2
		}
	}
	cachePrefixes := make(map[string]*tableSchema)
	hasLog := false
	for name, entityType := range r.entities {
		tableSchema, err := initTableSchema(r, entityType)
		if err != nil {
			return nil, err
		}
		registry.tableSchemas[entityType] = tableSchema
		duplicated, has := cachePrefixes[tableSchema.cachePrefix]
		if has {
			return nil, fmt.Errorf("duplicated table cache prefix %s and %s", tableSchema.tableName, duplicated.tableName)
		}
		cachePrefixes[tableSchema.cachePrefix] = tableSchema
		registry.entities[name] = entityType
		if tableSchema.redisSearchIndex != nil {
			index := tableSchema.redisSearchIndex
			if registry.redisSearchIndexes[index.RedisPool] == nil {
				registry.redisSearchIndexes[index.RedisPool] = make(map[string]*RedisSearchIndex)
			}
			registry.redisSearchIndexes[index.RedisPool][index.Name] = index
		}
		if tableSchema.hasLog {
			hasLog = true
		}
	}
	_, has := r.redisStreamPools[lazyChannelName]
	if !has {
		r.RegisterRedisStream(lazyChannelName, "default", []string{asyncConsumerGroupName})
	}
	if hasLog {
		_, has = r.redisStreamPools[logChannelName]
		if !has {
			r.RegisterRedisStream(logChannelName, "default", []string{asyncConsumerGroupName})
		}
	}
	if len(registry.redisSearchIndexes) > 0 {
		_, has = r.redisStreamPools[redisSearchIndexerChannelName]
		if !has {
			r.RegisterRedisStream(redisSearchIndexerChannelName, "default", []string{asyncConsumerGroupName})
		}
	}
	registry.redisStreamGroups = r.redisStreamGroups
	registry.redisStreamPools = r.redisStreamPools
	engine := registry.CreateEngine()
	for _, schema := range registry.tableSchemas {
		_, err := checkStruct(schema, engine, schema.t, make(map[string]*index), make(map[string]*foreignIndex), "")
		if err != nil {
			return nil, errors.Wrapf(err, "invalid entity struct '%s'", schema.t.String())
		}
	}
	return registry, nil
}

func (r *Registry) SetDefaultEncoding(encoding string) {
	r.defaultEncoding = encoding
}

func (r *Registry) RegisterEntity(entity ...Entity) {
	if r.entities == nil {
		r.entities = make(map[string]reflect.Type)
	}
	for _, e := range entity {
		t := reflect.TypeOf(e)
		if t.Kind() == reflect.Ptr {
			t = t.Elem()
		}
		r.entities[t.String()] = t
	}
}

func (r *Registry) RegisterRedisSearchIndex(index ...*RedisSearchIndex) {
	if r.redisSearchIndices == nil {
		r.redisSearchIndices = make(map[string]map[string]*RedisSearchIndex)
	}
	for _, i := range index {
		if r.redisSearchIndices[i.RedisPool] == nil {
			r.redisSearchIndices[i.RedisPool] = make(map[string]*RedisSearchIndex)
		}
		r.redisSearchIndices[i.RedisPool][i.Name] = i
	}
}

func (r *Registry) RegisterElasticIndex(index ElasticIndexDefinition, serverPool ...string) {
	if r.elasticIndices == nil {
		r.elasticIndices = make(map[string]map[string]ElasticIndexDefinition)
	}
	pool := "default"
	if len(serverPool) > 0 {
		pool = serverPool[0]
	}
	if r.elasticIndices[pool] == nil {
		r.elasticIndices[pool] = make(map[string]ElasticIndexDefinition)
	}
	r.elasticIndices[pool][index.GetName()] = index
}

func (r *Registry) RegisterEnumStruct(code string, val interface{}) {
	enum := initEnum(val)
	if r.enums == nil {
		r.enums = make(map[string]Enum)
	}
	r.enums[code] = enum
}

func (r *Registry) RegisterEnum(code string, val ...string) {
	e := enum{}
	e.fields = val
	e.defaultValue = val[0]
	e.mapping = make(map[string]string)
	for _, name := range val {
		e.mapping[name] = name
	}
	if r.enums == nil {
		r.enums = make(map[string]Enum)
	}
	r.enums[code] = &e
}

func (r *Registry) RegisterMySQLPool(dataSourceName string, code ...string) {
	r.registerSQLPool(dataSourceName, code...)
}

func (r *Registry) RegisterElastic(url string, code ...string) {
	r.registerElastic(url, false, code...)
}

func (r *Registry) RegisterElasticWithTraceLog(url string, code ...string) {
	r.registerElastic(url, true, code...)
}

func (r *Registry) RegisterLocalCache(size int, code ...string) {
	dbCode := "default"
	if len(code) > 0 {
		dbCode = code[0]
	}
	if r.localCachePools == nil {
		r.localCachePools = make(map[string]LocalCachePoolConfig)
	}
	r.localCachePools[dbCode] = &localCachePoolConfig{code: dbCode, limit: size}
}

func (r *Registry) RegisterRedis(address string, db int, code ...string) {
	client := redis.NewClient(&redis.Options{
		Addr:       address,
		DB:         db,
		MaxConnAge: time.Minute * 2,
	})
	r.registerRedis(client, code, address, db)
}

func (r *Registry) RegisterRedisSentinel(masterName string, db int, sentinels []string, code ...string) {
	client := redis.NewFailoverClient(&redis.FailoverOptions{
		MasterName:    masterName,
		SentinelAddrs: sentinels,
		DB:            db,
		MaxConnAge:    time.Minute * 2,
	})
	r.registerRedis(client, code, fmt.Sprintf("%v", sentinels), db)
}

func (r *Registry) RegisterRedisStream(name string, redisPool string, groups []string) {
	if r.redisStreamGroups == nil {
		r.redisStreamGroups = make(map[string]map[string]map[string]bool)
		r.redisStreamPools = make(map[string]string)
	}
	_, has := r.redisStreamPools[name]
	if has {
		panic(fmt.Errorf("stream with name %s aleady exists", name))
	}
	r.redisStreamPools[name] = redisPool
	if r.redisStreamGroups[redisPool] == nil {
		r.redisStreamGroups[redisPool] = make(map[string]map[string]bool)
	}
	groupsMap := make(map[string]bool, len(groups))
	for _, group := range groups {
		groupsMap[group] = true
	}
	r.redisStreamGroups[redisPool][name] = groupsMap
}

func (r *Registry) registerSQLPool(dataSourceName string, code ...string) {
	dbCode := "default"
	if len(code) > 0 {
		dbCode = code[0]
	}
	and := "?"
	if strings.Index(dataSourceName, "?") > 0 {
		and = "&"
	}
	dataSourceName += and + "multiStatements=true"
	db := &mySQLPoolConfig{code: dbCode, dataSourceName: dataSourceName}
	if r.mysqlPools == nil {
		r.mysqlPools = make(map[string]MySQLPoolConfig)
	}
	parts := strings.Split(dataSourceName, "/")
	dbName := strings.Split(parts[len(parts)-1], "?")[0]

	pos := strings.Index(dataSourceName, "limit_connections=")
	if pos > 0 {
		val := dataSourceName[pos+18:]
		val = strings.Split(val, "&")[0]
		db.maxConnections, _ = strconv.Atoi(val)
		dataSourceName = strings.Replace(dataSourceName, "limit_connections="+val, "", -1)
		dataSourceName = strings.Trim(dataSourceName, "?&")
		dataSourceName = strings.Replace(dataSourceName, "?&", "?", -1)
		db.dataSourceName = dataSourceName
	}
	db.databaseName = dbName
	r.mysqlPools[dbCode] = db
}

func (r *Registry) RegisterClickHouse(url string, code ...string) {
	dbCode := "default"
	if len(code) > 0 {
		dbCode = code[0]
	}
	db := &ClickHouseConfig{code: dbCode, url: url}
	if r.clickHouseClients == nil {
		r.clickHouseClients = make(map[string]*ClickHouseConfig)
	}
	r.clickHouseClients[dbCode] = db
}

func (r *Registry) registerElastic(url string, withTrace bool, code ...string) {
	clientOptions := []elastic.ClientOptionFunc{elastic.SetSniff(false), elastic.SetURL(url),
		elastic.SetHealthcheckInterval(5 * time.Second), elastic.SetRetrier(elastic.NewBackoffRetrier(elastic.NewExponentialBackoff(10*time.Millisecond, 5*time.Second)))}
	if withTrace {
		clientOptions = append(clientOptions, elastic.SetTraceLog(log2.New(os.Stdout, "", log2.LstdFlags)))
	}
	client, err := elastic.NewClient(
		clientOptions...,
	)
	checkError(err)
	dbCode := "default"
	if len(code) > 0 {
		dbCode = code[0]
	}
	config := &ElasticConfig{code: dbCode, client: client}
	if r.elasticServers == nil {
		r.elasticServers = make(map[string]*ElasticConfig)
	}
	r.elasticServers[dbCode] = config
}

func (r *Registry) registerRedis(client *redis.Client, code []string, address string, db int) {
	dbCode := "default"
	if len(code) > 0 {
		dbCode = code[0]
	}
	redisCache := &redisCacheConfig{code: dbCode, client: client, address: address, db: db}
	if r.redisPools == nil {
		r.redisPools = make(map[string]RedisPoolConfig)
	}
	r.redisPools[dbCode] = redisCache
}

type RedisPoolConfig interface {
	GetCode() string
	GetDB() int
	GetAddress() string
	getClient() *redis.Client
}

type redisCacheConfig struct {
	code    string
	client  *redis.Client
	db      int
	address string
}

func (p *redisCacheConfig) GetCode() string {
	return p.code
}

func (p *redisCacheConfig) GetDB() int {
	return p.db
}

func (p *redisCacheConfig) GetAddress() string {
	return p.address
}

func (p *redisCacheConfig) getClient() *redis.Client {
	return p.client
}

type ElasticConfig struct {
	code   string
	client *elastic.Client
}
