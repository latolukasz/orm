package orm

import (
	"database/sql"
	"regexp"
	"time"

	"github.com/go-sql-driver/mysql"

	log2 "github.com/apex/log"

	"github.com/pkg/errors"
)

type MySQLPoolConfig interface {
	GetCode() string
	GetDatabase() string
	GetDataSourceURI() string
	GetVersion() int
	getClient() *sql.DB
	getAutoincrement() uint64
	getMaxConnections() int
}

type mySQLPoolConfig struct {
	dataSourceName string
	code           string
	databaseName   string
	client         *sql.DB
	autoincrement  uint64
	version        int
	maxConnections int
}

func (p *mySQLPoolConfig) GetCode() string {
	return p.code
}

func (p *mySQLPoolConfig) GetDatabase() string {
	return p.databaseName
}

func (p *mySQLPoolConfig) GetDataSourceURI() string {
	return p.dataSourceName
}

func (p *mySQLPoolConfig) GetVersion() int {
	return p.version
}

func (p *mySQLPoolConfig) getClient() *sql.DB {
	return p.client
}

func (p *mySQLPoolConfig) getAutoincrement() uint64 {
	return p.autoincrement
}

func (p *mySQLPoolConfig) getMaxConnections() int {
	return p.maxConnections
}

type ExecResult interface {
	LastInsertId() uint64
	RowsAffected() uint64
}

type execResult struct {
	r sql.Result
}

func (e *execResult) LastInsertId() uint64 {
	id, err := e.r.LastInsertId()
	checkError(err)
	return uint64(id)
}

func (e *execResult) RowsAffected() uint64 {
	id, err := e.r.RowsAffected()
	checkError(err)
	return uint64(id)
}

type sqlClient interface {
	Begin() error
	Commit() error
	Rollback() (bool, error)
	Exec(query string, args ...interface{}) (sql.Result, error)
	QueryRow(query string, args ...interface{}) SQLRow
	Query(query string, args ...interface{}) (SQLRows, error)
}

type dbClientQuery interface {
	Exec(query string, args ...interface{}) (sql.Result, error)
	QueryRow(query string, args ...interface{}) *sql.Row
	Query(query string, args ...interface{}) (*sql.Rows, error)
}

type dbClient interface {
	dbClientQuery
	Begin() (*sql.Tx, error)
}

type dbClientTX interface {
	dbClientQuery
	Commit() error
	Rollback() error
}

type standardSQLClient struct {
	db dbClient
	tx dbClientTX
}

func (db *standardSQLClient) Begin() error {
	if db.tx != nil {
		return errors.New("transaction already started")
	}
	tx, err := db.db.Begin()
	if err != nil {
		return err
	}
	db.tx = tx
	return nil
}

func (db *standardSQLClient) Commit() error {
	if db.tx == nil {
		return errors.New("transaction not started")
	}
	err := db.tx.Commit()
	if err != nil {
		return err
	}
	db.tx = nil
	return nil
}

func (db *standardSQLClient) Rollback() (bool, error) {
	if db.tx == nil {
		return false, nil
	}
	err := db.tx.Rollback()
	if err != nil {
		return true, err
	}
	db.tx = nil
	return true, nil
}

func (db *standardSQLClient) Exec(query string, args ...interface{}) (sql.Result, error) {
	if db.tx != nil {
		res, err := db.tx.Exec(query, args...)
		if err != nil {
			return nil, err
		}
		return res, nil
	}
	res, err := db.db.Exec(query, args...)
	if err != nil {
		return nil, err
	}
	return res, nil
}

func (db *standardSQLClient) QueryRow(query string, args ...interface{}) SQLRow {
	if db.tx != nil {
		return db.tx.QueryRow(query, args...)
	}
	return db.db.QueryRow(query, args...)
}

func (db *standardSQLClient) Query(query string, args ...interface{}) (SQLRows, error) {
	if db.tx != nil {
		rows, err := db.tx.Query(query, args...)
		if err != nil {
			return nil, err
		}
		return rows, nil
	}
	rows, err := db.db.Query(query, args...)
	if err != nil {
		return nil, err
	}
	return rows, nil
}

type SQLRows interface {
	Next() bool
	Err() error
	Close() error
	Scan(dest ...interface{}) error
	Columns() ([]string, error)
}

type Rows interface {
	Next() bool
	Scan(dest ...interface{})
	Columns() []string
}

type rowsStruct struct {
	sqlRows SQLRows
}

func (r *rowsStruct) Next() bool {
	return r.sqlRows.Next()
}

func (r *rowsStruct) Columns() []string {
	columns, err := r.sqlRows.Columns()
	checkError(err)
	return columns
}

func (r *rowsStruct) Scan(dest ...interface{}) {
	err := r.sqlRows.Scan(dest...)
	checkError(err)
}

type SQLRow interface {
	Scan(dest ...interface{}) error
}

type DB struct {
	engine        *Engine
	client        sqlClient
	config        MySQLPoolConfig
	inTransaction bool
}

func (db *DB) GetPoolConfig() MySQLPoolConfig {
	return db.config
}

func (db *DB) Begin() {
	start := time.Now()
	err := db.client.Begin()
	if db.engine.hasDBLogger {
		db.fillLogFields("[ORM][MYSQL][BEGIN]", start, "transaction", "START TRANSACTION", nil, err)
	}
	checkError(err)
	db.inTransaction = true
}

func (db *DB) Commit() {
	start := time.Now()
	err := db.client.Commit()
	if db.engine.hasDBLogger {
		db.fillLogFields("[ORM][MYSQL][COMMIT]", start, "transaction", "COMMIT", nil, err)
	}
	checkError(err)
	db.inTransaction = false
	if db.engine.afterCommitLocalCacheSets != nil {
		for cacheCode, pairs := range db.engine.afterCommitLocalCacheSets {
			cache := db.engine.GetLocalCache(cacheCode)
			cache.MSet(pairs...)
		}
		db.engine.afterCommitLocalCacheSets = nil
	}

	if db.engine.afterCommitRedisFlusher != nil {
		db.engine.afterCommitRedisFlusher.Flush()
		db.engine.afterCommitRedisFlusher = nil
	}
}

func (db *DB) Rollback() {
	start := time.Now()
	has, err := db.client.Rollback()
	if has {
		if db.engine.hasDBLogger {
			db.fillLogFields("[ORM][MYSQL][ROLLBACK]", start, "transaction", "ROLLBACK", nil, err)
		}
	}
	checkError(err)
	db.engine.afterCommitLocalCacheSets = nil
	db.engine.afterCommitRedisFlusher = nil
	db.inTransaction = false
}

func (db *DB) Exec(query string, args ...interface{}) ExecResult {
	start := time.Now()
	rows, err := db.client.Exec(query, args...)
	if db.engine.hasDBLogger {
		db.fillLogFields("[ORM][MYSQL][EXEC]", start, "exec", query, args, err)
	}
	if err != nil {
		panic(db.convertToError(err))
	}
	return &execResult{r: rows}
}

func (db *DB) QueryRow(query *Where, toFill ...interface{}) (found bool) {
	start := time.Now()
	row := db.client.QueryRow(query.String(), query.GetParameters()...)
	err := row.Scan(toFill...)
	if err != nil {
		if err.Error() == "sql: no rows in result set" {
			if db.engine.hasDBLogger {
				db.fillLogFields("[ORM][MYSQL][SELECT]", start, "select", query.String(), query.GetParameters(), nil)
			}
			return false
		}
		if db.engine.hasDBLogger {
			db.fillLogFields("[ORM][MYSQL][SELECT]", start, "select", query.String(), query.GetParameters(), err)
		}
		panic(err)
	}
	if db.engine.hasDBLogger {
		db.fillLogFields("[ORM][MYSQL][SELECT]", start, "select", query.String(), query.GetParameters(), nil)
	}
	return true
}

func (db *DB) Query(query string, args ...interface{}) (rows Rows, deferF func()) {
	start := time.Now()
	result, err := db.client.Query(query, args...)
	if db.engine.hasDBLogger {
		db.fillLogFields("[ORM][MYSQL][SELECT]", start, "select", query, args, err)
	}
	checkError(err)
	return &rowsStruct{result}, func() {
		if result != nil {
			err := result.Err()
			checkError(err)
			err = result.Close()
			checkError(err)
		}
	}
}

func (db *DB) fillLogFields(message string, start time.Time, typeCode string, query string, args []interface{}, err error) {
	now := time.Now()
	stop := time.Since(start).Microseconds()
	e := db.engine.queryLoggers[QueryLoggerSourceDB].log.WithFields(log2.Fields{
		"pool":         db.GetPoolConfig().GetCode(),
		"db":           db.GetPoolConfig().GetDatabase(),
		"Query":        query,
		"microseconds": stop,
		"target":       "mysql",
		"type":         typeCode,
		"started":      start.UnixNano(),
		"finished":     now.UnixNano(),
	})
	if args != nil {
		e = e.WithField("args", args)
	}
	if err != nil {
		injectLogError(err, e).Error(message)
	} else {
		e.Info(message)
	}
}

func (db *DB) convertToError(err error) error {
	sqlErr, yes := err.(*mysql.MySQLError)
	if yes {
		if sqlErr.Number == 1062 {
			var abortLabelReg, _ = regexp.Compile(` for key '(.*?)'`)
			labels := abortLabelReg.FindStringSubmatch(sqlErr.Message)
			if len(labels) > 0 {
				return &DuplicatedKeyError{Message: sqlErr.Message, Index: labels[1]}
			}
		} else if sqlErr.Number == 1451 || sqlErr.Number == 1452 {
			var abortLabelReg, _ = regexp.Compile(" CONSTRAINT `(.*?)`")
			labels := abortLabelReg.FindStringSubmatch(sqlErr.Message)
			if len(labels) > 0 {
				return &ForeignKeyError{Message: "foreign key error in key `" + labels[1] + "`", Constraint: labels[1]}
			}
		}
	}
	return err
}
