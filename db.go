package orm

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"
)

type MySQLConfig interface {
	GetCode() string
	GetDatabaseName() string
	GetDataSourceURI() string
	GetOptions() *MySQLOptions
	getClient() *sql.DB
}

type mySQLConfig struct {
	dataSourceName string
	code           string
	databaseName   string
	client         *sql.DB
	options        *MySQLOptions
}

func (p *mySQLConfig) GetCode() string {
	return p.code
}

func (p *mySQLConfig) GetDatabaseName() string {
	return p.databaseName
}

func (p *mySQLConfig) GetDataSourceURI() string {
	return p.dataSourceName
}

func (p *mySQLConfig) getClient() *sql.DB {
	return p.client
}

func (p *mySQLConfig) GetOptions() *MySQLOptions {
	return p.options
}

type ExecResult interface {
	LastInsertId() uint64
	RowsAffected() uint64
}

type PreparedStmt interface {
	Exec(ctx Context, args ...any) ExecResult
	Query(ctx Context, args ...any) (rows Rows, close func())
	QueryRow(ctx Context, args []any, toFill ...any) (found bool)
	Close() error
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

type sqlClientBase interface {
	Prepare(query string) (*sql.Stmt, error)
	Exec(query string, args ...any) (sql.Result, error)
	ExecContext(context context.Context, query string, args ...any) (sql.Result, error)
	QueryRow(query string, args ...any) SQLRow
	QueryRowContext(context context.Context, query string, args ...any) SQLRow
	Query(query string, args ...any) (SQLRows, error)
	QueryContext(context context.Context, query string, args ...any) (SQLRows, error)
}

type sqlClient interface {
	sqlClientBase
	Begin() (*sql.Tx, error)
}

type txClient interface {
	sqlClientBase
	Commit() error
	Rollback() error
}

type DBClientQuery interface {
	Prepare(query string) (*sql.Stmt, error)
	Exec(query string, args ...any) (sql.Result, error)
	ExecContext(context context.Context, query string, args ...any) (sql.Result, error)
	QueryRow(query string, args ...any) *sql.Row
	QueryRowContext(context context.Context, query string, args ...any) *sql.Row
	Query(query string, args ...any) (*sql.Rows, error)
	QueryContext(context context.Context, query string, args ...any) (*sql.Rows, error)
}

type DBClient interface {
	DBClientQuery
}

type DBClientNoTX interface {
	DBClientQuery
	Begin() (*sql.Tx, error)
}

type TXClient interface {
	DBClientQuery
}

type standardSQLClient struct {
	db DBClient
}

type txSQLClient struct {
	standardSQLClient
	tx *sql.Tx
}

func (tx *txSQLClient) Commit() error {
	return tx.tx.Commit()
}

func (tx *txSQLClient) Rollback() error {
	return tx.tx.Rollback()
}

func (db *standardSQLClient) Prepare(query string) (*sql.Stmt, error) {
	res, err := db.db.Prepare(query)
	if err != nil {
		return nil, err
	}
	return res, nil
}

func (db *standardSQLClient) Exec(query string, args ...any) (sql.Result, error) {
	res, err := db.db.Exec(query, args...)
	if err != nil {
		return nil, err
	}
	return res, nil
}

func (db *standardSQLClient) Begin() (*sql.Tx, error) {
	return db.db.(DBClientNoTX).Begin()
}

func (db *standardSQLClient) ExecContext(context context.Context, query string, args ...any) (sql.Result, error) {
	res, err := db.db.ExecContext(context, query, args...)
	if err != nil {
		return nil, err
	}
	return res, nil
}

func (db *standardSQLClient) QueryRow(query string, args ...any) SQLRow {
	return db.db.QueryRow(query, args...)
}

func (db *standardSQLClient) QueryRowContext(context context.Context, query string, args ...any) SQLRow {
	return db.db.QueryRowContext(context, query, args...)
}

func (db *standardSQLClient) Query(query string, args ...any) (SQLRows, error) {
	rows, err := db.db.Query(query, args...)
	if err != nil {
		return nil, err
	}
	return rows, nil
}

func (db *standardSQLClient) QueryContext(context context.Context, query string, args ...any) (SQLRows, error) {
	rows, err := db.db.QueryContext(context, query, args...)
	if err != nil {
		return nil, err
	}
	return rows, nil
}

type SQLRows interface {
	Next() bool
	Err() error
	Close() error
	Scan(dest ...any) error
	Columns() ([]string, error)
}

type Rows interface {
	Next() bool
	Scan(dest ...any)
	Columns() []string
}

type rowsStruct struct {
	sqlRows SQLRows
}

func (r *rowsStruct) Next() bool {
	has := r.sqlRows.Next()
	if !has {
		_ = r.sqlRows.Close()
	}
	return has
}

func (r *rowsStruct) Columns() []string {
	columns, err := r.sqlRows.Columns()
	checkError(err)
	return columns
}

func (r *rowsStruct) Scan(dest ...any) {
	err := r.sqlRows.Scan(dest...)
	checkError(err)
}

type preparedStmtStruct struct {
	stmt  *sql.Stmt
	db    *dbImplementation
	query string
}

func (p preparedStmtStruct) Exec(ctx Context, args ...any) ExecResult {
	hasLogger, _ := ctx.getDBLoggers()
	start := getNow(hasLogger)
	rows, err := p.stmt.Exec(args...)
	if hasLogger {
		message := p.query
		if len(args) > 0 {
			message += " " + fmt.Sprintf("%v", args)
		}
		p.db.fillLogFields(ctx, "PREPARED EXEC", message, start, err)
	}
	checkError(err)
	return &execResult{r: rows}
}

func (p preparedStmtStruct) Query(ctx Context, args ...any) (rows Rows, close func()) {
	hasLogger, _ := ctx.getDBLoggers()
	start := getNow(hasLogger)
	result, err := p.stmt.Query(args...)
	if hasLogger {
		message := p.query
		if len(args) > 0 {
			message += " " + fmt.Sprintf("%v", args)
		}
		p.db.fillLogFields(ctx, "SELECT PREPARED", message, start, err)
	}
	checkError(err)
	return &rowsStruct{result}, func() {
		if result != nil {
			err := result.Err()
			_ = result.Close()
			checkError(err)
		}
	}
}

func (p preparedStmtStruct) QueryRow(ctx Context, args []any, toFill ...any) (found bool) {
	hasLogger, _ := ctx.getDBLoggers()
	start := getNow(hasLogger)
	row := p.stmt.QueryRow(args...)
	err := row.Scan(toFill...)
	message := ""
	if hasLogger {
		message = p.query
		if len(args) > 0 {
			message += " " + fmt.Sprintf("%v", args)
		}
	}
	if err != nil {
		if err.Error() == "sql: no rows in result set" {
			if hasLogger {
				p.db.fillLogFields(ctx, "SELECT PREPARED", message, start, nil)
			}
			return false
		}
		if hasLogger {
			p.db.fillLogFields(ctx, "SELECT PREPARED", message, start, err)
		}
		checkError(err)
	}
	if hasLogger {
		p.db.fillLogFields(ctx, "SELECT PREPARED", message, start, nil)
	}
	return true
}

func (p preparedStmtStruct) Close() error {
	return p.stmt.Close()
}

type SQLRow interface {
	Scan(dest ...any) error
}

type DBBase interface {
	GetConfig() MySQLConfig
	GetDBClient() DBClient
	SetMockDBClient(mock DBClient)
	Prepare(ctx Context, query string) (stmt PreparedStmt, close func())
	Exec(ctx Context, query string, args ...any) ExecResult
	QueryRow(ctx Context, query Where, toFill ...any) (found bool)
	Query(ctx Context, query string, args ...any) (rows Rows, close func())
}

type DB interface {
	DBBase
	Begin(ctx Context) DBTransaction
}

type DBTransaction interface {
	DBBase
	Commit(ctx Context)
	Rollback(ctx Context)
}

type dbImplementation struct {
	client      sqlClient
	config      MySQLConfig
	transaction bool
}

func (db *dbImplementation) GetConfig() MySQLConfig {
	return db.config
}

func (db *dbImplementation) Commit(ctx Context) {
	if !db.transaction {
		return
	}
	hasLogger, _ := ctx.getDBLoggers()
	start := getNow(hasLogger)
	err := db.client.(txClient).Commit()
	db.client.(*txSQLClient).db = nil
	db.client.(*txSQLClient).tx = nil
	db.transaction = false
	if hasLogger {
		db.fillLogFields(ctx, "TRANSACTION", "COMMIT", start, err)
	}
	checkError(err)
}

func (db *dbImplementation) Rollback(ctx Context) {
	if !db.transaction {
		return
	}
	hasLogger, _ := ctx.getDBLoggers()
	start := getNow(hasLogger)
	err := db.client.(txClient).Rollback()
	db.transaction = false
	if hasLogger {
		db.fillLogFields(ctx, "TRANSACTION", "ROLLBACK", start, err)
	}
	checkError(err)
}

func (db *dbImplementation) Begin(ctx Context) DBTransaction {
	hasLogger, _ := ctx.getDBLoggers()
	start := getNow(hasLogger)
	tx, err := db.client.Begin()
	if hasLogger {
		db.fillLogFields(ctx, "TRANSACTION", "START TRANSACTION", start, err)
	}
	checkError(err)
	dbTX := &dbImplementation{config: db.config, client: &txSQLClient{standardSQLClient{db: tx}, tx}, transaction: true}
	return dbTX
}

func (db *dbImplementation) GetDBClient() DBClient {
	return db.client.(*standardSQLClient).db
}

func (db *dbImplementation) SetMockDBClient(mock DBClient) {
	db.client.(*standardSQLClient).db = mock
}

func (db *dbImplementation) Prepare(ctx Context, query string) (stmt PreparedStmt, close func()) {
	hasLogger, _ := ctx.getDBLoggers()
	start := getNow(hasLogger)
	result, err := db.client.Prepare(query)
	if hasLogger {
		message := query
		db.fillLogFields(ctx, "PREPARE", message, start, err)
	}
	checkError(err)
	return &preparedStmtStruct{result, db, query}, func() {
		if result != nil {
			_ = result.Close()
		}
	}
}

func (db *dbImplementation) Exec(ctx Context, query string, args ...any) ExecResult {
	results, err := db.exec(ctx, query, args...)
	checkError(err)
	return results
}

func (db *dbImplementation) exec(ctx Context, query string, args ...any) (ExecResult, error) {
	hasLogger, _ := ctx.getDBLoggers()
	start := getNow(hasLogger)
	rows, err := db.client.Exec(query, args...)
	if hasLogger {
		message := query
		if len(args) > 0 {
			message += " " + fmt.Sprintf("%v", args)
		}
		db.fillLogFields(ctx, "EXEC", message, start, err)
	}
	return &execResult{r: rows}, err
}

func (db *dbImplementation) QueryRow(ctx Context, query Where, toFill ...any) (found bool) {
	hasLogger, _ := ctx.getDBLoggers()
	start := getNow(hasLogger)
	row := db.client.QueryRow(query.String(), query.GetParameters()...)
	err := row.Scan(toFill...)
	message := ""
	if hasLogger {
		message = query.String()
		if len(query.GetParameters()) > 0 {
			message += " " + fmt.Sprintf("%v", query.GetParameters())
		}
	}
	if err != nil {
		if err.Error() == "sql: no rows in result set" {
			if hasLogger {
				db.fillLogFields(ctx, "SELECT", message, start, nil)
			}
			return false
		}
		if hasLogger {
			db.fillLogFields(ctx, "SELECT", message, start, err)
		}
		panic(err)
	}
	if hasLogger {
		db.fillLogFields(ctx, "SELECT", message, start, nil)
	}
	return true
}

func (db *dbImplementation) Query(ctx Context, query string, args ...any) (rows Rows, close func()) {
	hasLogger, _ := ctx.getDBLoggers()
	start := getNow(hasLogger)
	result, err := db.client.Query(query, args...)
	if hasLogger {
		message := query
		if len(args) > 0 {
			message += " " + fmt.Sprintf("%v", args)
		}
		db.fillLogFields(ctx, "SELECT", message, start, err)
	}
	checkError(err)
	return &rowsStruct{result}, func() {
		if result != nil {
			err := result.Err()
			_ = result.Close()
			checkError(err)
		}
	}
}

func (db *dbImplementation) fillLogFields(ctx Context, operation, query string, start *time.Time, err error) {
	query = strings.ReplaceAll(query, "\n", " ")
	_, loggers := ctx.getDBLoggers()
	fillLogFields(ctx, loggers, db.GetConfig().GetCode(), sourceMySQL, operation, query, start, false, err)
}
