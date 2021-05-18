package orm

import (
	"database/sql"
	"fmt"
	"reflect"
	"strconv"
)

func searchIDsWithCount(skipFakeDelete bool, engine *Engine, where *Where, pager *Pager, entityType reflect.Type) (results []uint64, totalRows int) {
	return searchIDs(skipFakeDelete, engine, where, pager, true, entityType)
}

func prepareScan(schema *tableSchema) (pointers []interface{}) {
	count := len(schema.columnNames)
	pointers = make([]interface{}, count)
	prepareScanForFields(schema.fields, 0, pointers)
	return pointers
}

func prepareScanForFields(fields *tableFields, start int, pointers []interface{}) int {
	for i := 0; i < len(fields.uintegers); i++ {
		v := uint64(0)
		pointers[start] = &v
		start++
	}
	for i := 0; i < len(fields.integers); i++ {
		v := int64(0)
		pointers[start] = &v
		start++
	}
	for i := 0; i < len(fields.booleans); i++ {
		v := false
		pointers[start] = &v
		start++
	}
	for i := 0; i < len(fields.floats); i++ {
		v := float64(0)
		pointers[start] = &v
		start++
	}
	for i := 0; i < len(fields.times); i++ {
		v := ""
		pointers[start] = &v
		start++
	}
	for i := 0; i < len(fields.uintegersNullable); i++ {
		v := sql.NullInt64{}
		pointers[start] = &v
		start++
	}
	for i := 0; i < len(fields.integersNullable); i++ {
		v := sql.NullInt64{}
		pointers[start] = &v
		start++
	}
	for i := 0; i < len(fields.stringsEnums); i++ {
		v := sql.NullString{}
		pointers[start] = &v
		start++
	}
	for i := 0; i < len(fields.strings); i++ {
		v := sql.NullString{}
		pointers[start] = &v
		start++
	}
	for i := 0; i < len(fields.sliceStringsSets); i++ {
		v := sql.NullString{}
		pointers[start] = &v
		start++
	}
	for i := 0; i < len(fields.bytes); i++ {
		v := sql.NullString{}
		pointers[start] = &v
		start++
	}
	if fields.fakeDelete > 0 {
		v := uint64(0)
		pointers[start] = &v
		start++
	}
	for i := 0; i < len(fields.booleansNullable); i++ {
		v := sql.NullBool{}
		pointers[start] = &v
		start++
	}
	for i := 0; i < len(fields.floatsNullable); i++ {
		v := sql.NullFloat64{}
		pointers[start] = &v
		start++
	}
	for i := 0; i < len(fields.timesNullable); i++ {
		v := sql.NullString{}
		pointers[start] = &v
		start++
	}
	for i := 0; i < len(fields.jsons); i++ {
		v := sql.NullString{}
		pointers[start] = &v
		start++
	}
	for i := 0; i < len(fields.refs); i++ {
		v := sql.NullInt64{}
		pointers[start] = &v
		start++
	}
	for i := 0; i < len(fields.refsMany); i++ {
		v := sql.NullString{}
		pointers[start] = &v
		start++
	}
	for _, subFields := range fields.structs {
		start = prepareScanForFields(subFields, start, pointers)
	}
	return start
}

func searchRow(skipFakeDelete bool, engine *Engine, where *Where, entity Entity, lazy bool, references []string) (bool, *tableSchema, []interface{}) {
	orm := initIfNeeded(engine.registry, entity)
	schema := orm.tableSchema
	whereQuery := where.String()
	if skipFakeDelete && schema.hasFakeDelete {
		whereQuery = "`FakeDelete` = 0 AND " + whereQuery
	}
	/* #nosec */
	query := "SELECT " + schema.fieldsQuery + " FROM `" + schema.tableName + "` WHERE " + whereQuery + " LIMIT 1"

	pool := schema.GetMysql(engine)
	results, def := pool.Query(query, where.GetParameters()...)
	defer def()
	if !results.Next() {
		return false, schema, nil
	}
	pointers := prepareScan(schema)
	results.Scan(pointers...)
	def()
	id := pointers[0].(uint64)
	fillFromDBRow(id, engine, pointers, entity, true, lazy)
	if len(references) > 0 {
		warmUpReferences(engine, schema, entity.getORM().value, references, false, lazy)
	}
	return true, schema, pointers
}

func search(skipFakeDelete bool, engine *Engine, where *Where, pager *Pager, withCount, lazy, checkIsSlice bool, entities reflect.Value, references ...string) (totalRows int) {
	if pager == nil {
		pager = NewPager(1, 50000)
	}
	entities.SetLen(0)
	entityType, has, name := getEntityTypeForSlice(engine.registry, entities.Type(), checkIsSlice)
	if !has {
		panic(fmt.Errorf("entity '%s' is not registered", name))
	}
	schema := getTableSchema(engine.registry, entityType)
	whereQuery := where.String()
	if skipFakeDelete && schema.hasFakeDelete {
		whereQuery = "`FakeDelete` = 0 AND " + whereQuery
	}
	/* #nosec */
	pageStart := strconv.Itoa((pager.CurrentPage - 1) * pager.PageSize)
	pageEnd := strconv.Itoa(pager.PageSize)
	query := "SELECT " + schema.fieldsQuery + " FROM `" + schema.tableName + "` WHERE " + whereQuery + " LIMIT " + pageStart + "," + pageEnd
	pool := schema.GetMysql(engine)
	results, def := pool.Query(query, where.GetParameters()...)
	defer def()

	valOrigin := entities
	val := valOrigin
	i := 0
	for results.Next() {
		pointers := prepareScan(schema)
		results.Scan(pointers...)
		value := reflect.New(entityType)
		id := pointers[0].(uint64)
		fillFromDBRow(id, engine, pointers, value.Interface().(Entity), true, lazy)
		val = reflect.Append(val, value)
		i++
	}
	def()
	totalRows = getTotalRows(engine, withCount, pager, where, schema, i)
	if len(references) > 0 && i > 0 {
		warmUpReferences(engine, schema, val, references, true, lazy)
	}
	valOrigin.Set(val)
	return totalRows
}

func searchOne(skipFakeDelete bool, engine *Engine, where *Where, entity Entity, lazy bool, references []string) (bool, *tableSchema, []interface{}) {
	return searchRow(skipFakeDelete, engine, where, entity, lazy, references)
}

func searchIDs(skipFakeDelete bool, engine *Engine, where *Where, pager *Pager, withCount bool, entityType reflect.Type) (ids []uint64, total int) {
	if pager == nil {
		pager = NewPager(1, 50000)
	}
	schema := getTableSchema(engine.registry, entityType)
	whereQuery := where.String()
	if skipFakeDelete && schema.hasFakeDelete {
		/* #nosec */
		whereQuery = "`FakeDelete` = 0 AND " + whereQuery
	}
	/* #nosec */
	startPage := strconv.Itoa((pager.CurrentPage - 1) * pager.PageSize)
	endPage := strconv.Itoa(pager.PageSize)
	query := "SELECT `ID` FROM `" + schema.tableName + "` WHERE " + whereQuery + " LIMIT " + startPage + "," + endPage
	pool := schema.GetMysql(engine)
	results, def := pool.Query(query, where.GetParameters()...)
	defer def()
	result := make([]uint64, 0)
	for results.Next() {
		var row uint64
		results.Scan(&row)
		result = append(result, row)
	}
	def()
	totalRows := getTotalRows(engine, withCount, pager, where, schema, len(result))
	return result, totalRows
}

func getTotalRows(engine *Engine, withCount bool, pager *Pager, where *Where, schema *tableSchema, foundRows int) int {
	totalRows := 0
	if withCount {
		totalRows = foundRows
		if totalRows == pager.GetPageSize() || (foundRows == 0 && pager.CurrentPage > 1) {
			/* #nosec */
			query := "SELECT count(1) FROM `" + schema.tableName + "` WHERE " + where.String()
			var foundTotal string
			pool := schema.GetMysql(engine)
			pool.QueryRow(NewWhere(query, where.GetParameters()...), &foundTotal)
			totalRows, _ = strconv.Atoi(foundTotal)
		} else {
			totalRows += (pager.GetCurrentPage() - 1) * pager.GetPageSize()
		}
	}
	return totalRows
}

func fillFromDBRow(id uint64, engine *Engine, pointers []interface{}, entity Entity, fillDataLoader bool, lazy bool) {
	orm := initIfNeeded(engine.registry, entity)
	orm.idElem.SetUint(id)
	orm.inDB = true
	orm.loaded = true
	orm.lazy = lazy
	orm.deserializeFromDB(engine, pointers)
	if !lazy {
		orm.deserialize(engine)
	}
	if !fillDataLoader {
		return
	}
	schema := entity.getORM().tableSchema
	if !schema.hasLocalCache && engine.dataLoader != nil {
		engine.dataLoader.Prime(schema, id, orm.binary)
	}
}

func getEntityTypeForSlice(registry *validatedRegistry, sliceType reflect.Type, checkIsSlice bool) (reflect.Type, bool, string) {
	name := sliceType.String()
	if name[0] == 42 {
		name = name[1:]
	}
	if name[0] == 91 {
		name = name[3:]
	} else if checkIsSlice {
		panic(fmt.Errorf("interface %s is no slice of orm.Entity", sliceType.String()))
	}
	e, has := registry.entities[name]
	return e, has, name
}
