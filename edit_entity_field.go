package orm

import (
	"hash/maphash"
	"reflect"

	"github.com/puzpuzpuz/xsync/v2"
)

func EditEntityField(ctx Context, entity any, field string, value any) error {
	return editEntityField(ctx, entity, field, value)
}

func editEntityField(ctx Context, entity any, field string, value any) error {
	schema := getEntitySchemaFromSource(ctx, entity)
	setter, has := schema.fieldBindSetters[field]
	if !has {
		return &BindError{field, "unknown field"}
	}
	newValue, err := setter(value)
	if err != nil {
		return err
	}
	reflectValue := reflect.ValueOf(entity)
	elem := reflectValue.Elem()
	getter := schema.fieldGetters[field]
	v := getter(elem)
	oldValue, err := setter(v)
	if err == nil && oldValue == newValue {
		return nil
	}
	id := elem.Field(0).Uint()
	cImplementation := ctx.(*ormImplementation)
	var asyncError error
	func() {
		cImplementation.mutexFlush.Lock()
		defer cImplementation.mutexFlush.Unlock()
		if cImplementation.trackedEntities == nil {
			cImplementation.trackedEntities = xsync.NewTypedMapOf[uint64, *xsync.MapOf[uint64, EntityFlush]](func(seed maphash.Seed, u uint64) uint64 {
				return u
			})
		}
		entities, _ := cImplementation.trackedEntities.LoadOrCompute(schema.index, func() *xsync.MapOf[uint64, EntityFlush] {
			return xsync.NewTypedMapOf[uint64, EntityFlush](func(seed maphash.Seed, u uint64) uint64 {
				return u
			})
		})
		actual, loaded := entities.LoadOrCompute(id, func() EntityFlush {
			editable := &editableFields{}
			editable.ctx = ctx
			editable.schema = schema
			editable.id = id
			editable.value = reflectValue
			editable.newBind = Bind{field: newValue}
			editable.oldBind = Bind{field: oldValue}
			addUniqueIndexFieldsToBind(schema, field, editable.oldBind, editable.newBind, elem)
			return editable
		})
		if loaded {
			editable, is := actual.(*editableFields)
			if is {
				editable.newBind[field] = newValue
				editable.oldBind[field] = oldValue
				addUniqueIndexFieldsToBind(schema, field, editable.oldBind, editable.newBind, elem)
				return
			}
			fSetter := schema.fieldSetters[field]
			editableE, is := actual.(*editableEntity)
			if is {
				fSetter(newValue, editableE.value.Elem())
				return
			}
			insertableE, is := actual.(*insertableEntity)
			if is {
				fSetter(newValue, insertableE.value.Elem())
				return
			}
			asyncError = &BindError{Field: field, Message: "setting field in entity marked to delete not allowed"}
		}
	}()
	return asyncError
}

func addUniqueIndexFieldsToBind(schema *entitySchema, field string, oldBind, newBind Bind, elem reflect.Value) {
	uniqueIndexes := schema.GetUniqueIndexes()
	for _, indexColumns := range uniqueIndexes {
		for _, indexColumn := range indexColumns {
			if indexColumn == field {
				for _, indexColumnToAdd := range indexColumns {
					if oldBind[indexColumnToAdd] == nil {
						setter, _ := schema.fieldBindSetters[indexColumnToAdd]
						val, _ := setter(elem.FieldByName(indexColumnToAdd).Interface())
						newBind[indexColumnToAdd] = val
						oldBind[indexColumnToAdd] = val
					}
				}
				break
			}
		}
	}
}
