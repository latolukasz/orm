package orm

import (
	"reflect"
	"testing"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
)

type testPluginEntity struct {
	ID   uint64
	Name string
}

type testPluginToTest struct {
	option    int
	lastValue any
}

func (p *testPluginToTest) ValidateRegistry(engine EngineSetter, registry Registry) error {
	if p.option == 1 {
		return errors.New("error 1")
	}
	engine.SetOption("ValidateRegistry", "a")
	registry.SetOption("ValidateRegistry", "a2")
	return nil
}

func (p *testPluginToTest) InitRegistryFromYaml(registry Registry, yaml map[string]any) error {
	if p.option == 2 {
		return errors.New("error 2")
	}
	p.lastValue = yaml
	registry.SetOption("InitRegistryFromYaml", "b")
	return nil
}

func (p *testPluginToTest) ValidateEntitySchema(schema EntitySchemaSetter) error {
	if p.option == 3 {
		return errors.New("error 3")
	}
	schema.SetOption("ValidateEntitySchema", "c")
	return nil
}

func (p *testPluginToTest) EntityFlush(schema EntitySchema, entity reflect.Value, before, after Bind, engine Engine) (PostFlushAction, error) {
	if p.option == 4 {
		return nil, errors.New("error 4")
	}
	p.lastValue = []any{schema, entity, before, after, engine}
	if p.option == 5 {
		after["Name"] = "a1"
		return func(_ Context) {
			entity.FieldByName("Name").SetString("a1")
		}, nil
	} else if p.option == 6 {
		after["Name"] = "b1"
		return func(_ Context) {
			entity.FieldByName("Name").SetString("b1")
		}, nil
	} else if p.option == 7 {
		return func(_ Context) {
			p.option = 100
		}, nil
	}
	return nil, nil
}

func TestPlugin(t *testing.T) {
	registry := NewRegistry()
	registry.RegisterPlugin(&testPluginToTest{})
	orm := PrepareTables(t, registry)
	assert.Equal(t, "a", orm.Engine().Option("ValidateRegistry"))
	assert.Equal(t, "a2", orm.Engine().Registry().Option("ValidateRegistry"))

	registry = NewRegistry()
	registry.RegisterPlugin(&testPluginToTest{option: 1})
	_, err := registry.Validate()
	assert.EqualError(t, err, "error 1")

	registry = NewRegistry()
	p := &testPluginToTest{}
	registry.RegisterPlugin(p)
	yaml := map[string]any{"orm": map[string]any{"local_cache": 200}}
	err = registry.InitByYaml(yaml)
	assert.Equal(t, yaml, p.lastValue)
	assert.NoError(t, err)
	engine, err := registry.Validate()
	assert.NoError(t, err)
	assert.Equal(t, "b", engine.Registry().Option("InitRegistryFromYaml"))

	registry = NewRegistry()
	registry.RegisterPlugin(&testPluginToTest{option: 2})
	err = registry.InitByYaml(yaml)
	assert.EqualError(t, err, "error 2")

	registry = NewRegistry()
	registry.RegisterPlugin(&testPluginToTest{})
	orm = PrepareTables(t, registry, testPluginEntity{})
	schema := GetEntitySchema[testPluginEntity](orm)
	assert.Equal(t, "c", schema.Option("ValidateEntitySchema"))

	registry = NewRegistry()
	p = &testPluginToTest{option: 5}
	registry.RegisterPlugin(p)
	orm = PrepareTables(t, registry, testPluginEntity{})
	entity := NewEntity[testPluginEntity](orm)
	entity.Name = "a"
	err = orm.Flush()
	assert.NoError(t, err)
	values := p.lastValue.([]any)
	assert.Len(t, values, 5)
	assert.Equal(t, schema.GetTableName(), values[0].(EntitySchema).GetTableName())
	assert.Nil(t, values[2])
	assert.NotNil(t, values[3])
	assert.Len(t, values[3], 2)
	assert.Equal(t, orm.Engine(), values[4])
	assert.Equal(t, "a1", entity.Name)
	entity, _ = GetByID[testPluginEntity](orm, entity.ID)
	assert.Equal(t, "a1", entity.Name)

	entity = NewEntity[testPluginEntity](orm)
	entity.Name = "b"
	err = orm.FlushAsync()
	err = runAsyncConsumer(orm, false)
	assert.NoError(t, err)
	values = p.lastValue.([]any)
	assert.Nil(t, values[2])
	assert.NotNil(t, values[3])
	assert.Len(t, values[3], 2)
	entity, _ = GetByID[testPluginEntity](orm, entity.ID)
	assert.Equal(t, "a1", entity.Name)

	p.option = 4
	entity = NewEntity[testPluginEntity](orm)
	entity.Name = "a"
	err = orm.Flush()
	assert.EqualError(t, err, "error 4")

	registry = NewRegistry()
	p = &testPluginToTest{option: 6}
	registry.RegisterPlugin(p)
	orm = PrepareTables(t, registry, testPluginEntity{})
	entity = NewEntity[testPluginEntity](orm)
	entity.Name = "a"
	err = orm.Flush()
	entity = EditEntity(orm, entity)
	entity.Name = "b"
	err = orm.Flush()
	assert.NoError(t, err)
	values = p.lastValue.([]any)
	assert.Len(t, values, 5)
	assert.Equal(t, schema.GetTableName(), values[0].(EntitySchema).GetTableName())
	assert.NotNil(t, values[2])
	assert.NotNil(t, values[3])
	assert.Len(t, values[2], 1)
	assert.Len(t, values[3], 1)
	assert.Equal(t, "b1", entity.Name)
	entity, _ = GetByID[testPluginEntity](orm, entity.ID)
	assert.Equal(t, "b1", entity.Name)

	entity = EditEntity(orm, entity)
	entity.Name = "c"
	err = orm.FlushAsync()
	err = runAsyncConsumer(orm, false)
	assert.NoError(t, err)
	values = p.lastValue.([]any)
	assert.NotNil(t, values[2])
	assert.NotNil(t, values[3])
	assert.Len(t, values[2], 1)
	assert.Len(t, values[3], 1)
	entity, _ = GetByID[testPluginEntity](orm, entity.ID)
	assert.Equal(t, "b1", entity.Name)

	p.lastValue = nil
	p.option = 0
	err = EditEntityField(orm, entity, "Name", "c2")
	assert.NoError(t, err)
	assert.NoError(t, orm.Flush())
	values = p.lastValue.([]any)
	assert.Len(t, values[2], 1)
	assert.Len(t, values[3], 1)
	assert.Equal(t, "b1", values[2].(Bind)["Name"])
	assert.Equal(t, "c2", values[3].(Bind)["Name"])

	p.option = 4
	entity = EditEntity(orm, entity)
	entity.Name = "d"
	err = orm.Flush()
	assert.EqualError(t, err, "error 4")

	p.option = 4
	entity = NewEntity[testPluginEntity](orm)
	entity.Name = "a"
	err = orm.Flush()
	assert.EqualError(t, err, "error 4")

	registry = NewRegistry()
	p = &testPluginToTest{option: 7}
	registry.RegisterPlugin(p)
	orm = PrepareTables(t, registry, testPluginEntity{})
	entity = NewEntity[testPluginEntity](orm)
	entity.Name = "a"
	err = orm.Flush()
	DeleteEntity(orm, entity)
	err = orm.Flush()
	assert.NoError(t, err)
	values = p.lastValue.([]any)
	assert.Len(t, values, 5)
	assert.Equal(t, schema.GetTableName(), values[0].(EntitySchema).GetTableName())
	assert.NotNil(t, values[2])
	assert.Nil(t, values[3])
	assert.Len(t, values[2], 2)
	entity, _ = GetByID[testPluginEntity](orm, entity.ID)
	assert.Nil(t, entity)
	assert.Equal(t, 100, p.option)

	entity = NewEntity[testPluginEntity](orm)
	entity.Name = "a"
	_ = orm.Flush()
	p.option = 4
	DeleteEntity(orm, entity)
	err = orm.Flush()
	assert.EqualError(t, err, "error 4")
}
