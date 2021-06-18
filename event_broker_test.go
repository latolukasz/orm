package orm

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestRedisStreamGroupConsumerClean(t *testing.T) {
	registry := &Registry{}
	registry.RegisterRedis("localhost:6382", 15)
	registry.RegisterRedisStream("test-stream", "default", []string{"test-group-1", "test-group-2"})
	ctx := context.Background()
	validatedRegistry, err := registry.Validate(ctx)
	assert.NoError(t, err)
	engine := validatedRegistry.CreateEngine(ctx)
	engine.GetRedis().FlushDB()
	broker := engine.GetEventBroker()
	eventFlusher := engine.GetEventBroker().NewFlusher()
	for i := 1; i <= 10; i++ {
		eventFlusher.PublishMap("test-stream", EventAsMap{"name": fmt.Sprintf("a%d", i)})
	}
	eventFlusher.Flush()

	consumer1 := broker.Consumer("test-consumer", "test-group-1")
	consumer1.(*eventsConsumer).blockTime = time.Millisecond
	consumer1.(*eventsConsumer).garbageTick = time.Millisecond * 15
	consumer1.DisableLoop()
	consumer2 := broker.Consumer("test-consumer", "test-group-2")
	consumer2.(*eventsConsumer).blockTime = time.Millisecond
	consumer2.(*eventsConsumer).garbageTick = time.Millisecond * 15
	consumer2.DisableLoop()

	consumer1.Consume(1, true, func(events []Event) {})
	time.Sleep(time.Millisecond * 20)
	assert.Equal(t, int64(10), engine.GetRedis().XLen("test-stream"))

	consumer2.Consume(1, true, func(events []Event) {})
	time.Sleep(time.Millisecond * 20)
	consumer2.(*eventsConsumer).garbageCollector(engine)
	assert.Equal(t, int64(0), engine.GetRedis().XLen("test-stream"))
}

func TestRedisStreamGroupConsumerErrorHandler(t *testing.T) {
	registry := &Registry{}
	registry.RegisterRedis("localhost:6382", 15)
	registry.RegisterRedisStream("test-stream", "default", []string{"test-group"})
	ctx := context.Background()
	validatedRegistry, err := registry.Validate(ctx)
	assert.NoError(t, err)
	engine := validatedRegistry.CreateEngine(ctx)
	engine.GetRedis().FlushDB()
	broker := engine.GetEventBroker()

	consumer := broker.Consumer("test-consumer", "test-group")
	consumer.(*eventsConsumer).blockTime = time.Millisecond
	consumer.(*eventsConsumer).garbageTick = time.Millisecond * 15
	consumer.DisableLoop()

	eventFlusher := engine.GetEventBroker().NewFlusher()
	for i := 1; i <= 10; i++ {
		eventFlusher.PublishMap("test-stream", EventAsMap{"name": fmt.Sprintf("a%d", i)})
	}
	eventFlusher.Flush()
	assert.PanicsWithError(t, "test err a1", func() {
		consumer.Consume(1, true, func(events []Event) {
			panic(fmt.Errorf("test err %v", events[0].RawData()["name"]))
		})
	})
	assert.Equal(t, int64(10), engine.GetRedis().XLen("test-stream"))
	assert.Equal(t, int64(1), engine.GetRedis().XInfoGroups("test-stream")[0].Pending)
	i := 0
	consumer.Consume(1, true, func(events []Event) {
		i++
		assert.Equal(t, fmt.Sprintf("a%d", i), events[0].RawData()["name"])
		events[0].Skip()
	})
	assert.Equal(t, 10, i)
	assert.Equal(t, int64(10), engine.GetRedis().XLen("test-stream"))
	assert.Equal(t, int64(10), engine.GetRedis().XInfoGroups("test-stream")[0].Pending)

	j := 0
	consumer.SetErrorHandler(func(err interface{}, event Event) error {
		j++
		return nil
	})
	i = 0
	consumer.Consume(1, true, func(events []Event) {
		i++
		panic(fmt.Errorf("test err %v", events[0].RawData()["name"]))
	})
	time.Sleep(time.Millisecond * 20)
	consumer.(*eventsConsumer).garbageCollector(engine)
	assert.Equal(t, 20, i)
	assert.Equal(t, 10, j)
	assert.Equal(t, int64(10), engine.GetRedis().XLen("test-stream"))
	assert.Equal(t, int64(10), engine.GetRedis().XInfoGroups("test-stream")[0].Pending)

	j = 0
	consumer.SetErrorHandler(func(err interface{}, event Event) error {
		j++
		if j == 4 {
			j++
		}
		assert.Equal(t, fmt.Sprintf("a%d", j), event.RawData()["name"])
		return nil
	})
	i = 0
	consumer.Consume(10, true, func(events []Event) {
		i++
		if i == 1 {
			for k, e := range events {
				if k == 3 {
					e.Ack()
				}
			}
			panic(fmt.Errorf("test err %v", events[0].RawData()["name"]))
		} else {
			assert.Len(t, events, 1)
			if i == 5 {
				i++
			}
			assert.Equal(t, fmt.Sprintf("a%d", i-1), events[0].RawData()["name"])
			panic(fmt.Errorf("test err %v", events[0].RawData()["name"]))
		}
	})
	assert.Equal(t, 11, i)
	assert.Equal(t, 10, j)
	time.Sleep(time.Millisecond * 20)
	consumer.(*eventsConsumer).garbageCollector(engine)
	assert.Equal(t, int64(10), engine.GetRedis().XLen("test-stream"))
	assert.Equal(t, int64(9), engine.GetRedis().XInfoGroups("test-stream")[0].Pending)

	j = 0
	consumer.SetErrorHandler(func(err interface{}, event Event) error {
		j++
		return fmt.Errorf("strange error: %v", err)
	})
	assert.PanicsWithError(t, "strange error: test err a1", func() {
		consumer.Consume(1, true, func(events []Event) {
			panic(fmt.Errorf("test err %v", events[0].RawData()["name"]))
		})
	})
	assert.Equal(t, 1, j)
	assert.Equal(t, int64(10), engine.GetRedis().XLen("test-stream"))
	assert.Equal(t, int64(9), engine.GetRedis().XInfoGroups("test-stream")[0].Pending)
}

func TestRedisStreamGroupConsumerAutoScaled(t *testing.T) {
	registry := &Registry{}
	registry.RegisterRedis("localhost:6382", 15)
	registry.RegisterRedisStream("test-stream", "default", []string{"test-group"})
	ctx := context.Background()
	validatedRegistry, err := registry.Validate(ctx)
	assert.NoError(t, err)
	engine := validatedRegistry.CreateEngine(ctx)
	engine.GetRedis().FlushDB()
	broker := engine.GetEventBroker()

	consumer := broker.Consumer("test-consumer", "test-group")
	consumer.(*eventsConsumer).blockTime = time.Millisecond
	consumer.DisableLoop()
	consumer.Consume(1, true, func(events []Event) {})
	assert.Equal(t, 1, consumer.(*eventsConsumer).nr)
	consumer.Consume(1, true, func(events []Event) {
		for _, event := range events {
			event.Skip()
		}
	})
	assert.Equal(t, 1, consumer.(*eventsConsumer).nr)

	engine.GetRedis().FlushDB()
	for i := 1; i <= 10; i++ {
		engine.GetEventBroker().PublishMap("test-stream", EventAsMap{"name": fmt.Sprintf("a%d", i)})
	}
	iterations1 := false
	iterations2 := false
	wg := &sync.WaitGroup{}
	wg.Add(2)
	go func() {
		defer wg.Done()
		consumer := broker.Consumer("test-consumer", "test-group")
		consumer.(*eventsConsumer).blockTime = time.Millisecond
		consumer.DisableLoop()
		consumer.Consume(5, true, func(events []Event) {
			assert.Equal(t, 1, consumer.(*eventsConsumer).nr)
			iterations1 = true
			for _, event := range events {
				event.Skip()
			}
			time.Sleep(time.Millisecond * 100)
		})
	}()
	time.Sleep(time.Millisecond)
	go func() {
		defer wg.Done()
		consumer := broker.Consumer("test-consumer", "test-group")
		consumer.(*eventsConsumer).blockTime = time.Millisecond
		consumer.DisableLoop()
		consumer.SetLimit(2)
		consumer.Consume(5, true, func(events []Event) {
			for _, event := range events {
				event.Skip()
			}
			iterations2 = true
		})
	}()
	wg.Wait()
	assert.True(t, iterations1)
	assert.True(t, iterations2)

	pending := engine.GetRedis().XPending("test-stream", "test-group")
	assert.Len(t, pending.Consumers, 2)
	assert.NotEmpty(t, pending.Consumers["test-consumer-1"])
	assert.NotEmpty(t, pending.Consumers["test-consumer-2"])

	consumer = broker.Consumer("test-consumer", "test-group")
	consumer.(*eventsConsumer).blockTime = time.Millisecond
	consumer.DisableLoop()
	consumer.(*eventsConsumer).minIdle = time.Millisecond
	consumer.(*eventsConsumer).claimDuration = time.Millisecond
	time.Sleep(time.Millisecond * 100)
	consumer.Consume(100, true, func(events []Event) {
		for _, event := range events {
			event.Skip()
		}
	})

	pending = engine.GetRedis().XPending("test-stream", "test-group")
	assert.Len(t, pending.Consumers, 1)
	assert.Equal(t, int64(10), pending.Consumers["test-consumer-1"])
}

func TestRedisStreamGroupConsumer(t *testing.T) {
	registry := &Registry{}
	registry.RegisterRedis("localhost:6382", 11)
	registry.RegisterRedisStream("test-stream", "default", []string{"test-group"})
	registry.RegisterRedisStream("test-stream-a", "default", []string{"test-group", "test-group-multi"})
	registry.RegisterRedisStream("test-stream-b", "default", []string{"test-group", "test-group-multi"})
	ctx, cancel := context.WithCancel(context.Background())
	validatedRegistry, err := registry.Validate(ctx)
	assert.NoError(t, err)
	engine := validatedRegistry.CreateEngine(ctx)
	engine.GetRedis().FlushDB()
	broker := engine.GetEventBroker()

	consumer := broker.Consumer("test-consumer", "test-group")

	consumer.(*eventsConsumer).blockTime = time.Millisecond * 10
	consumer.DisableLoop()
	heartBeats := 0
	consumer.SetHeartBeat(time.Second, func() {
		heartBeats++
	})
	consumer.Consume(5, true, func(events []Event) {
		for _, event := range events {
			event.Skip()
		}
	})
	assert.Equal(t, 1, heartBeats)

	for i := 1; i <= 10; i++ {
		engine.GetEventBroker().PublishMap("test-stream", EventAsMap{"name": fmt.Sprintf("a%d", i)})
	}
	iterations := 0
	consumer.Consume(5, true, func(events []Event) {
		iterations++
		assert.Len(t, events, 5)
		if iterations == 1 {
			assert.Equal(t, "a1", events[0].RawData()["name"])
			assert.Equal(t, "a2", events[1].RawData()["name"])
			assert.Equal(t, "a3", events[2].RawData()["name"])
			assert.Equal(t, "a4", events[3].RawData()["name"])
			assert.Equal(t, "a5", events[4].RawData()["name"])
		} else {
			assert.Equal(t, "a6", events[0].RawData()["name"])
			assert.Equal(t, "a7", events[1].RawData()["name"])
			assert.Equal(t, "a8", events[2].RawData()["name"])
			assert.Equal(t, "a9", events[3].RawData()["name"])
			assert.Equal(t, "a10", events[4].RawData()["name"])
		}
		for _, event := range events {
			event.Skip()
		}
	})
	assert.Equal(t, 2, iterations)
	assert.Equal(t, 2, heartBeats)
	time.Sleep(time.Millisecond * 20)
	consumer.(*eventsConsumer).garbageCollector(engine)
	time.Sleep(time.Second)
	assert.Equal(t, int64(10), engine.GetRedis().XLen("test-stream"))
	assert.Equal(t, int64(10), engine.GetRedis().XInfoGroups("test-stream")[0].Pending)
	iterations = 0
	consumer.Consume(10, true, func(events []Event) {
		iterations++
		assert.Len(t, events, 10)
		for _, event := range events {
			assert.Len(t, event.RawData(), 1)
		}
	})
	assert.Equal(t, 1, iterations)

	engine.GetRedis().XTrim("test-stream", 0, false)
	for i := 11; i <= 20; i++ {
		engine.GetEventBroker().PublishMap("test-stream", EventAsMap{"name": fmt.Sprintf("a%d", i)})
	}
	iterations = 0
	consumer.Consume(5, true, func(events []Event) {
		iterations++
		assert.Len(t, events, 5)
		if iterations == 1 {
			assert.Equal(t, "a11", events[0].RawData()["name"])
		} else {
			assert.Equal(t, "a16", events[0].RawData()["name"])
		}
		for _, event := range events {
			event.Skip()
		}
	})
	assert.Equal(t, 2, iterations)
	assert.Equal(t, int64(10), engine.GetRedis().XLen("test-stream"))
	assert.Equal(t, int64(10), engine.GetRedis().XInfoGroups("test-stream")[0].Pending)
	iterations = 0
	heartBeats = 0
	consumer.Consume(5, true, func(events []Event) {
		iterations++
		assert.Len(t, events, 5)
		if iterations == 1 {
			assert.Equal(t, "a11", events[0].RawData()["name"])
		} else {
			assert.Equal(t, "a16", events[0].RawData()["name"])
		}
		events[0].Ack()
		events[1].Ack()
		events[2].Skip()
		events[3].Skip()
		events[4].Skip()
		time.Sleep(time.Millisecond * 200)
	})
	time.Sleep(time.Millisecond * 300)
	assert.Equal(t, 2, iterations)
	assert.Equal(t, 1, heartBeats)
	assert.Equal(t, int64(10), engine.GetRedis().XLen("test-stream"))
	assert.Equal(t, int64(6), engine.GetRedis().XInfoGroups("test-stream")[0].Pending)

	engine.GetRedis().FlushDB()
	for i := 1; i <= 10; i++ {
		engine.GetEventBroker().PublishMap("test-stream", EventAsMap{"name": fmt.Sprintf("a%d", i)})
	}
	consumer = broker.Consumer("test-consumer", "test-group")
	consumer.(*eventsConsumer).blockTime = time.Millisecond
	iterations = 0
	consumer.Consume(5, true, func(events []Event) {
		iterations++
		if iterations == 1 {
			assert.Len(t, events, 5)
			assert.Equal(t, "a1", events[0].RawData()["name"])
			events[0].Ack()
			events[1].Ack()
			events[2].Skip()
			events[3].Skip()
			events[4].Skip()
		} else if iterations == 2 {
			assert.Len(t, events, 5)
			assert.Equal(t, "a6", events[0].RawData()["name"])
			events[0].Ack()
			events[1].Ack()
			events[2].Skip()
			events[3].Skip()
			events[4].Skip()
		} else if iterations == 3 {
			assert.Len(t, events, 5)
			assert.Equal(t, "a3", events[0].RawData()["name"])
			assert.Equal(t, "a4", events[1].RawData()["name"])
			assert.Equal(t, "a5", events[2].RawData()["name"])
			assert.Equal(t, "a8", events[3].RawData()["name"])
			assert.Equal(t, "a9", events[4].RawData()["name"])
			events[0].Ack()
			events[1].Ack()
			events[2].Skip()
			events[3].Skip()
			events[4].Skip()
		} else if iterations == 4 {
			assert.Len(t, events, 1)
			assert.Equal(t, "a10", events[0].RawData()["name"])
			events[0].Ack()
		} else if iterations == 5 {
			assert.Len(t, events, 3)
			assert.Equal(t, "a5", events[0].RawData()["name"])
			assert.Equal(t, "a8", events[1].RawData()["name"])
			assert.Equal(t, "a9", events[2].RawData()["name"])
			events[0].Ack()
			events[1].Ack()
			events[2].Skip()
		} else if iterations == 6 {
			assert.Len(t, events, 1)
			assert.Equal(t, "a9", events[0].RawData()["name"])
			events[0].Ack()
			go func() {
				time.Sleep(time.Millisecond * 100)
				consumer.DisableLoop()
			}()
		}
	})
	assert.Equal(t, 6, iterations)
	engine.GetRedis().FlushDB()
	iterations = 0
	consumer = broker.Consumer("test-consumer-multi", "test-group-multi")
	consumer.(*eventsConsumer).blockTime = time.Millisecond
	consumer.DisableLoop()
	for i := 1; i <= 10; i++ {
		engine.GetEventBroker().PublishMap("test-stream-a", EventAsMap{"name": fmt.Sprintf("a%d", i)})
		engine.GetEventBroker().PublishMap("test-stream-b", EventAsMap{"name": fmt.Sprintf("b%d", i)})
	}
	consumer.Consume(8, true, func(events []Event) {
		iterations++
		if iterations == 1 {
			assert.Len(t, events, 16)
		} else {
			assert.Len(t, events, 4)
		}
		for _, event := range events {
			event.Skip()
		}
	})
	assert.Equal(t, 2, iterations)

	engine.GetRedis().FlushDB()
	iterations = 0
	messages := 0
	valid := false
	consumer = broker.Consumer("test-consumer-unique", "test-group")
	for i := 1; i <= 10; i++ {
		engine.GetEventBroker().PublishMap("test-stream", EventAsMap{"name": fmt.Sprintf("a%d", i)})
	}
	go func() {
		consumer = broker.Consumer("test-consumer-unique", "test-group")
		consumer.DisableLoop()
		consumer.(*eventsConsumer).blockTime = time.Millisecond * 10
		consumer.Consume(8, true, func(events []Event) {
			iterations++
			messages += len(events)
			for _, event := range events {
				event.Skip()
			}
		})
	}()
	time.Sleep(time.Millisecond * 100)
	assert.Equal(t, 2, iterations)
	assert.Equal(t, 10, messages)

	engine.GetRedis().FlushDB()
	consumer = broker.Consumer("test-consumer-unique", "test-group")
	consumer.(*eventsConsumer).blockTime = time.Millisecond * 400
	valid = true
	go func() {
		time.Sleep(time.Millisecond * 200)
		cancel()
	}()
	consumer.Consume(1, true, func(events []Event) {
		valid = false
		for _, event := range events {
			event.Skip()
		}
	})
	assert.True(t, valid)

	type testStructEvent struct {
		Name string
		Age  int
	}

	engine = validatedRegistry.CreateEngine(context.Background())
	eventFlusher := engine.GetEventBroker().NewFlusher()
	eventFlusher.Publish("test-stream", testStructEvent{Name: "a", Age: 18})
	eventFlusher.Publish("test-stream", testStructEvent{Name: "b", Age: 20})
	eventFlusher.Flush()
	valid = false
	consumer = engine.GetEventBroker().Consumer("test-consumer-unique", "test-group")
	consumer.DisableLoop()
	consumer.(*eventsConsumer).blockTime = time.Millisecond * 10
	consumer.Consume(10, true, func(events []Event) {
		valid = true
		assert.Len(t, events, 2)
		for i, event := range events {
			data := &testStructEvent{}
			assert.True(t, event.IsSerialized())
			err := event.Unserialize(data)
			assert.NoError(t, err)
			if i == 0 {
				assert.Equal(t, "a", data.Name)
				assert.Equal(t, 18, data.Age)
			} else {
				assert.Equal(t, "b", data.Name)
				assert.Equal(t, 20, data.Age)
			}
		}
	})
	assert.True(t, valid)
}
