# RabbitMQ Broker Adapter Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Enable Asynq to use RabbitMQ as the message broker without changing client or server method signatures.

**Architecture:** Create an `internal/rmq` package implementing `base.Broker`, using `github.com/rabbitmq/amqp091-go`. Introduce `BrokerProvider` and `RabbitMQClientOpt` in `asynq.go`. Wire `NewClient` and `NewServer` to instantiate the RabbitMQ broker when provided, bypassing Redis-specific background tickers in `server.go`. Unsupported state-store features return `ErrNotSupported` or log/ignore.

**Tech Stack:** Go 1.24.0, `github.com/rabbitmq/amqp091-go`, `github.com/hibiken/asynq`

**Spec:** `docs/superpowers/specs/2026-09-30-rabbitmq-broker-design.md`

## Global Constraints

- Go 1.24.0 compatibility
- Do not break existing Redis backward compatibility (all old functions and types must still work as before)
- Do not change existing public methods on `Client` and `Server`

---

### Task 1: Update asynq.go and base.go Interfaces

**Files:**
- Modify: `asynq.go`
- Modify: `server.go`
- Modify: `client.go`
- Modify: `internal/base/base.go`

**Interfaces:**
- Consumes: Nothing
- Produces: `BrokerProvider` interface, `RabbitMQClientOpt` struct, `isRedisBroker(b base.Broker) bool` helper

- [ ] **Step 1: Write the failing tests**

Write failing tests in `asynq_test.go` checking if `RabbitMQClientOpt` is accepted by `NewClient` and `NewServer` (using a dummy connection).

- [ ] **Step 2: Run test to verify it fails**

Run: `go test -v ./asynq_test.go`
Expected: Compile error because `RabbitMQClientOpt` does not exist.

- [ ] **Step 3: Write minimal implementation**

In `asynq.go`, define:
```go
// BrokerProvider is an interface that provides a base.Broker.
type BrokerProvider interface {
	MakeBroker() (base.Broker, error)
}

// RabbitMQClientOpt is used to create a RabbitMQ broker.
type RabbitMQClientOpt struct {
	URL string // e.g., "amqp://guest:guest@localhost:5672/"
}

func (opt RabbitMQClientOpt) MakeBroker() (base.Broker, error) {
	// For now, return nil to make it compile. We will implement rmq.NewBroker next.
	return nil, nil
}
```

Update `RedisConnOpt` to have a `MakeBroker` method or update `NewClient`/`NewServer` to use a type switch:
```go
// In client.go:
func NewClient(r RedisConnOpt) *Client {
    // backward compat logic
}
func NewClientWithBroker(b base.Broker) *Client {
    // New logic
}
```
*Wait, let's just make `RedisConnOpt` keep `MakeRedisClient()` and add `makeBroker(r interface{}) (base.Broker, error)` helper to `asynq.go` that handles both `RedisConnOpt` and `RabbitMQClientOpt`.*

- [ ] **Step 4: Run test to verify it passes**

Run: `go test -v ./...`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add asynq.go client.go server.go asynq_test.go
git commit -m "feat: introduce BrokerProvider and RabbitMQClientOpt interfaces"
```

---

### Task 2: Implement rmq Broker (Connection & Queues)

**Files:**
- Create: `internal/rmq/rmq.go`
- Create: `internal/rmq/rmq_test.go`

**Interfaces:**
- Consumes: `RabbitMQClientOpt`
- Produces: `rmq.NewBroker(url string) (base.Broker, error)`

- [ ] **Step 1: Write the failing tests**

In `rmq_test.go`, test `NewBroker` connects to AMQP and pings successfully.

- [ ] **Step 2: Run test to verify it fails**

Run: `go test -v ./internal/rmq/...`
Expected: FAIL (no such file or missing implementation)

- [ ] **Step 3: Write minimal implementation**

In `internal/rmq/rmq.go`:
```go
package rmq

import (
	"context"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/hibiken/asynq/internal/base"
	"github.com/hibiken/asynq/internal/errors"
)

type Broker struct {
	conn    *amqp.Connection
	channel *amqp.Channel
}

func NewBroker(url string) (*Broker, error) {
	conn, err := amqp.Dial(url)
	if err != nil {
		return nil, err
	}
	ch, err := conn.Channel()
	if err != nil {
		return nil, err
	}

	// Declare Exchanges
	err = ch.ExchangeDeclare("asynq.delayed", "x-delayed-message", true, false, false, false, amqp.Table{"x-delayed-type": "direct"})
	if err != nil { return nil, err }
	err = ch.ExchangeDeclare("asynq.dead_letter", "direct", true, false, false, false, nil)
	if err != nil { return nil, err }

	return &Broker{conn: conn, channel: ch}, nil
}

func (b *Broker) Ping() error {
	if b.conn.IsClosed() {
		return errors.New("amqp connection closed")
	}
	return nil
}

func (b *Broker) Close() error {
	b.channel.Close()
	return b.conn.Close()
}
// Add empty stubs for ALL other base.Broker interface methods returning errors.ErrNotSupported
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test -v ./internal/rmq/...`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add internal/rmq/
git commit -m "feat: implement basic rmq connection and Ping"
```

---

### Task 3: Implement Enqueue, Schedule, and DLQ in rmq

**Files:**
- Modify: `internal/rmq/rmq.go`
- Modify: `internal/rmq/rmq_test.go`

**Interfaces:**
- Consumes: `rmq.Broker`
- Produces: `Enqueue`, `Schedule`, `Archive`

- [ ] **Step 1: Write the failing tests**

Test enqueueing a message, scheduling a message with delay, and archiving a message. Assert messages arrive in corresponding queues (requires binding queue to exchange in test).

- [ ] **Step 2: Run test to verify it fails**

Run: `go test -v ./internal/rmq/...`

- [ ] **Step 3: Write minimal implementation**

In `internal/rmq/rmq.go`:
```go
func (b *Broker) declareQueue(qname string) error {
	_, err := b.channel.QueueDeclare(qname, true, false, false, false, nil)
	if err != nil { return err }
	err = b.channel.QueueBind(qname, qname, "asynq.delayed", false, nil)
	if err != nil { return err }
	return nil
}

func (b *Broker) Enqueue(ctx context.Context, msg *base.TaskMessage) error {
	if err := b.declareQueue(msg.Queue); err != nil { return err }
	data, err := base.EncodeMessage(msg)
	if err != nil { return err }
	return b.channel.PublishWithContext(ctx, "asynq.delayed", msg.Queue, false, false, amqp.Publishing{
		ContentType: "application/protobuf",
		Body:        data,
		Headers:     amqp.Table{"x-delay": 0},
	})
}

func (b *Broker) Schedule(ctx context.Context, msg *base.TaskMessage, processAt time.Time) error {
	if err := b.declareQueue(msg.Queue); err != nil { return err }
	data, err := base.EncodeMessage(msg)
	if err != nil { return err }
	delay := processAt.Sub(time.Now()).Milliseconds()
	if delay < 0 { delay = 0 }
	return b.channel.PublishWithContext(ctx, "asynq.delayed", msg.Queue, false, false, amqp.Publishing{
		ContentType: "application/protobuf",
		Body:        data,
		Headers:     amqp.Table{"x-delay": delay},
	})
}

func (b *Broker) Archive(ctx context.Context, msg *base.TaskMessage, errMsg string) error {
    msg.ErrorMsg = errMsg
    dlqName := "asynq:dead_letter:" + msg.Queue
	_, err := b.channel.QueueDeclare(dlqName, true, false, false, false, nil)
	if err != nil { return err }
	err = b.channel.QueueBind(dlqName, msg.Queue, "asynq.dead_letter", false, nil)
	if err != nil { return err }

	data, err := base.EncodeMessage(msg)
	if err != nil { return err }
	return b.channel.PublishWithContext(ctx, "asynq.dead_letter", msg.Queue, false, false, amqp.Publishing{
		ContentType: "application/protobuf",
		Body:        data,
	})
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test -v ./internal/rmq/...`

- [ ] **Step 5: Commit**

```bash
git add internal/rmq/
git commit -m "feat: implement Enqueue, Schedule, and DLQ for rmq"
```

---

### Task 4: Implement Dequeue, Done, Retry, and Requeue

**Files:**
- Modify: `internal/rmq/rmq.go`

**Interfaces:**
- Consumes: `rmq.Broker`
- Produces: `Dequeue`, `Done`, `Retry`, `Requeue`

- [ ] **Step 1: Write the failing tests**

Test dequeuing a message previously enqueued. Test `Retry` republishes with delay.

- [ ] **Step 2: Run test to verify it fails**

- [ ] **Step 3: Write minimal implementation**

```go
func (b *Broker) Dequeue(qnames ...string) (*base.TaskMessage, time.Time, error) {
	// Simple polling loop over queues (for strict priority or round robin)
    for {
        for _, q := range qnames {
            msg, ok, err := b.channel.Get(q, false) // autoAck=false
            if err != nil { return nil, time.Time{}, err }
            if ok {
                taskMsg, err := base.DecodeMessage(msg.Body)
                if err != nil {
                    msg.Nack(false, false) // discard malformed
                    continue
                }
                // HACK: store deliveryTag in UniqueKey or GroupKey to acknowledge later in Done()
                // Wait, base interface doesn't pass deliveryTag to Done. We will ack in Done using msg.ID if we use a memory map, or we can just auto-ack on Dequeue and handle reliability via the Retry mechanism.
                // For simplicity: autoAck=true, or use a sync.Map mapping ID to DeliveryTag.
                msg.Ack(false)
                // Return task with a lease of 5 minutes
                return taskMsg, time.Now().Add(5 * time.Minute), nil
            }
        }
        time.Sleep(100 * time.Millisecond) // polling wait
    }
}

func (b *Broker) Done(ctx context.Context, msg *base.TaskMessage) error {
    // If auto-acked on Dequeue, this is a no-op.
	return nil
}

func (b *Broker) Requeue(ctx context.Context, msg *base.TaskMessage) error {
	return b.Enqueue(ctx, msg)
}

func (b *Broker) Retry(ctx context.Context, msg *base.TaskMessage, processAt time.Time, errMsg string, isFailure bool) error {
	msg.ErrorMsg = errMsg
	msg.LastFailedAt = time.Now().Unix()
	msg.Retried++
	if msg.Retried > msg.Retry {
		return b.Archive(ctx, msg, errMsg)
	}
	return b.Schedule(ctx, msg, processAt)
}
```

- [ ] **Step 4: Run test to verify it passes**

- [ ] **Step 5: Commit**

```bash
git add internal/rmq/
git commit -m "feat: implement Dequeue and Retry in rmq"
```

---

### Task 5: Wire Client and Server to Use rmq Broker

**Files:**
- Modify: `client.go`
- Modify: `server.go`

**Interfaces:**
- Consumes: `RabbitMQClientOpt` and `rmq.Broker`
- Produces: Correct instantiation of broker based on option.

- [ ] **Step 1: Write the failing tests**

Write end-to-end test creating `NewServer(RabbitMQClientOpt{URL: "amqp://guest:guest@localhost:5672/"}, ...)` and verifying it skips Redis-only components.

- [ ] **Step 2: Run test to verify it fails**

- [ ] **Step 3: Write minimal implementation**

In `asynq.go`:
```go
func createBroker(opt interface{}) (base.Broker, error) {
	switch o := opt.(type) {
	case RedisConnOpt:
		return rdb.NewRDB(o.MakeRedisClient().(redis.UniversalClient)), nil
	case RabbitMQClientOpt:
		return rmq.NewBroker(o.URL)
	default:
		return nil, errors.New("unsupported broker option")
	}
}
```

In `client.go`:
```go
// Adjust NewClient to use createBroker
```

In `server.go` `NewServer` & `Start`:
```go
	b, err := createBroker(r)
    if err != nil { panic(err) }
    isRedis := false
    if _, ok := b.(*rdb.RDB); ok { isRedis = true }
	
    // Only initialize and start heartbeater, forwarder, recoverer, janitor, aggregator if isRedis is true.
```

- [ ] **Step 4: Run test to verify it passes**

- [ ] **Step 5: Commit**

```bash
git add asynq.go client.go server.go
git commit -m "feat: wire RabbitMQClientOpt in NewClient and NewServer"
```
