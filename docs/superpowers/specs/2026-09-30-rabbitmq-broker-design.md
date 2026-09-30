# RabbitMQ Broker Adapter for Asynq Design Spec

## 1. Objective
Enable Asynq to use RabbitMQ as the message broker while keeping client-facing and server-facing function signatures and ergonomics identical (`NewClient`, `NewServer`, `client.Enqueue`, `asynq.ProcessIn`, `asynq.MaxRetry`, `srv.Run`, `mux.HandleFunc`).

## 2. Architecture & Design

### 2.1 Interface & Option Changes
- In `asynq.go`:
  - Introduce `type RabbitMQClientOpt struct { URL string }`
  - Introduce `type BrokerProvider interface { MakeBroker() (base.Broker, error) }`
  - Make `RedisConnOpt` implement or convert to `base.Broker` (via `rdb.NewRDB`).
  - Update `NewClient(r RedisConnOpt)` / `NewServer(r RedisConnOpt, cfg Config)` parameter type or use type switch on `interface{}` / `BrokerProvider` to remain backward-compatible without breaking existing code.

### 2.2 Broker Implementation (`internal/rmq/rmq.go`)
Implement `base.Broker` interface:
- **Topology:**
  - Exchanges:
    - Delayed Exchange: `asynq.delayed` (type `x-delayed-message`, args `{"x-delayed-type": "direct"}`).
    - DLQ Exchange: `asynq.dead_letter` (type `direct`).
  - Queues:
    - Target queues created per queue name (e.g. `default`, `critical`).
    - Bound to `asynq.delayed` with routing key = queue name.
    - Bound to `asynq.dead_letter` for DLQ queues (e.g. `asynq:dead_letter:<qname>`).
- **Methods:**
  - `Ping()`: Verify AMQP connection/channel health.
  - `Close()`: Close AMQP connection and channels.
  - `Enqueue(ctx, msg)`: Marshal to Protobuf via `base.EncodeMessage(msg)` and publish to target queue or `asynq.delayed` with `x-delay: 0`.
  - `Schedule(ctx, msg, processAt)`: Calculate delay `processAt.Sub(time.Now())` in milliseconds, set header `x-delay: <ms>`, publish to `asynq.delayed`.
  - `Retry(ctx, msg, processAt, errMsg, isFailure)`: Update `msg.Retried`, `msg.ErrorMsg`, `msg.LastFailedAt`. If `Retried < Retry`, publish to `asynq.delayed` with `x-delay`. If retries exhausted, publish to `asynq.dead_letter`.
  - `Archive(ctx, msg, errMsg)`: Publish directly to DLQ `asynq.dead_letter`.
  - `Dequeue(qnames...)`: Fetch next message from the specified queues (using channel subscription or basic.get with backoff), unmarshal Protobuf, return `(*TaskMessage, leaseDeadline, nil)`.
  - `Done(ctx, msg)`: Acknowledge message if needed or no-op if auto-acked.
  - `Requeue(ctx, msg)`: Re-publish message to queue.
  - Unsupported State Methods:
    - `EnqueueUnique`, `ScheduleUnique`, `AddToGroup`, `AddToGroupUnique`, `ListGroups`, `AggregationCheck`, `ReadAggregationSet`, `DeleteAggregationSet`, `ReclaimStaleAggregationSets`, `DeleteExpiredCompletedTasks`, `ListLeaseExpired`, `ExtendLease`, `WriteServerState`, `ClearServerState`, `CancelationPubSub`, `PublishCancelation`, `WriteResult`: Return `errors.ErrNotSupported` / no-op as state store is omitted.

### 2.3 Server Adaptations
- In `server.go`:
  - Detect whether broker is RabbitMQ (`rmq.Broker`) vs Redis (`rdb.RDB`).
  - When RabbitMQ is used, bypass Redis-only background tickers (`heartbeater`, `forwarder`, `recoverer`, `janitor`, `aggregator`, `subscriber`).
  - Core `processor` continues running untouched to handle tasks, concurrency, timeouts, and `ServeMux` dispatch.

## 3. Testing Strategy
- Unit tests in `internal/rmq/rmq_test.go` testing exchange declaration, publish/consume, delayed scheduling, retry, and DLQ.
- Integration test with running RabbitMQ container (`localhost:5672`) verifying end-to-end client enqueue and server process with `asynq.NewClient` and `asynq.NewServer`.
