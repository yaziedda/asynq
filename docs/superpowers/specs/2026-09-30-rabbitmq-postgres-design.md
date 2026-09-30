# RabbitMQ + PostgreSQL State Store Design Spec

## 1. Goal
Provide a full-featured Asynq broker (`RabbitMQWithPostgresOpt`) where:
- **RabbitMQ** handles the transport: queueing, priority, delayed messages, delivery.
- **PostgreSQL** handles the state store: task status tracking, deduplication (unique keys), results & retention, groups, and all **Inspector & Web Dashboard** queries.

## 2. PostgreSQL Schema
Database: `asynq_rmq`

Tables:
1. `asynq_tasks`:
   - `id` (VARCHAR PRIMARY KEY)
   - `queue` (VARCHAR NOT NULL)
   - `type` (VARCHAR NOT NULL)
   - `payload` (BYTEA)
   - `state` (VARCHAR NOT NULL: 'pending', 'active', 'scheduled', 'retry', 'archived', 'completed', 'aggregating')
   - `retry` (INT)
   - `retried` (INT)
   - `error_msg` (TEXT)
   - `last_failed_at` (TIMESTAMPTZ)
   - `timeout` (BIGINT)
   - `deadline` (BIGINT)
   - `unique_key` (VARCHAR)
   - `group_key` (VARCHAR)
   - `retention` (BIGINT)
   - `completed_at` (TIMESTAMPTZ)
   - `next_process_at` (TIMESTAMPTZ)
   - `created_at` (TIMESTAMPTZ DEFAULT NOW())
   - `updated_at` (TIMESTAMPTZ DEFAULT NOW())

2. `asynq_unique_keys`:
   - `key` (VARCHAR PRIMARY KEY)
   - `task_id` (VARCHAR NOT NULL)
   - `expires_at` (TIMESTAMPTZ NOT NULL)

3. `asynq_results`:
   - `task_id` (VARCHAR PRIMARY KEY)
   - `result` (BYTEA)
   - `completed_at` (TIMESTAMPTZ NOT NULL)

4. `asynq_servers`:
   - `server_id` (VARCHAR PRIMARY KEY)
   - `host` (VARCHAR)
   - `pid` (INT)
   - `concurrency` (INT)
   - `queues` (JSONB)
   - `strict_priority` (BOOLEAN)
   - `started_at` (TIMESTAMPTZ)
   - `last_heartbeat` (TIMESTAMPTZ)

## 3. Package Structure
- `internal/rmqpg/`:
  - `broker.go`: Implements `base.Broker` delegating transport to AMQP and state writes to PostgreSQL.
  - `inspector.go`: Implements inspector queries (`ListPending`, `ListActive`, `GetTaskInfo`, `DeleteTask`, `ArchiveTask`, etc.) directly on PostgreSQL.
  - `schema.go`: Auto-migration / DDL for PostgreSQL tables.

## 4. Public API in asynq
- `type RabbitMQWithPostgresOpt struct { RabbitMQURL string; PostgresURL string }`
- `NewClient(opt)`
- `NewServer(opt, cfg)`
- `NewInspector(opt)`
