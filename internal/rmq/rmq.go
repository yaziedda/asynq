// Copyright 2020 Kentaro Hibino. All rights reserved.
// Use of this source code is governed by a MIT license
// that can be found in the LICENSE file.

// Package rmq implements a RabbitMQ-backed broker for asynq.
//
// It supports the core task-queue features: Enqueue, Dequeue, Schedule,
// Retry with exponential backoff, and dead-letter archival. It does NOT
// support the state-store features that require a queryable data store
// (Inspector, Web UI, dedup, group aggregation, lease recovery). Those
// methods return errors.ErrNotSupported or are no-ops.
package rmq

import (
	"fmt"
	"sync"
	"time"

	"github.com/yaziedda/asynq/internal/base"
	"github.com/yaziedda/asynq/internal/errors"
	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/redis/go-redis/v9"
)

const (
	// delayedExchange is the custom exchange used to route tasks with an
	// optional delay (via the rabbitmq-delayed-message-exchange plugin).
	delayedExchange = "asynq.delayed"

	// deadLetterExchange routes tasks whose retries are exhausted or that
	// are explicitly archived.
	deadLetterExchange = "asynq.dead_letter"

	// defaultLease is how long a dequeued task is leased to a worker before
	// it is considered stale. On the RabbitMQ broker the lease is advisory;
	// message redelivery is governed by AMQP ack/nack semantics.
	defaultLease = 30 * time.Minute
)

// deadLetterQueue returns the name of the dead-letter queue for qname.
func deadLetterQueue(qname string) string {
	return "asynq:dead_letter:" + qname
}

// Broker is a RabbitMQ implementation of base.Broker.
type Broker struct {
	conn    *amqp.Connection
	channel *amqp.Channel

	mu sync.Mutex
	// declared tracks queues that have already been declared+bound so we
	// avoid redundant AMQP round-trips on every publish.
	declared map[string]bool
	// deliveries maps a task ID to its unacknowledged AMQP delivery tag so
	// Done/Requeue can ack/nack the correct message after processing.
	deliveries map[string]uint64
}

// NewBroker connects to RabbitMQ at the given AMQP URL, declares the
// required exchanges, and returns a ready-to-use Broker.
func NewBroker(url string) (*Broker, error) {
	conn, err := amqp.Dial(url)
	if err != nil {
		return nil, fmt.Errorf("rmq: failed to dial %q: %w", url, err)
	}
	ch, err := conn.Channel()
	if err != nil {
		conn.Close()
		return nil, fmt.Errorf("rmq: failed to open channel: %w", err)
	}
	if err := ch.ExchangeDeclare(
		delayedExchange, "x-delayed-message", true, false, false, false,
		amqp.Table{"x-delayed-type": "direct"},
	); err != nil {
		ch.Close()
		conn.Close()
		return nil, fmt.Errorf("rmq: failed to declare delayed exchange (is the rabbitmq-delayed-message-exchange plugin enabled?): %w", err)
	}
	if err := ch.ExchangeDeclare(
		deadLetterExchange, "direct", true, false, false, false, nil,
	); err != nil {
		ch.Close()
		conn.Close()
		return nil, fmt.Errorf("rmq: failed to declare dead-letter exchange: %w", err)
	}
	if err := ch.Confirm(false); err != nil {
		ch.Close()
		conn.Close()
		return nil, fmt.Errorf("rmq: failed to put channel in confirm mode: %w", err)
	}
	return &Broker{
		conn:       conn,
		channel:    ch,
		declared:   make(map[string]bool),
		deliveries: make(map[string]uint64),
	}, nil
}

// ensureQueue declares qname and binds it to the delayed exchange. It is
// idempotent and cached per broker instance.
func (b *Broker) ensureQueue(qname string) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.declared[qname] {
		return nil
	}
	if _, err := b.channel.QueueDeclare(qname, true, false, false, false, nil); err != nil {
		return fmt.Errorf("rmq: failed to declare queue %q: %w", qname, err)
	}
	if err := b.channel.QueueBind(qname, qname, delayedExchange, false, nil); err != nil {
		return fmt.Errorf("rmq: failed to bind queue %q to delayed exchange: %w", qname, err)
	}
	b.declared[qname] = true
	return nil
}

// ensureDeadLetterQueue declares the dead-letter queue for qname and binds
// it to the dead-letter exchange.
func (b *Broker) ensureDeadLetterQueue(qname string) error {
	dlq := deadLetterQueue(qname)
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.declared[dlq] {
		return nil
	}
	if _, err := b.channel.QueueDeclare(dlq, true, false, false, false, nil); err != nil {
		return fmt.Errorf("rmq: failed to declare dead-letter queue %q: %w", dlq, err)
	}
	if err := b.channel.QueueBind(dlq, qname, deadLetterExchange, false, nil); err != nil {
		return fmt.Errorf("rmq: failed to bind dead-letter queue %q: %w", dlq, err)
	}
	b.declared[dlq] = true
	return nil
}

// Ping verifies the connection is alive.
func (b *Broker) Ping() error {
	if b.conn == nil || b.conn.IsClosed() {
		return errors.E(errors.Op("rmq.Ping"), errors.Internal, "amqp connection is closed")
	}
	return nil
}

// Close closes the channel and connection.
func (b *Broker) Close() error {
	if b.channel != nil {
		b.channel.Close()
	}
	if b.conn != nil {
		return b.conn.Close()
	}
	return nil
}

// compile-time check that Broker implements base.Broker.
var _ base.Broker = (*Broker)(nil)

// ensure redis import is used (CancelationPubSub returns *redis.PubSub per
// the current base.Broker contract).
var _ = (*redis.PubSub)(nil)
