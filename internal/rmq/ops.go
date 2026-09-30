// Copyright 2020 Kentaro Hibino. All rights reserved.
// Use of this source code is governed by a MIT license
// that can be found in the LICENSE file.

package rmq

import (
	"context"
	"time"

	"github.com/hibiken/asynq/internal/base"
	"github.com/hibiken/asynq/internal/errors"
	amqp "github.com/rabbitmq/amqp091-go"
)

// publishDelayed publishes msg to the delayed exchange routed to msg.Queue
// with the given delay in milliseconds (0 = immediate).
func (b *Broker) publishDelayed(ctx context.Context, msg *base.TaskMessage, delayMS int64) error {
	if err := b.ensureQueue(msg.Queue); err != nil {
		return err
	}
	data, err := base.EncodeMessage(msg)
	if err != nil {
		return err
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.channel.PublishWithContext(ctx, delayedExchange, msg.Queue, false, false, amqp.Publishing{
		ContentType:  "application/x-protobuf",
		DeliveryMode: amqp.Persistent,
		MessageId:    msg.ID,
		Body:         data,
		Headers:      amqp.Table{"x-delay": delayMS},
	})
}

// Enqueue publishes a task for immediate processing.
func (b *Broker) Enqueue(ctx context.Context, msg *base.TaskMessage) error {
	return b.publishDelayed(ctx, msg, 0)
}

// Schedule publishes a task to be processed at processAt.
func (b *Broker) Schedule(ctx context.Context, msg *base.TaskMessage, processAt time.Time) error {
	delay := time.Until(processAt).Milliseconds()
	if delay < 0 {
		delay = 0
	}
	return b.publishDelayed(ctx, msg, delay)
}

// Retry re-schedules a failed task with backoff, or archives it to the
// dead-letter queue when retries are exhausted.
func (b *Broker) Retry(ctx context.Context, msg *base.TaskMessage, processAt time.Time, errMsg string, isFailure bool) error {
	msg.ErrorMsg = errMsg
	if isFailure {
		msg.LastFailedAt = time.Now().Unix()
	}
	msg.Retried++
	if msg.Retried > msg.Retry {
		return b.Archive(ctx, msg, errMsg)
	}
	return b.Schedule(ctx, msg, processAt)
}

// Archive publishes a task to its dead-letter queue for inspection.
func (b *Broker) Archive(ctx context.Context, msg *base.TaskMessage, errMsg string) error {
	msg.ErrorMsg = errMsg
	if err := b.ensureDeadLetterQueue(msg.Queue); err != nil {
		return err
	}
	data, err := base.EncodeMessage(msg)
	if err != nil {
		return err
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.channel.PublishWithContext(ctx, deadLetterExchange, msg.Queue, false, false, amqp.Publishing{
		ContentType:  "application/x-protobuf",
		DeliveryMode: amqp.Persistent,
		MessageId:    msg.ID,
		Body:         data,
	})
}

// Requeue puts a task back onto its queue for immediate reprocessing.
func (b *Broker) Requeue(ctx context.Context, msg *base.TaskMessage) error {
	b.mu.Lock()
	tag, ok := b.deliveries[msg.ID]
	if ok {
		delete(b.deliveries, msg.ID)
	}
	b.mu.Unlock()
	if ok {
		// Nack with requeue=true returns the original delivery to the queue.
		if err := b.channel.Nack(tag, false, true); err == nil {
			return nil
		}
	}
	return b.Enqueue(ctx, msg)
}

// Dequeue pulls the next available task from the given queues, checking
// them in the order provided. It blocks with a short poll interval until a
// task is available. Returns errors.ErrNoProcessableTask when all queues
// are empty.
func (b *Broker) Dequeue(qnames ...string) (*base.TaskMessage, time.Time, error) {
	for _, qname := range qnames {
		if err := b.ensureQueue(qname); err != nil {
			return nil, time.Time{}, err
		}
		b.mu.Lock()
		delivery, ok, err := b.channel.Get(qname, false)
		b.mu.Unlock()
		if err != nil {
			return nil, time.Time{}, err
		}
		if !ok {
			continue
		}
		msg, err := base.DecodeMessage(delivery.Body)
		if err != nil {
			// discard malformed message so it doesn't block the queue.
			_ = delivery.Nack(false, false)
			continue
		}
		b.mu.Lock()
		b.deliveries[msg.ID] = delivery.DeliveryTag
		b.mu.Unlock()
		return msg, time.Now().Add(defaultLease), nil
	}
	return nil, time.Time{}, errors.E(errors.Op("rmq.Dequeue"), errors.NotFound, errors.ErrNoProcessableTask)
}

// Done acknowledges successful processing of a task.
func (b *Broker) Done(ctx context.Context, msg *base.TaskMessage) error {
	b.mu.Lock()
	tag, ok := b.deliveries[msg.ID]
	if ok {
		delete(b.deliveries, msg.ID)
	}
	b.mu.Unlock()
	if !ok {
		return nil
	}
	return b.channel.Ack(tag, false)
}
