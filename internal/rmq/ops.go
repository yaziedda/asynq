package rmq

import (
	"context"
	"fmt"
	"time"

	"github.com/hibiken/asynq/internal/base"
	"github.com/hibiken/asynq/internal/errors"
	amqp "github.com/rabbitmq/amqp091-go"
)

func (b *Broker) publish(ctx context.Context, exchange, routingKey string, headers amqp.Table, data []byte, messageID string) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	pub := amqp.Publishing{
		ContentType:  "application/x-protobuf",
		DeliveryMode: amqp.Persistent,
		MessageId:    messageID,
		Body:         data,
		Headers:      headers,
	}
	conf, err := b.channel.PublishWithDeferredConfirmWithContext(ctx, exchange, routingKey, false, false, pub)
	if err != nil {
		return fmt.Errorf("rmq: publish failed: %w", err)
	}
	if !conf.Wait() {
		return errors.New("rmq: publish not confirmed by broker")
	}
	return nil
}

func (b *Broker) encodeAndPublish(ctx context.Context, exchange, routingKey string, headers amqp.Table, msg *base.TaskMessage) error {
	data, err := base.EncodeMessage(msg)
	if err != nil {
		return err
	}
	return b.publish(ctx, exchange, routingKey, headers, data, msg.ID)
}

func (b *Broker) Enqueue(ctx context.Context, msg *base.TaskMessage) error {
	if err := b.ensureQueue(msg.Queue); err != nil {
		return err
	}
	return b.encodeAndPublish(ctx, "", msg.Queue, nil, msg)
}

func (b *Broker) Schedule(ctx context.Context, msg *base.TaskMessage, processAt time.Time) error {
	if err := b.ensureQueue(msg.Queue); err != nil {
		return err
	}
	delay := time.Until(processAt).Milliseconds()
	if delay <= 0 {
		return b.Enqueue(ctx, msg)
	}
	return b.encodeAndPublish(ctx, delayedExchange, msg.Queue, amqp.Table{"x-delay": delay}, msg)
}

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

func (b *Broker) Archive(ctx context.Context, msg *base.TaskMessage, errMsg string) error {
	msg.ErrorMsg = errMsg
	if err := b.ensureDeadLetterQueue(msg.Queue); err != nil {
		return err
	}
	return b.encodeAndPublish(ctx, deadLetterExchange, msg.Queue, nil, msg)
}

func (b *Broker) Requeue(ctx context.Context, msg *base.TaskMessage) error {
	b.mu.Lock()
	tag, ok := b.deliveries[msg.ID]
	if ok {
		delete(b.deliveries, msg.ID)
	}
	b.mu.Unlock()
	if ok {
		if err := b.channel.Nack(tag, false, true); err == nil {
			return nil
		}
	}
	return b.Enqueue(ctx, msg)
}

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

func (b *Broker) MarkAsComplete(ctx context.Context, msg *base.TaskMessage) error {
	return b.Done(ctx, msg)
}
