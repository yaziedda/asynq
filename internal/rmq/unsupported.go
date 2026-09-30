// Copyright 2020 Kentaro Hibino. All rights reserved.
// Use of this source code is governed by a MIT license
// that can be found in the LICENSE file.

package rmq

import (
	"context"
	"time"

	"github.com/yaziedda/asynq/internal/base"
	"github.com/yaziedda/asynq/internal/errors"
	"github.com/redis/go-redis/v9"
)

var errUnsupported = errors.ErrNotSupported

func (b *Broker) EnqueueUnique(ctx context.Context, msg *base.TaskMessage, ttl time.Duration) error {
	return errUnsupported
}

func (b *Broker) ScheduleUnique(ctx context.Context, msg *base.TaskMessage, processAt time.Time, ttl time.Duration) error {
	return errUnsupported
}

func (b *Broker) BatchEnqueue(ctx context.Context, items []base.BatchEnqueueItem) (int, error) {
	var count int
	for _, it := range items {
		var err error
		if it.ProcessAt.After(time.Now()) {
			err = b.Schedule(ctx, it.Msg, it.ProcessAt)
		} else {
			err = b.Enqueue(ctx, it.Msg)
		}
		if err != nil {
			return count, err
		}
		count++
	}
	return count, nil
}

func (b *Broker) ForwardIfReady(qnames ...string) error {
	// Delayed exchange handles ready-check inside RabbitMQ; nothing for asynq to poll.
	return nil
}

func (b *Broker) AddToGroup(ctx context.Context, msg *base.TaskMessage, gname string) error {
	return errUnsupported
}

func (b *Broker) AddToGroupUnique(ctx context.Context, msg *base.TaskMessage, groupKey string, ttl time.Duration) error {
	return errUnsupported
}

func (b *Broker) ListGroups(qname string) ([]string, error) {
	return nil, errUnsupported
}

func (b *Broker) AggregationCheck(qname, gname string, t time.Time, gracePeriod, maxDelay time.Duration, maxSize int) (string, error) {
	return "", errUnsupported
}

func (b *Broker) ReadAggregationSet(qname, gname, aggregationSetID string) ([]*base.TaskMessage, time.Time, error) {
	return nil, time.Time{}, errUnsupported
}

func (b *Broker) DeleteAggregationSet(ctx context.Context, qname, gname, aggregationSetID string) error {
	return errUnsupported
}

func (b *Broker) ReclaimStaleAggregationSets(qname string) error {
	return nil
}

func (b *Broker) DeleteExpiredCompletedTasks(qname string, batchSize int) error {
	return nil
}

func (b *Broker) ListLeaseExpired(cutoff time.Time, qnames ...string) ([]*base.TaskMessage, error) {
	return nil, nil
}

func (b *Broker) ExtendLease(qname string, ids ...string) (time.Time, error) {
	return time.Now().Add(defaultLease), nil
}

func (b *Broker) WriteServerState(info *base.ServerInfo, workers []*base.WorkerInfo, ttl time.Duration) error {
	return nil
}

func (b *Broker) ClearServerState(host string, pid int, serverID string) error {
	return nil
}

func (b *Broker) CancelationPubSub() (*redis.PubSub, error) {
	return nil, errUnsupported
}

func (b *Broker) PublishCancelation(id string) error {
	return errUnsupported
}

func (b *Broker) WriteResult(qname, id string, data []byte) (int, error) {
	return 0, errUnsupported
}
