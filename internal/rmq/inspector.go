// Copyright 2020 Kentaro Hibino. All rights reserved.
// Use of this source code is governed by a MIT license
// that can be found in the LICENSE file.

package rmq

import (
	"encoding/base64"
	"fmt"
	"net/url"
	"strings"
	"time"

	"github.com/hibiken/asynq/internal/base"
	"github.com/hibiken/asynq/internal/errors"
	"github.com/hibiken/asynq/internal/rdb"
	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/redis/go-redis/v9"
)

// Inspector is a RabbitMQ implementation of the inspector backend.
// It uses RabbitMQ's Management HTTP API to inspect queues and peek at tasks.
type Inspector struct {
	mgmt    *MgmtClient
	conn    *amqp.Connection
	channel *amqp.Channel
}

// NewInspector creates an Inspector by parsing the AMQP URL and connecting
// to RabbitMQ's Management HTTP API (defaulting to port 15672).
func NewInspector(amqpURL string) (*Inspector, error) {
	u, err := url.Parse(amqpURL)
	if err != nil {
		return nil, fmt.Errorf("rmq: invalid amqp url: %w", err)
	}

	user := "guest"
	pass := "guest"
	if u.User != nil {
		user = u.User.Username()
		p, ok := u.User.Password()
		if ok {
			pass = p
		}
	}

	host := u.Hostname()
	if host == "" {
		host = "localhost"
	}
	mgmtURL := fmt.Sprintf("http://%s:15672", host)

	vhost := strings.TrimPrefix(u.Path, "/")
	if vhost == "" {
		vhost = "/"
	}

	conn, err := amqp.Dial(amqpURL)
	if err != nil {
		return nil, fmt.Errorf("rmq: failed to dial amqp for inspector: %w", err)
	}
	ch, err := conn.Channel()
	if err != nil {
		conn.Close()
		return nil, fmt.Errorf("rmq: failed to open amqp channel for inspector: %w", err)
	}

	return &Inspector{
		mgmt:    NewMgmtClient(mgmtURL, user, pass, vhost),
		conn:    conn,
		channel: ch,
	}, nil
}

func (i *Inspector) Close() error {
	i.channel.Close()
	return i.conn.Close()
}

// AllQueues returns all application queues, filtering out internal asynq queues.
func (i *Inspector) AllQueues() ([]string, error) {
	stats, err := i.mgmt.ListQueues()
	if err != nil {
		return nil, err
	}
	var queues []string
	seen := make(map[string]bool)
	for _, s := range stats {
		if strings.HasPrefix(s.Name, "asynq:") {
			continue
		}
		if !seen[s.Name] {
			seen[s.Name] = true
			queues = append(queues, s.Name)
		}
	}
	return queues, nil
}

// CurrentStats returns the stats for the given queue by inspecting RabbitMQ queues directly.
func (i *Inspector) CurrentStats(qname string) (*rdb.Stats, error) {
	q, err := i.channel.QueueDeclare(qname, true, false, false, false, nil)
	if err != nil {
		return nil, errors.E(errors.Op("rmq.CurrentStats"), errors.NotFound, &errors.QueueNotFoundError{Queue: qname})
	}

	archivedCount := 0
	if dlq, err := i.channel.QueueDeclare("asynq:dead_letter:"+qname, true, false, false, false, nil); err == nil {
		archivedCount = dlq.Messages
	}

	completedCount := 0
	if comp, err := i.channel.QueueDeclare("asynq:completed:"+qname, true, false, false, false, nil); err == nil {
		completedCount = comp.Messages
	}

	return &rdb.Stats{
		Queue:     qname,
		Size:      q.Messages + archivedCount + completedCount,
		Pending:   q.Messages,
		Active:    0,
		Archived:  archivedCount,
		Completed: completedCount,
		Timestamp: time.Now(),
	}, nil
}

// HistoricalStats returns empty stats for the last n days.
func (i *Inspector) HistoricalStats(qname string, n int) ([]*rdb.DailyStats, error) {
	var res []*rdb.DailyStats
	today := time.Now()
	for d := 0; d < n; d++ {
		date := today.AddDate(0, 0, -d)
		res = append(res, &rdb.DailyStats{
			Queue:     qname,
			Processed: 0,
			Failed:    0,
			Time:      date,
		})
	}
	return res, nil
}

// peekTasks peeks messages from a queue and unmarshals them into TaskInfo.
func (i *Inspector) peekTasks(qname string, count int, state base.TaskState) ([]*base.TaskInfo, error) {
	peeked, err := i.mgmt.Peek(qname, count)
	if err != nil {
		return nil, err
	}
	var tasks []*base.TaskInfo
	for _, p := range peeked {
		// Payload may be base64-encoded or raw string
		data := []byte(p.Payload)
		if decoded, err := base64.StdEncoding.DecodeString(p.Payload); err == nil {
			data = decoded
		}
		msg, err := base.DecodeMessage(data)
		if err != nil {
			continue
		}
		tasks = append(tasks, &base.TaskInfo{
			Message: msg,
			State:   state,
		})
	}
	return tasks, nil
}

func (i *Inspector) ListPending(qname string, pgn rdb.Pagination) ([]*base.TaskInfo, error) {
	count := pgn.Size
	if count <= 0 {
		count = 30
	}
	return i.peekTasks(qname, count, base.TaskStatePending)
}

func (i *Inspector) ListActive(qname string, pgn rdb.Pagination) ([]*base.TaskInfo, error) {
	return nil, nil
}

func (i *Inspector) ListScheduled(qname string, pgn rdb.Pagination) ([]*base.TaskInfo, error) {
	return nil, nil
}

func (i *Inspector) ListRetry(qname string, pgn rdb.Pagination) ([]*base.TaskInfo, error) {
	return nil, nil
}

func (i *Inspector) ListArchived(qname string, pgn rdb.Pagination) ([]*base.TaskInfo, error) {
	count := pgn.Size
	if count <= 0 {
		count = 30
	}
	return i.peekTasks("asynq:dead_letter:"+qname, count, base.TaskStateArchived)
}

func (i *Inspector) ListCompleted(qname string, pgn rdb.Pagination) ([]*base.TaskInfo, error) {
	count := pgn.Size
	if count <= 0 {
		count = 30
	}
	return i.peekTasks("asynq:completed:"+qname, count, base.TaskStateCompleted)
}

func (i *Inspector) ListAggregating(qname, gname string, pgn rdb.Pagination) ([]*base.TaskInfo, error) {
	return nil, nil
}

func (i *Inspector) GroupStats(qname string) ([]*rdb.GroupStat, error) {
	return nil, nil
}

func (i *Inspector) GetTaskInfo(qname, id string) (*base.TaskInfo, error) {
	// Search through pending and archived tasks
	tasks, err := i.ListPending(qname, rdb.Pagination{Size: 100})
	if err == nil {
		for _, t := range tasks {
			if t.Message.ID == id {
				return t, nil
			}
		}
	}
	archived, err := i.ListArchived(qname, rdb.Pagination{Size: 100})
	if err == nil {
		for _, t := range archived {
			if t.Message.ID == id {
				return t, nil
			}
		}
	}
	return nil, errors.E(errors.Op("rmq.GetTaskInfo"), errors.NotFound, &errors.TaskNotFoundError{Queue: qname, ID: id})
}

func (i *Inspector) DeleteTask(qname, id string) error {
	return errors.ErrNotSupported
}

func (i *Inspector) DeleteAllPendingTasks(qname string) (int64, error) {
	n, err := i.channel.QueuePurge(qname, false)
	return int64(n), err
}

func (i *Inspector) DeleteAllScheduledTasks(qname string) (int64, error) {
	return 0, errors.ErrNotSupported
}

func (i *Inspector) DeleteAllRetryTasks(qname string) (int64, error) {
	return 0, errors.ErrNotSupported
}

func (i *Inspector) DeleteAllArchivedTasks(qname string) (int64, error) {
	n, err := i.channel.QueuePurge("asynq:dead_letter:"+qname, false)
	return int64(n), err
}

func (i *Inspector) DeleteAllCompletedTasks(qname string) (int64, error) {
	n, err := i.channel.QueuePurge("asynq:completed:"+qname, false)
	return int64(n), err
}

func (i *Inspector) DeleteAllAggregatingTasks(qname, gname string) (int64, error) {
	return 0, errors.ErrNotSupported
}

func (i *Inspector) ArchiveTask(qname, id string) error {
	return errors.ErrNotSupported
}

func (i *Inspector) ArchiveAllPendingTasks(qname string) (int64, error) {
	return 0, errors.ErrNotSupported
}

func (i *Inspector) ArchiveAllScheduledTasks(qname string) (int64, error) {
	return 0, errors.ErrNotSupported
}

func (i *Inspector) ArchiveAllRetryTasks(qname string) (int64, error) {
	return 0, errors.ErrNotSupported
}

func (i *Inspector) ArchiveAllAggregatingTasks(qname, gname string) (int64, error) {
	return 0, errors.ErrNotSupported
}

func (i *Inspector) RunTask(qname, id string) error {
	return errors.ErrNotSupported
}

func (i *Inspector) RunAllScheduledTasks(qname string) (int64, error) {
	return 0, errors.ErrNotSupported
}

func (i *Inspector) RunAllRetryTasks(qname string) (int64, error) {
	return 0, errors.ErrNotSupported
}

func (i *Inspector) RunAllArchivedTasks(qname string) (int64, error) {
	return 0, errors.ErrNotSupported
}

func (i *Inspector) RunAllAggregatingTasks(qname, gname string) (int64, error) {
	return 0, errors.ErrNotSupported
}

func (i *Inspector) Pause(qname string) error {
	return errors.ErrNotSupported
}

func (i *Inspector) Unpause(qname string) error {
	return errors.ErrNotSupported
}

func (i *Inspector) RemoveQueue(qname string, force bool) error {
	return errors.ErrNotSupported
}

func (i *Inspector) ListServers() ([]*base.ServerInfo, error) {
	return nil, nil
}

func (i *Inspector) ListWorkers() ([]*base.WorkerInfo, error) {
	return nil, nil
}

func (i *Inspector) ClusterKeySlot(qname string) (int64, error) {
	return 0, nil
}

func (i *Inspector) ClusterNodes(qname string) ([]redis.ClusterNode, error) {
	return nil, nil
}

func (i *Inspector) ListLeaseExpired(cutoff time.Time, qnames ...string) ([]*base.TaskMessage, error) {
	return nil, nil
}

func (i *Inspector) ListSchedulerEntries() ([]*base.SchedulerEntry, error) {
	return nil, nil
}

func (i *Inspector) ListSchedulerEnqueueEvents(entryID string, pgn rdb.Pagination) ([]*base.SchedulerEnqueueEvent, error) {
	return nil, nil
}

func (i *Inspector) UpdateTaskPayload(qname, id string, payload []byte) error {
	return errors.ErrNotSupported
}

func (i *Inspector) PublishCancelation(id string) error {
	return errors.ErrNotSupported
}
