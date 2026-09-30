package rmq_test

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/hibiken/asynq/internal/base"
	"github.com/hibiken/asynq/internal/errors"
	"github.com/hibiken/asynq/internal/rmq"
)

const amqpURL = "amqp://guest:guest@localhost:5672/"

func newTestBroker(t *testing.T) *rmq.Broker {
	t.Helper()
	b, err := rmq.NewBroker(amqpURL)
	if err != nil {
		t.Fatalf("failed to connect to rabbitmq: %v", err)
	}
	t.Cleanup(func() { b.Close() })
	return b
}

func TestPing(t *testing.T) {
	b := newTestBroker(t)
	if err := b.Ping(); err != nil {
		t.Fatalf("Ping failed: %v", err)
	}
}

func TestEnqueueAndDequeue(t *testing.T) {
	b := newTestBroker(t)
	ctx := context.Background()
	qname := "test_q_" + uuid.NewString()

	msg := &base.TaskMessage{
		ID:      uuid.NewString(),
		Type:    "email:send",
		Payload: []byte(`{"user":"alice"}`),
		Queue:   qname,
		Retry:   3,
	}

	if err := b.Enqueue(ctx, msg); err != nil {
		t.Fatalf("Enqueue failed: %v", err)
	}

	dequeued, lease, err := b.Dequeue(qname)
	if err != nil {
		t.Fatalf("Dequeue failed: %v", err)
	}
	if dequeued.ID != msg.ID {
		t.Errorf("got task ID %q, want %q", dequeued.ID, msg.ID)
	}
	if string(dequeued.Payload) != string(msg.Payload) {
		t.Errorf("got payload %q, want %q", dequeued.Payload, msg.Payload)
	}
	if lease.Before(time.Now()) {
		t.Errorf("lease expiration %v should be in the future", lease)
	}

	if err := b.Done(ctx, dequeued); err != nil {
		t.Fatalf("Done failed: %v", err)
	}

	// Queue should now be empty
	_, _, err = b.Dequeue(qname)
	if !errors.Is(err, errors.ErrNoProcessableTask) {
		t.Errorf("got err %v, want ErrNoProcessableTask", err)
	}
}

func TestScheduledTask(t *testing.T) {
	b := newTestBroker(t)
	ctx := context.Background()
	qname := "test_sched_" + uuid.NewString()

	msg := &base.TaskMessage{
		ID:      uuid.NewString(),
		Type:    "reminder",
		Payload: []byte(`{"time":"soon"}`),
		Queue:   qname,
		Retry:   1,
	}

	// Schedule 1.5 seconds in the future
	processAt := time.Now().Add(1500 * time.Millisecond)
	if err := b.Schedule(ctx, msg, processAt); err != nil {
		t.Fatalf("Schedule failed: %v", err)
	}

	// Should not be immediately available
	_, _, err := b.Dequeue(qname)
	if !errors.Is(err, errors.ErrNoProcessableTask) {
		t.Errorf("expected queue to be empty immediately, got: %v", err)
	}

	// Wait for delayed exchange to route it
	time.Sleep(2 * time.Second)

	dequeued, _, err := b.Dequeue(qname)
	if err != nil {
		t.Fatalf("Dequeue after delay failed: %v", err)
	}
	if dequeued.ID != msg.ID {
		t.Errorf("got task ID %q, want %q", dequeued.ID, msg.ID)
	}
	_ = b.Done(ctx, dequeued)
}

func TestRetryAndDeadLetter(t *testing.T) {
	b := newTestBroker(t)
	ctx := context.Background()
	qname := "test_retry_" + uuid.NewString()

	msg := &base.TaskMessage{
		ID:      uuid.NewString(),
		Type:    "failing:task",
		Payload: []byte(`{}`),
		Queue:   qname,
		Retry:   1, // max 1 retry
		Retried: 0,
	}

	// First failure: should re-schedule
	if err := b.Retry(ctx, msg, time.Now(), "transient error", true); err != nil {
		t.Fatalf("Retry failed: %v", err)
	}
	if msg.Retried != 1 {
		t.Errorf("msg.Retried = %d, want 1", msg.Retried)
	}

	// Pull from queue
	dequeued, _, err := b.Dequeue(qname)
	if err != nil {
		t.Fatalf("Dequeue retried task failed: %v", err)
	}
	_ = b.Done(ctx, dequeued)

	// Second failure: retries exhausted -> should go to DLQ
	if err := b.Retry(ctx, dequeued, time.Now(), "fatal error", true); err != nil {
		t.Fatalf("Retry (archive) failed: %v", err)
	}

	// Main queue should be empty
	_, _, err = b.Dequeue(qname)
	if !errors.Is(err, errors.ErrNoProcessableTask) {
		t.Errorf("expected main queue to be empty, got %v", err)
	}
}
