package rmq_test

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/hibiken/asynq/internal/base"
	"github.com/hibiken/asynq/internal/rmq"
)

func TestMgmtClientListAndPeek(t *testing.T) {
	b := newTestBroker(t)
	mgmt := rmq.NewMgmtClient("http://localhost:15672", "guest", "guest", "/")

	qname := "test_mgmt_" + uuid.NewString()
	ctx := context.Background()

	msg := &base.TaskMessage{
		ID:      uuid.NewString(),
		Type:    "mgmt:test",
		Payload: []byte(`{"sample":"data"}`),
		Queue:   qname,
		Retry:   3,
	}
	if err := b.Enqueue(ctx, msg); err != nil {
		t.Fatalf("Enqueue failed: %v", err)
	}

	var stat *rmq.QueueStat
	var err error
	for i := 0; i < 10; i++ {
		stat, err = mgmt.GetQueue(qname)
		if err == nil && stat != nil && stat.Messages >= 1 {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if err != nil {
		t.Fatalf("GetQueue failed: %v", err)
	}
	if stat == nil {
		t.Fatalf("expected queue %q to exist", qname)
	}
	if stat.Messages < 1 {
		t.Errorf("expected at least 1 message, got %d", stat.Messages)
	}

	peeked, err := mgmt.Peek(qname, 5)
	if err != nil {
		t.Fatalf("Peek failed: %v", err)
	}
	if len(peeked) == 0 {
		t.Fatalf("expected to peek at least 1 message, got %d", len(peeked))
	}

	_ = mgmt.PurgeQueue(qname)
}
