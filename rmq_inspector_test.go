package asynq_test

import (
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/yaziedda/asynq"
)

func TestRabbitMQInspector(t *testing.T) {
	opt := asynq.RabbitMQClientOpt{URL: "amqp://guest:guest@localhost:5672/"}
	client := asynq.NewClient(opt)
	defer client.Close()

	inspector := asynq.NewInspector(opt)
	defer inspector.Close()

	qname := "insp_test_" + uuid.NewString()

	// 1. Enqueue 3 tasks
	for i := 0; i < 3; i++ {
		task := asynq.NewTask("send:email", []byte(`{"user":"bob"}`))
		if _, err := client.Enqueue(task, asynq.Queue(qname)); err != nil {
			t.Fatalf("Enqueue failed: %v", err)
		}
	}

	// 2. Inspector should list the queue
	queues, err := inspector.Queues()
	if err != nil {
		t.Fatalf("inspector.Queues failed: %v", err)
	}
	found := false
	for _, q := range queues {
		if q == qname {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("expected queue %q to be in queues %v", qname, queues)
	}

	// 3. Inspect tasks in queue (peek)
	tasks, err := inspector.ListPendingTasks(qname)
	if err != nil {
		t.Fatalf("ListPendingTasks failed: %v", err)
	}
	if len(tasks) < 3 {
		t.Errorf("expected at least 3 pending tasks, got %d", len(tasks))
	}
	if len(tasks) > 0 && tasks[0].Type != "send:email" {
		t.Errorf("got task type %q, want 'send:email'", tasks[0].Type)
	}

	// 4. Check QueueInfo
	// Note: give RabbitMQ stats emitter a tiny window to refresh counts
	var info *asynq.QueueInfo
	for attempt := 0; attempt < 10; attempt++ {
		info, err = inspector.GetQueueInfo(qname)
		if err == nil && info.Pending >= 3 {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if err != nil {
		t.Fatalf("GetQueueInfo failed: %v", err)
	}
	if info.Pending < 3 {
		t.Errorf("expected pending >= 3, got %d", info.Pending)
	}

	// 5. Delete all pending tasks (Purge)
	deleted, err := inspector.DeleteAllPendingTasks(qname)
	if err != nil {
		t.Fatalf("DeleteAllPendingTasks failed: %v", err)
	}
	if deleted < 3 {
		t.Logf("purged %d messages", deleted)
	}

	// 6. After purge, pending tasks should be 0
	tasksAfter, err := inspector.ListPendingTasks(qname)
	if err != nil {
		t.Fatalf("ListPendingTasks after purge failed: %v", err)
	}
	if len(tasksAfter) != 0 {
		t.Errorf("expected 0 pending tasks after purge, got %d", len(tasksAfter))
	}
}
