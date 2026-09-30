package asynq_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/hibiken/asynq"
)

const rabbitmqURL = "amqp://guest:guest@localhost:5672/"

func TestRabbitMQClientAndServerEndToEnd(t *testing.T) {
	qname := "rmq_test_queue_" + uuid.NewString()

	// 1. Create client
	client := asynq.NewClient(asynq.RabbitMQClientOpt{URL: rabbitmqURL})
	defer client.Close()

	// 2. Enqueue immediate task
	task := asynq.NewTask("send:welcome_email", []byte(`{"user_id":"12345"}`))
	info, err := client.Enqueue(task, asynq.Queue(qname))
	if err != nil {
		t.Fatalf("client.Enqueue failed: %v", err)
	}
	if info.Type != "send:welcome_email" {
		t.Errorf("got type %q, want 'send:welcome_email'", info.Type)
	}

	// 3. Enqueue scheduled task (delay 2s)
	schedTask := asynq.NewTask("send:reminder_email", []byte(`{"reminder_id":"999"}`))
	_, err = client.Enqueue(schedTask, asynq.Queue(qname), asynq.ProcessIn(2*time.Second))
	if err != nil {
		t.Fatalf("client.Enqueue with delay failed: %v", err)
	}

	// 4. Start Server
	srv := asynq.NewServer(
		asynq.RabbitMQClientOpt{URL: rabbitmqURL},
		asynq.Config{
			Concurrency: 5,
			Queues: map[string]int{
				qname: 1,
			},
		},
	)

	var mu sync.Mutex
	processed := make(map[string]bool)

	mux := asynq.NewServeMux()
	mux.HandleFunc("send:welcome_email", func(ctx context.Context, t *asynq.Task) error {
		mu.Lock()
		defer mu.Unlock()
		processed[t.Type()] = true
		return nil
	})
	mux.HandleFunc("send:reminder_email", func(ctx context.Context, t *asynq.Task) error {
		mu.Lock()
		defer mu.Unlock()
		processed[t.Type()] = true
		return nil
	})

	if err := srv.Start(mux); err != nil {
		t.Fatalf("srv.Start failed: %v", err)
	}
	defer srv.Shutdown()

	// Wait for immediate task to process
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		mu.Lock()
		ok := processed["send:welcome_email"]
		mu.Unlock()
		if ok {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	mu.Lock()
	if !processed["send:welcome_email"] {
		mu.Unlock()
		t.Fatal("immediate task 'send:welcome_email' was not processed in time")
	}
	mu.Unlock()

	// Wait for scheduled task to process (was delayed by 2s)
	deadline = time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		mu.Lock()
		ok := processed["send:reminder_email"]
		mu.Unlock()
		if ok {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	mu.Lock()
	if !processed["send:reminder_email"] {
		mu.Unlock()
		t.Fatal("scheduled task 'send:reminder_email' was not processed in time")
	}
	mu.Unlock()
}
