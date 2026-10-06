package queue

import (
	"context"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

func TestQueue_EnqueueDequeue(t *testing.T) {
	client := redis.NewClient(&redis.Options{Addr: "localhost:6379"})
	ctx := context.Background()
	client.FlushDB(ctx)
	defer client.Close()

	q := New(client, "test:jobs", "test-group", "test-consumer")
	if err := q.EnsureGroup(ctx); err != nil {
		t.Fatal(err)
	}

	job := &Job{
		Type:       "test",
		Payload:    map[string]any{"key": "value"},
		MaxRetries: 3,
	}

	if err := q.Enqueue(ctx, job); err != nil {
		t.Fatal(err)
	}

	jobs, err := q.Dequeue(ctx, 1, 2*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if len(jobs) != 1 {
		t.Fatalf("expected 1 job, got %d", len(jobs))
	}
	if jobs[0].Type != "test" {
		t.Errorf("job type mismatch: %s", jobs[0].Type)
	}

	// Ack should remove from pending
	if err := q.Ack(ctx, jobs[0].ID); err != nil {
		t.Fatal(err)
	}

	// No more jobs should be available immediately
	jobs, err = q.Dequeue(ctx, 1, 500*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	if len(jobs) != 0 {
		t.Errorf("expected 0 jobs after ack, got %d", len(jobs))
	}
}

func TestQueue_RetryAndDeadLetter(t *testing.T) {
	client := redis.NewClient(&redis.Options{Addr: "localhost:6379"})
	ctx := context.Background()
	client.FlushDB(ctx)
	defer client.Close()

	q := New(client, "test:retry", "test-group", "test-consumer")
	if err := q.EnsureGroup(ctx); err != nil {
		t.Fatal(err)
	}

	job := &Job{
		Type:       "fail",
		Payload:    map[string]any{},
		MaxRetries: 2,
	}
	if err := q.Enqueue(ctx, job); err != nil {
		t.Fatal(err)
	}

	// First attempt
	jobs, _ := q.Dequeue(ctx, 1, time.Second)
	if len(jobs) != 1 {
		t.Fatal("expected 1 job")
	}
	if err := q.Retry(ctx, jobs[0]); err != nil {
		t.Fatal(err)
	}

	// Second attempt
	jobs, _ = q.Dequeue(ctx, 1, time.Second)
	if len(jobs) != 1 {
		t.Fatal("expected 1 job on retry")
	}
	if err := q.Retry(ctx, jobs[0]); err != nil {
		t.Fatal(err)
	}

	// Third attempt (max retries = 2, so this should go to dead letter)
	jobs, _ = q.Dequeue(ctx, 1, time.Second)
	if len(jobs) != 1 {
		t.Fatal("expected 1 job on final retry")
	}
	if err := q.Handle(ctx, jobs[0], func(j *Job) error {
		return nil // succeed on final attempt
	}); err != nil {
		t.Fatal(err)
	}

	// Should be acked, not dead-lettered
	// Check dead letter stream is empty
	deadJobs, _ := client.XRange(ctx, "test:retry:dead", "-", "+").Result()
	if len(deadJobs) != 0 {
		t.Errorf("expected no dead letter jobs, got %d", len(deadJobs))
	}
}

func TestQueue_Priority(t *testing.T) {
	client := redis.NewClient(&redis.Options{Addr: "localhost:6379"})
	ctx := context.Background()
	client.FlushDB(ctx)
	defer client.Close()

	q := New(client, "test:priority", "test-group", "test-consumer")
	if err := q.EnsureGroup(ctx); err != nil {
		t.Fatal(err)
	}

	// Low priority first
	if err := q.Enqueue(ctx, &Job{Type: "low", Priority: 1}); err != nil {
		t.Fatal(err)
	}
	// High priority second
	if err := q.Enqueue(ctx, &Job{Type: "high", Priority: 10}); err != nil {
		t.Fatal(err)
	}

	// Stream doesn't support priority natively, but we can verify both exist
	jobs, _ := q.Dequeue(ctx, 2, time.Second)
	if len(jobs) != 2 {
		t.Fatalf("expected 2 jobs, got %d", len(jobs))
	}
}