package main

import (
	"context"
	"log"
	"os"

	"github.com/sablelight/job-queue/internal/config"
	"github.com/sablelight/job-queue/internal/queue"
	"github.com/redis/go-redis/v9"
)

func main() {
	redisAddr := config.RegisterOption("redis.addr", "Redis address", "localhost:6379")
	streamKey := config.RegisterOption("stream", "Redis stream key", "jobs")

	client := redis.NewClient(&redis.Options{Addr: redisAddr.GetString()})
	defer client.Close()

	q := queue.New(client, streamKey.GetString(), "workers", "enqueue-cli")

	// Add some test jobs
	jobs := []*queue.Job{
		{Type: "send_email", Payload: map[string]any{"to": "user@example.com", "subject": "Welcome!", "body": "Hello there"}},
		{Type: "process_webhook", Payload: map[string]any{"url": "https://example.com/webhook", "data": map[string]string{"event": "user.signup"}}},
		{Type: "generate_report", Payload: map[string]any{"report_id": "rpt-123", "format": "pdf"}},
		{Type: "send_email", Payload: map[string]any{"to": "admin@example.com", "subject": "Alert", "body": "High CPU usage"}},
	}

	for _, job := range jobs {
		if err := q.Enqueue(context.Background(), job); err != nil {
			log.Fatalf("enqueue failed: %v", err)
		}
		log.Printf("Enqueued job %s: %s", job.ID, job.Type)
	}
}