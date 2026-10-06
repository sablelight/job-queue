package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/sablelight/job-queue/internal/config"
	"github.com/sablelight/job-queue/internal/queue"
	"github.com/sablelight/job-queue/internal/worker"
	"github.com/redis/go-redis/v9"
)

func main() {
	redisAddr := config.RegisterOption("redis.addr", "Redis address", "localhost:6379")
	streamKey := config.RegisterOption("stream", "Redis stream key", "jobs")
	groupName := config.RegisterOption("group", "Consumer group name", "workers")
	consumerName := config.RegisterOption("consumer", "Consumer name (unique per instance)", "worker-1")
	workers := config.RegisterOption("workers", "Number of worker goroutines", "4")
	pollInterval := config.RegisterOption("poll", "Poll interval (ms)", "1000")
	claimCount := config.RegisterOption("claim", "Jobs to claim per poll", "10")

	log.Printf("Starting job-queue worker")
	log.Printf("  redis: %s, stream: %s", redisAddr.GetString(), streamKey.GetString())
	log.Printf("  group: %s, consumer: %s", groupName.GetString(), consumerName.GetString())
	log.Printf("  workers: %d, poll: %dms, claim: %d", workers.GetInt(), pollInterval.GetInt(), claimCount.GetInt())

	client := redis.NewClient(&redis.Options{Addr: redisAddr.GetString()})
	defer client.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	q := queue.New(client, streamKey.GetString(), groupName.GetString(), consumerName.GetString())

	pool := worker.NewPool(q, workers.GetInt(), time.Duration(pollInterval.GetInt())*time.Millisecond, claimCount.GetInt())

	// Register example handlers
	pool.Register("send_email", func(job *queue.Job) error {
		log.Printf("Sending email: %v", job.Payload)
		time.Sleep(100 * time.Millisecond) // simulate work
		return nil
	})

	pool.Register("process_webhook", func(job *queue.Job) error {
		log.Printf("Processing webhook: %v", job.Payload)
		time.Sleep(50 * time.Millisecond)
		return nil
	})

	pool.Register("generate_report", func(job *queue.Job) error {
		log.Printf("Generating report: %v", job.Payload)
		time.Sleep(500 * time.Millisecond)
		return nil
	})

	if err := pool.Start(ctx); err != nil {
		log.Fatalf("pool start: %v", err)
	}

	// Handle shutdown
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)
	<-stop
	log.Println("Shutting down...")

	cancel()
	pool.Stop()
	log.Println("Stopped")
}