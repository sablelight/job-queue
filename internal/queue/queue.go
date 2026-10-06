package queue

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/redis/go-redis/v9"
)

// Job represents a unit of work.
type Job struct {
	ID        string                 `json:"id"`
	Type      string                 `json:"type"`
	Payload   map[string]any         `json:"payload"`
	Priority  int                    `json:"priority"` // higher = more urgent
	Retries   int                    `json:"retries"`
	MaxRetries int                   `json:"max_retries"`
	CreatedAt time.Time              `json:"created_at"`
	StartedAt *time.Time             `json:"started_at,omitempty"`
	CompletedAt *time.Time           `json:"completed_at,omitempty"`
	Error     string                 `json:"error,omitempty"`
}

// Queue manages job enqueueing and dequeueing via Redis streams.
type Queue struct {
	client     *redis.Client
	streamKey  string
	groupName  string
	consumerName string
	deadLetterKey string
}

func New(client *redis.Client, streamKey, groupName, consumerName string) *Queue {
	return &Queue{
		client:        client,
		streamKey:     streamKey,
		groupName:     groupName,
		consumerName:  consumerName,
		deadLetterKey: streamKey + ":dead",
	}
}

// EnsureGroup creates the consumer group if it doesn't exist.
func (q *Queue) EnsureGroup(ctx context.Context) error {
	err := q.client.XGroupCreateMkStream(ctx, q.streamKey, q.groupName, "0").Err()
	if err != nil && err.Error() != "BUSYGROUP Consumer Group name already exists" {
		return err
	}
	return nil
}

// Enqueue adds a job to the stream.
func (q *Queue) Enqueue(ctx context.Context, job *Job) error {
	if job.ID == "" {
		job.ID = fmt.Sprintf("job-%d-%s", time.Now().UnixNano(), randomString(8))
	}
	job.CreatedAt = time.Now()
	if job.MaxRetries == 0 {
		job.MaxRetries = 3
	}

	data, err := json.Marshal(job)
	if err != nil {
		return err
	}

	args := &redis.XAddArgs{
		Stream: q.streamKey,
		Values: map[string]interface{}{
			"data": string(data),
		},
	}
	return q.client.XAdd(ctx, args).Err()
}

// Dequeue blocks until a job is available, then claims it for this consumer.
func (q *Queue) Dequeue(ctx context.Context, count int, block time.Duration) ([]*Job, error) {
	streams, err := q.client.XReadGroup(ctx, &redis.XReadGroupArgs{
		Group:    q.groupName,
		Consumer: q.consumerName,
		Streams:  []string{q.streamKey, ">"},
		Count:    int64(count),
		Block:    block,
	}).Result()
	if err != nil {
		if err == redis.Nil {
			return nil, nil
		}
		return nil, err
	}

	var jobs []*Job
	for _, stream := range streams {
		for _, msg := range stream.Messages {
			var job Job
			if data, ok := msg.Values["data"].(string); ok {
				if err := json.Unmarshal([]byte(data), &job); err == nil {
					job.ID = msg.ID // use stream entry ID for ack
					jobs = append(jobs, &job)
				}
			}
		}
	}
	return jobs, nil
}

// Ack acknowledges a job as completed.
func (q *Queue) Ack(ctx context.Context, jobIDs ...string) error {
	return q.client.XAck(ctx, q.streamKey, q.groupName, jobIDs...).Err()
}

// Retry requeues a job with incremented retry count.
func (q *Queue) Retry(ctx context.Context, job *Job) error {
	job.Retries++
	job.Error = ""
	return q.Enqueue(ctx, job)
}

// DeadLetter moves a job to the dead-letter stream.
func (q *Queue) DeadLetter(ctx context.Context, job *Job) error {
	data, err := json.Marshal(job)
	if err != nil {
		return err
	}
	return q.client.XAdd(ctx, &redis.XAddArgs{
		Stream: q.deadLetterKey,
		Values: map[string]interface{}{
			"data":      string(data),
			"failed_at": time.Now().Format(time.RFC3339),
		},
	}).Err()
}

// Handle processes a job with retry/dead-letter logic.
func (q *Queue) Handle(ctx context.Context, job *Job, handler func(*Job) error) error {
	if err := handler(job); err != nil {
		log.Printf("job %s failed: %v", job.ID, err)
		job.Error = err.Error()

		if job.Retries >= job.MaxRetries {
			return q.DeadLetter(ctx, job)
		}
		return q.Retry(ctx, job)
	}
	return q.Ack(ctx, job.ID)
}

func randomString(n int) string {
	const letters = "abcdefghijklmnopqrstuvwxyz0123456789"
	b := make([]byte, n)
	for i := range b {
		b[i] = letters[time.Now().UnixNano()%int64(len(letters))]
		time.Sleep(time.Nanosecond)
	}
	return string(b)
}