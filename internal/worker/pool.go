package worker

import (
	"context"
	"log"
	"sync"
	"time"

	"github.com/sablelight/job-queue/internal/queue"
)

// HandlerFunc is a function that processes a job.
type HandlerFunc func(*queue.Job) error

// Pool manages a pool of workers processing jobs from a queue.
type Pool struct {
	queue     *queue.Queue
	handlers  map[string]HandlerFunc
	workers   int
	pollInterval time.Duration
	claimCount int

	mu       sync.RWMutex
	running  bool
	wg       sync.WaitGroup
	stopCh   chan struct{}
}

// NewPool creates a new worker pool.
func NewPool(q *queue.Queue, workers int, pollInterval time.Duration, claimCount int) *Pool {
	return &Pool{
		queue:        q,
		handlers:     make(map[string]HandlerFunc),
		workers:      workers,
		pollInterval: pollInterval,
		claimCount:   claimCount,
		stopCh:       make(chan struct{}),
	}
}

// Register registers a handler for a job type.
func (p *Pool) Register(jobType string, handler HandlerFunc) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.handlers[jobType] = handler
}

// Start begins processing jobs.
func (p *Pool) Start(ctx context.Context) error {
	p.mu.Lock()
	if p.running {
		p.mu.Unlock()
		return nil
	}
	p.running = true
	p.mu.Unlock()

	// Ensure consumer group exists
	if err := p.queue.EnsureGroup(ctx); err != nil {
		return err
	}

	for i := 0; i < p.workers; i++ {
		p.wg.Add(1)
		go p.worker(ctx, i)
	}

	log.Printf("Started %d workers", p.workers)
	return nil
}

// Stop gracefully shuts down the pool.
func (p *Pool) Stop() {
	p.mu.Lock()
	if !p.running {
		p.mu.Unlock()
		return
	}
	p.running = false
	close(p.stopCh)
	p.mu.Unlock()

	p.wg.Wait()
	log.Println("All workers stopped")
}

func (p *Pool) worker(ctx context.Context, id int) {
	defer p.wg.Done()

	ticker := time.NewTicker(p.pollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-p.stopCh:
			return
		case <-ticker.C:
			p.processBatch(ctx)
		}
	}
}

func (p *Pool) processBatch(ctx context.Context) {
	jobs, err := p.queue.Dequeue(ctx, p.claimCount, 5*time.Second)
	if err != nil {
		log.Printf("worker dequeue error: %v", err)
		return
	}

	for _, job := range jobs {
		p.mu.RLock()
		handler, ok := p.handlers[job.Type]
		p.mu.RUnlock()

		if !ok {
			log.Printf("no handler for job type %s, sending to dead letter", job.Type)
			p.queue.DeadLetter(ctx, job)
			continue
		}

		if err := p.queue.Handle(ctx, job, handler); err != nil {
			log.Printf("job %s handling error: %v", job.ID, err)
		}
	}
}