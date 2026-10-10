package scheduler

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/model-scheduler/model-scheduler/internal/model"
	"github.com/model-scheduler/model-scheduler/internal/queue"
)

var (
	// ErrQueueFull indicates the scheduler's bounded queue cannot accept more requests.
	ErrQueueFull = errors.New("scheduler queue is full")
	// ErrWaitTimeout indicates the request exceeded its maximum permissible queue wait time.
	ErrWaitTimeout = errors.New("maximum queue wait time exceeded")
)

// Config defines the scheduler operating limits.
type Config struct {
	MaxConcurrency int                    `json:"max_concurrency"`
	MaxQueueSize   int                    `json:"max_queue_size"`
	MaxQueueWait   time.Duration          `json:"max_queue_wait"`
	Policy         queue.SchedulingPolicy `json:"policy"`
}

// Stats holds snapshot statistics of scheduler operations.
type Stats struct {
	QueuedRequests   int   `json:"queued_requests"`
	ActiveRequests   int   `json:"active_requests"`
	MaxQueueCapacity int   `json:"max_queue_capacity"`
	MaxConcurrency   int   `json:"max_concurrency"`
	TotalRequests    int64 `json:"total_requests"`
	TotalAdmitted    int64 `json:"total_admitted"`
	TotalCompleted   int64 `json:"total_completed"`
	TotalRejected    int64 `json:"total_rejected"`
	TotalCanceled    int64 `json:"total_canceled"`
	TotalTimedOut    int64 `json:"total_timed_out"`
}

// Lease represents granted execution capacity for a single upstream request.
type Lease struct {
	scheduler    *Scheduler
	meta         model.RequestMetadata
	waitDuration time.Duration
	released     atomic.Bool
}

// WaitDuration returns how long the request waited in the queue before being admitted.
func (l *Lease) WaitDuration() time.Duration {
	return l.waitDuration
}

// Release yields the execution slot back to the scheduler.
// Must be called exactly once when request processing completes.
func (l *Lease) Release() {
	if l.released.CompareAndSwap(false, true) {
		l.scheduler.releaseSlot()
		l.scheduler.totalCompleted.Add(1)
	}
}

// Scheduler coordinates request admission, bounded priority queuing, and concurrency.
type Scheduler struct {
	cfg Config
	pq  *queue.PriorityQueue

	mu     sync.Mutex
	active int

	totalRequests  atomic.Int64
	totalAdmitted  atomic.Int64
	totalCompleted atomic.Int64
	totalRejected  atomic.Int64
	totalCanceled  atomic.Int64
	totalTimedOut  atomic.Int64
}

// NewScheduler creates an initialized Scheduler.
func NewScheduler(cfg Config) *Scheduler {
	if cfg.MaxConcurrency <= 0 {
		cfg.MaxConcurrency = 5
	}
	if cfg.MaxQueueSize <= 0 {
		cfg.MaxQueueSize = 100
	}
	if cfg.MaxQueueWait <= 0 {
		cfg.MaxQueueWait = 30 * time.Second
	}
	if cfg.Policy == "" {
		cfg.Policy = queue.PolicyPriority
	}

	return &Scheduler{
		cfg: cfg,
		pq:  queue.NewPriorityQueueWithPolicy(cfg.MaxQueueSize, cfg.Policy),
	}
}

// Acquire requests an execution slot for an incoming request.
// If concurrency is available and the queue is empty, the slot is granted immediately.
// Otherwise, the request is placed into the bounded priority queue.
// Returns a Lease on success, which the caller MUST release.
func (s *Scheduler) Acquire(ctx context.Context, meta model.RequestMetadata) (*Lease, error) {
	s.totalRequests.Add(1)
	start := time.Now()

	s.mu.Lock()
	// Direct admission: available concurrency slot and no waiting queue items ahead
	if s.active < s.cfg.MaxConcurrency && s.pq.Len() == 0 {
		s.active++
		s.totalAdmitted.Add(1)
		s.mu.Unlock()
		return &Lease{
			scheduler:    s,
			meta:         meta,
			waitDuration: 0,
		}, nil
	}

	// Must enqueue into priority queue
	item := queue.NewItem(meta.RequestID, meta, ctx)
	if err := s.pq.Enqueue(item); err != nil {
		s.mu.Unlock()
		s.totalRejected.Add(1)
		return nil, ErrQueueFull
	}
	s.mu.Unlock()

	timer := time.NewTimer(s.cfg.MaxQueueWait)
	defer timer.Stop()

	// Wait for slot assignment, context cancellation, or queue timeout
	select {
	case <-item.Ready:
		if err := ctx.Err(); err != nil {
			s.releaseSlot()
			s.totalCanceled.Add(1)
			return nil, err
		}
		s.totalAdmitted.Add(1)
		lease := &Lease{
			scheduler:    s,
			meta:         meta,
			waitDuration: time.Since(start),
		}
		return lease, nil

	case <-ctx.Done():
		// Client disconnected or parent context canceled
		_, err := s.pq.Remove(item.ID)
		if err == nil {
			s.totalCanceled.Add(1)
			return nil, ctx.Err()
		}
		// Item was already popped by a releasing worker: wait for ready and release slot
		<-item.Ready
		s.releaseSlot()
		s.totalCanceled.Add(1)
		return nil, ctx.Err()

	case <-timer.C:
		// Queue wait timeout
		_, err := s.pq.Remove(item.ID)
		if err == nil {
			s.totalTimedOut.Add(1)
			return nil, ErrWaitTimeout
		}
		<-item.Ready
		s.releaseSlot()
		s.totalTimedOut.Add(1)
		return nil, ErrWaitTimeout
	}
}

// releaseSlot yields an active concurrency slot.
// If items are waiting in the queue, the next item is granted the slot.
// Otherwise, active concurrency is decremented.
func (s *Scheduler) releaseSlot() {
	s.mu.Lock()
	defer s.mu.Unlock()

	next := s.pq.Dequeue()
	if next == nil {
		s.active--
		return
	}

	// Slot transferred directly to dequeued item
	close(next.Ready)
}

// Stats returns a snapshot of scheduler metrics.
func (s *Scheduler) Stats() Stats {
	s.mu.Lock()
	active := s.active
	queued := s.pq.Len()
	s.mu.Unlock()

	return Stats{
		QueuedRequests:   queued,
		ActiveRequests:   active,
		MaxQueueCapacity: s.cfg.MaxQueueSize,
		MaxConcurrency:   s.cfg.MaxConcurrency,
		TotalRequests:    s.totalRequests.Load(),
		TotalAdmitted:    s.totalAdmitted.Load(),
		TotalCompleted:   s.totalCompleted.Load(),
		TotalRejected:    s.totalRejected.Load(),
		TotalCanceled:    s.totalCanceled.Load(),
		TotalTimedOut:    s.totalTimedOut.Load(),
	}
}
