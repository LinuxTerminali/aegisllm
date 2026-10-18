package scheduler

import (
	"context"
	"errors"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/LinuxTerminali/aegisllm/internal/capacity"
	"github.com/LinuxTerminali/aegisllm/internal/estimation"
	"github.com/LinuxTerminali/aegisllm/internal/model"
	"github.com/LinuxTerminali/aegisllm/internal/queue"
)

var (
	// ErrQueueFull indicates the scheduler's bounded queue cannot accept more requests.
	ErrQueueFull = errors.New("scheduler queue is full")
	// ErrWaitTimeout indicates the request exceeded its maximum permissible queue wait time.
	ErrWaitTimeout = errors.New("maximum queue wait time exceeded")
)

// Config defines the scheduler operating limits.
type Config struct {
	MaxConcurrency  int                    `json:"max_concurrency"`
	MaxQueueSize    int                    `json:"max_queue_size"`
	MaxQueueWait    time.Duration          `json:"max_queue_wait"`
	Policy          queue.SchedulingPolicy `json:"policy"`
	PriorityWeights map[int]int            `json:"priority_weights,omitempty"`
}

// Stats holds snapshot statistics of scheduler operations.
type Stats struct {
	QueuedRequests         int                   `json:"queued_requests"`
	ActiveRequests         int                   `json:"active_requests"`
	MaxQueueCapacity       int                   `json:"max_queue_capacity"`
	MaxConcurrency         int                   `json:"max_concurrency"`
	TotalRequests          int64                 `json:"total_requests"`
	TotalAdmitted          int64                 `json:"total_admitted"`
	TotalCompleted         int64                 `json:"total_completed"`
	TotalRejected          int64                 `json:"total_rejected"` // Combined rejections
	TotalRejectedQueueFull int64                 `json:"total_rejected_queue_full"`
	TotalRejectedCapacity  int64                 `json:"total_rejected_capacity"`
	TotalCanceled          int64                 `json:"total_canceled"`
	TotalTimedOut          int64                 `json:"total_timed_out"`
	Capacity               capacity.TrackerStats `json:"capacity"`
	QueueStats             queue.FairQueueStats  `json:"queue_stats"`
}

// Lease represents granted execution capacity for a single upstream request.
type Lease struct {
	scheduler    *Scheduler
	meta         model.RequestMetadata
	waitDuration time.Duration
	reservation  *capacity.Reservation
	released     atomic.Bool
	reconciled   atomic.Bool
}

// WaitDuration returns how long the request waited in the queue before being admitted.
func (l *Lease) WaitDuration() time.Duration {
	return l.waitDuration
}

// EstimatedTokens returns the estimated token count reserved for this lease.
func (l *Lease) EstimatedTokens() int {
	if l.reservation != nil {
		return l.reservation.EstimatedTokens
	}
	return 0
}

// Reconcile reconciles the initial reservation against provider-reported actual token usage.
func (l *Lease) Reconcile(actualTokens int) {
	if l.reconciled.CompareAndSwap(false, true) {
		if l.reservation != nil && l.scheduler.tracker != nil {
			l.scheduler.tracker.Reconcile(l.reservation, actualTokens)
		}
	}
}

// RePace is called before retrying after an upstream 429. The previous reservation did not
// consume provider quota, so it is returned, and a fresh reservation is taken through the
// tracker: the retry waits for quota (and any Retry-After hold) like every other request
// instead of racing the queue with blind backoff.
func (l *Lease) RePace(ctx context.Context) error {
	if l.released.Load() || l.scheduler.tracker == nil {
		return nil
	}
	if l.reservation != nil && l.reconciled.CompareAndSwap(false, true) {
		l.scheduler.tracker.Release(l.reservation)
	}
	est := 1
	if l.reservation != nil {
		est = l.reservation.EstimatedTokens
	}
	paceCtx, cancel := context.WithTimeout(ctx, l.scheduler.cfg.MaxQueueWait)
	defer cancel()
	res, err := l.scheduler.tracker.Reserve(paceCtx, l.meta.RequestID, est)
	if err != nil {
		l.reservation = nil
		return err
	}
	l.reservation = res
	l.reconciled.Store(false)
	return nil
}

// Release yields the execution slot back to the scheduler and releases unused capacity.
// Must be called exactly once when request processing completes.
func (l *Lease) Release() {
	if l.released.CompareAndSwap(false, true) {
		// If actual usage was never reconciled, release the initial reservation
		if l.reconciled.CompareAndSwap(false, true) {
			if l.reservation != nil && l.scheduler.tracker != nil {
				l.scheduler.tracker.Release(l.reservation)
			}
		}
		l.scheduler.releaseSlot()
		l.scheduler.totalCompleted.Add(1)
	}
}

// Scheduler coordinates request admission, bounded priority queuing, capacity pacing, and concurrency.
type Scheduler struct {
	cfg       Config
	pq        *queue.PriorityQueue
	tracker   capacity.Tracker
	estimator estimation.Estimator

	mu     sync.Mutex
	active int

	totalRequests          atomic.Int64
	totalAdmitted          atomic.Int64
	totalCompleted         atomic.Int64
	totalRejectedQueueFull atomic.Int64
	totalRejectedCapacity  atomic.Int64
	totalCanceled          atomic.Int64
	totalTimedOut          atomic.Int64
}

// NewScheduler creates an initialized Scheduler with default tracker and estimator.
func NewScheduler(cfg Config) *Scheduler {
	return NewSchedulerWithDependencies(cfg, nil, nil)
}

// NewSchedulerWithDependencies creates an initialized Scheduler with specific dependencies.
func NewSchedulerWithDependencies(cfg Config, tracker capacity.Tracker, est estimation.Estimator) *Scheduler {
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

	if tracker == nil {
		tracker = capacity.NewDualTokenBucketTracker(capacity.DefaultConfig())
	}
	if est == nil {
		est = estimation.NewHeuristicEstimator(estimation.DefaultConfig())
	}

	return &Scheduler{
		cfg:       cfg,
		pq:        queue.NewPriorityQueueWithWeights(cfg.MaxQueueSize, cfg.Policy, cfg.PriorityWeights),
		tracker:   tracker,
		estimator: est,
	}
}

// Acquire requests an execution slot and reserves token/request capacity.
// If concurrency is available and queue is empty, direct admission occurs.
// Otherwise, the request is placed into the bounded priority or fair queue.
func (s *Scheduler) Acquire(ctx context.Context, meta model.RequestMetadata, body []byte) (*Lease, error) {
	s.totalRequests.Add(1)
	start := time.Now()

	// Estimate token consumption
	estTokens := s.estimator.Estimate(body).TotalTokens

	// Concurrency slot acquisition
	var waitDur time.Duration
	s.mu.Lock()
	if s.active < s.cfg.MaxConcurrency && s.pq.Len() == 0 {
		s.active++
		s.mu.Unlock()
		waitDur = 0
	} else {
		// Must enqueue
		item := queue.NewItem(meta.RequestID, meta, ctx)
		if err := s.pq.Enqueue(item); err != nil {
			s.mu.Unlock()
			s.totalRejectedQueueFull.Add(1)
			return nil, ErrQueueFull
		}
		s.mu.Unlock()

		timer := time.NewTimer(s.cfg.MaxQueueWait)
		defer timer.Stop()

		select {
		case <-item.Ready:
			if err := ctx.Err(); err != nil {
				s.releaseSlot()
				s.totalCanceled.Add(1)
				return nil, err
			}
			waitDur = time.Since(start)

		case <-ctx.Done():
			_, err := s.pq.Remove(item.ID)
			if err == nil {
				s.totalCanceled.Add(1)
				return nil, ctx.Err()
			}
			<-item.Ready
			s.releaseSlot()
			s.totalCanceled.Add(1)
			return nil, ctx.Err()

		case <-timer.C:
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

	// Reserve capacity & pace (RPM and TPM). The wait is bounded by whatever is left of
	// MaxQueueWait so a request never waits longer in total than the configured queue deadline.
	remaining := s.cfg.MaxQueueWait - time.Since(start)
	if remaining <= 0 {
		s.releaseSlot()
		s.totalTimedOut.Add(1)
		return nil, ErrWaitTimeout
	}
	paceCtx, cancelPace := context.WithTimeout(ctx, remaining)
	reservation, err := s.tracker.Reserve(paceCtx, meta.RequestID, estTokens)
	cancelPace()
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) && ctx.Err() == nil {
			// Our pacing deadline fired, not the caller's: report as a queue timeout.
			s.releaseSlot()
			s.totalTimedOut.Add(1)
			return nil, ErrWaitTimeout
		}
		// Release acquired concurrency slot if pacing failed or context canceled
		s.releaseSlot()
		if errors.Is(err, ctx.Err()) {
			s.totalCanceled.Add(1)
		} else {
			s.totalRejectedCapacity.Add(1)
		}
		return nil, err
	}

	s.totalAdmitted.Add(1)
	if paced := time.Since(start); paced > waitDur {
		waitDur = paced
	}
	return &Lease{
		scheduler:    s,
		meta:         meta,
		waitDuration: waitDur,
		reservation:  reservation,
	}, nil
}

// releaseSlot yields an active concurrency slot.
func (s *Scheduler) releaseSlot() {
	s.mu.Lock()
	defer s.mu.Unlock()

	next := s.pq.Dequeue()
	if next == nil {
		s.active--
		return
	}

	close(next.Ready)
}

// ReconcileHeaders updates internal capacity tracker with provider response headers.
func (s *Scheduler) ReconcileHeaders(h http.Header) {
	if s.tracker != nil {
		s.tracker.ReconcileHeaders(h)
	}
}

// MarkUpstreamRateLimited tells the capacity tracker the provider rejected a request with 429,
// so queued requests are held back for retryAfter instead of piling on.
func (s *Scheduler) MarkUpstreamRateLimited(retryAfter time.Duration) {
	if s.tracker != nil {
		s.tracker.MarkExhausted(retryAfter)
	}
}

// Stats returns a snapshot of scheduler metrics.
func (s *Scheduler) Stats() Stats {
	s.mu.Lock()
	active := s.active
	queued := s.pq.Len()
	s.mu.Unlock()

	var capStats capacity.TrackerStats
	if s.tracker != nil {
		capStats = s.tracker.Stats()
	}

	qStats := s.pq.Stats()

	totalRejected := s.totalRejectedQueueFull.Load() + s.totalRejectedCapacity.Load()

	return Stats{
		QueuedRequests:         queued,
		ActiveRequests:         active,
		MaxQueueCapacity:       s.cfg.MaxQueueSize,
		MaxConcurrency:         s.cfg.MaxConcurrency,
		TotalRequests:          s.totalRequests.Load(),
		TotalAdmitted:          s.totalAdmitted.Load(),
		TotalCompleted:         s.totalCompleted.Load(),
		TotalRejected:          totalRejected,
		TotalRejectedQueueFull: s.totalRejectedQueueFull.Load(),
		TotalRejectedCapacity:  s.totalRejectedCapacity.Load(),
		TotalCanceled:          s.totalCanceled.Load(),
		TotalTimedOut:          s.totalTimedOut.Load(),
		Capacity:               capStats,
		QueueStats:             qStats,
	}
}
