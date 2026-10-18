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

	// dispatch wakes the dispatcher when a slot frees or an item is enqueued.
	dispatch chan struct{}
	stop     chan struct{}
	stopped  sync.Once
	wg       sync.WaitGroup

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

	s := &Scheduler{
		cfg:       cfg,
		pq:        queue.NewPriorityQueueWithWeights(cfg.MaxQueueSize, cfg.Policy, cfg.PriorityWeights),
		tracker:   tracker,
		estimator: est,
		dispatch:  make(chan struct{}, 1),
		stop:      make(chan struct{}),
	}
	s.wg.Add(1)
	go s.runDispatcher()
	return s
}

// Stop halts the dispatcher. Items still queued are failed with ErrWaitTimeout.
func (s *Scheduler) Stop() {
	s.stopped.Do(func() {
		close(s.stop)
		s.wg.Wait()
		s.mu.Lock()
		for {
			item := s.pq.Dequeue()
			if item == nil {
				break
			}
			item.Err = ErrWaitTimeout
			close(item.Ready)
		}
		s.mu.Unlock()
	})
}

func (s *Scheduler) kick() {
	select {
	case s.dispatch <- struct{}{}:
	default:
	}
}

// runDispatcher is the single admission gate. It pops the highest-priority queued item,
// waits for its quota (head of line only), then marks it ready. Because only the head is
// paced, a production request that arrives mid-burst waits at most one pacing interval,
// not behind every batch request that got in line before it.
func (s *Scheduler) runDispatcher() {
	defer s.wg.Done()
	for {
		select {
		case <-s.stop:
			return
		case <-s.dispatch:
		}
		for {
			s.mu.Lock()
			if s.active >= s.cfg.MaxConcurrency || s.pq.Len() == 0 {
				s.mu.Unlock()
				break
			}
			item := s.pq.Dequeue()
			s.active++
			s.mu.Unlock()

			deadline := item.EnqueuedAt.Add(s.cfg.MaxQueueWait)
			pctx, cancel := context.WithDeadline(item.Ctx, deadline)
			res, err := s.tracker.Reserve(pctx, item.ID, item.Tokens)
			cancel()
			if err != nil {
				s.mu.Lock()
				s.active--
				s.mu.Unlock()
				if errors.Is(err, context.DeadlineExceeded) && item.Ctx.Err() == nil {
					err = ErrWaitTimeout
				}
				item.Err = err
				close(item.Ready)
				continue
			}
			item.Payload = res
			close(item.Ready)

			select {
			case <-s.stop:
				return
			default:
			}
		}
	}
}

// Acquire requests an execution slot and reserves token/request capacity.
// If concurrency is available and queue is empty, direct admission occurs.
// Otherwise, the request is placed into the bounded priority or fair queue.
func (s *Scheduler) Acquire(ctx context.Context, meta model.RequestMetadata, body []byte) (*Lease, error) {
	s.totalRequests.Add(1)
	start := time.Now()

	estTokens := s.estimator.Estimate(body).TotalTokens

	// Fast path: a slot is free, nobody is waiting, and quota is available right now.
	s.mu.Lock()
	if s.active < s.cfg.MaxConcurrency && s.pq.Len() == 0 {
		if res, ok := s.tracker.TryReserve(meta.RequestID, estTokens); ok {
			s.active++
			s.mu.Unlock()
			s.totalAdmitted.Add(1)
			return &Lease{scheduler: s, meta: meta, reservation: res}, nil
		}
	}

	// Otherwise queue by priority; the dispatcher admits in order and paces only the head.
	item := queue.NewItem(meta.RequestID, meta, ctx)
	item.Tokens = estTokens
	if err := s.pq.Enqueue(item); err != nil {
		s.mu.Unlock()
		s.totalRejectedQueueFull.Add(1)
		return nil, ErrQueueFull
	}
	s.mu.Unlock()
	s.kick()

	timer := time.NewTimer(s.cfg.MaxQueueWait)
	defer timer.Stop()

	select {
	case <-item.Ready:
	case <-ctx.Done():
		if _, err := s.pq.Remove(item.ID); err == nil {
			s.totalCanceled.Add(1)
			return nil, ctx.Err()
		}
		<-item.Ready // already taken by the dispatcher; its Reserve honours ctx
	case <-timer.C:
		if _, err := s.pq.Remove(item.ID); err == nil {
			s.totalTimedOut.Add(1)
			return nil, ErrWaitTimeout
		}
		<-item.Ready // dispatcher paces with the same deadline; it finishes momentarily
	}

	if item.Err != nil {
		switch {
		case errors.Is(item.Err, ctx.Err()) && ctx.Err() != nil:
			s.totalCanceled.Add(1)
		case errors.Is(item.Err, ErrWaitTimeout):
			s.totalTimedOut.Add(1)
		case errors.Is(item.Err, capacity.ErrInsufficientCapacity):
			s.totalRejectedCapacity.Add(1)
		default:
			s.totalRejectedCapacity.Add(1)
		}
		return nil, item.Err
	}
	if ctx.Err() != nil {
		// Admitted a moment after the caller gave up: hand everything back.
		res, _ := item.Payload.(*capacity.Reservation)
		if res != nil {
			s.tracker.Release(res)
		}
		s.releaseSlot()
		s.totalCanceled.Add(1)
		return nil, ctx.Err()
	}

	s.totalAdmitted.Add(1)
	res, _ := item.Payload.(*capacity.Reservation)
	return &Lease{
		scheduler:    s,
		meta:         meta,
		waitDuration: time.Since(start),
		reservation:  res,
	}, nil
}

// releaseSlot yields an active concurrency slot.
func (s *Scheduler) releaseSlot() {
	s.mu.Lock()
	s.active--
	s.mu.Unlock()
	s.kick()
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
