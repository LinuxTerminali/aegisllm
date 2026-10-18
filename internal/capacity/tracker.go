package capacity

import (
	"context"
	"errors"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/LinuxTerminali/aegisllm/internal/upstream"
)

var (
	// ErrInsufficientCapacity is returned when the required RPM/TPM quota cannot be satisfied within the pacing deadline.
	ErrInsufficientCapacity = errors.New("insufficient capacity quota")
)

// Config holds capacity limits and pacing thresholds.
type Config struct {
	RequestsPerMinute float64       `json:"requests_per_minute"` // 0 = unlimited
	RequestBurst      int           `json:"request_burst"`       // Max request bucket capacity
	TokensPerMinute   float64       `json:"tokens_per_minute"`   // 0 = unlimited
	TokenBurst        int           `json:"token_burst"`         // Max token bucket capacity
	MaxPacingDelay    time.Duration `json:"max_pacing_delay"`    // Max pacing wait before rejecting; 0 = wait until the caller's deadline
	AutoLearn         bool          `json:"auto_learn"`          // Learn RPM/TPM limits from provider rate-limit headers when not configured
}

// DefaultConfig provides conservative capacity defaults.
func DefaultConfig() Config {
	return Config{
		RequestsPerMinute: 0, // Default unconstrained
		RequestBurst:      0,
		TokensPerMinute:   0, // Default unconstrained
		TokenBurst:        0,
		MaxPacingDelay:    0, // Wait until the caller's deadline instead of shedding load
		AutoLearn:         true,
	}
}

// Reservation tracks reserved capacity for an in-flight request.
type Reservation struct {
	ID              string
	EstimatedTokens int
	ReservedAt      time.Time
	released        atomic.Bool
	tracker         *DualTokenBucketTracker
}

// Tracker defines the capacity accounting and pacing interface.
type Tracker interface {
	Reserve(ctx context.Context, id string, estimatedTokens int) (*Reservation, error)
	Reconcile(res *Reservation, actualTokens int)
	Release(res *Reservation)
	ReconcileHeaders(h http.Header)
	// TryReserve reserves capacity only if no pacing wait is needed right now.
	// It never blocks and never deducts when it returns false.
	TryReserve(id string, estimatedTokens int) (*Reservation, bool)
	// MarkExhausted records an upstream rate-limit rejection so subsequent reservations
	// wait at least retryAfter before being admitted.
	MarkExhausted(retryAfter time.Duration)
	Stats() TrackerStats
}

// TrackerStats reports real-time capacity and pacing counters.
type TrackerStats struct {
	AvailableRequestTokens float64 `json:"available_request_tokens"`
	MaxRequestCapacity     float64 `json:"max_request_capacity"`
	AvailableTokenTokens   float64 `json:"available_token_tokens"`
	MaxTokenCapacity       float64 `json:"max_token_capacity"`
	TotalReservations      int64   `json:"total_reservations"`
	TotalPacedRequests     int64   `json:"total_paced_requests"`
	TotalPacingWaitMs      int64   `json:"total_pacing_wait_ms"`
	TotalTokensReserved    int64   `json:"total_tokens_reserved"`
	TotalTokensReconciled  int64   `json:"total_tokens_reconciled"`
	RequestsPerMinute      float64 `json:"requests_per_minute"`
	TokensPerMinute        float64 `json:"tokens_per_minute"`
	LimitsLearned          bool    `json:"limits_learned"`
	ExhaustedUntilMs       int64   `json:"exhausted_until_ms"`
}

// DualTokenBucketTracker tracks RPM and TPM independently with burst capacity and header reconciliation.
type DualTokenBucketTracker struct {
	cfg Config
	mu  sync.Mutex

	// Request bucket
	reqTokens    float64
	maxReqTokens float64
	reqRefillRPS float64

	// Token bucket
	tokTokens    float64
	maxTokTokens float64
	tokRefillTPS float64

	lastRefill time.Time

	reqEnabled bool
	tokEnabled bool
	learned    bool

	// exhaustedUntil is set when the upstream rejects with 429; no reservation is admitted before it.
	exhaustedUntil time.Time

	// Telemetry
	totalReservations     atomic.Int64
	totalPacedRequests    atomic.Int64
	totalPacingWaitMs     atomic.Int64
	totalTokensReserved   atomic.Int64
	totalTokensReconciled atomic.Int64
}

// NewDualTokenBucketTracker initializes a new capacity tracker.
func NewDualTokenBucketTracker(cfg Config) *DualTokenBucketTracker {
	if cfg.MaxPacingDelay < 0 {
		cfg.MaxPacingDelay = 0
	}

	t := &DualTokenBucketTracker{
		cfg:        cfg,
		lastRefill: time.Now(),
	}

	if cfg.RequestsPerMinute > 0 {
		t.setRequestLimit(cfg.RequestsPerMinute, float64(cfg.RequestBurst), true)
	}
	if cfg.TokensPerMinute > 0 {
		t.setTokenLimit(cfg.TokensPerMinute, float64(cfg.TokenBurst), true)
	}

	return t
}

// setRequestLimit configures (or re-configures) the request bucket. Caller must hold t.mu
// unless called from the constructor. When fill is true the bucket starts full.
func (t *DualTokenBucketTracker) setRequestLimit(rpm, burst float64, fill bool) {
	t.reqRefillRPS = rpm / 60.0
	if burst <= 0 {
		// Providers enforce per-minute windows: "30 RPM" means 30 may go at once at the
		// top of the window. Default the bucket to the whole window, not one second of it.
		burst = rpm
		if burst < 1.0 {
			burst = 1.0
		}
	}
	t.maxReqTokens = burst
	if fill || t.reqTokens > burst {
		t.reqTokens = burst
	}
	t.reqEnabled = true
}

// setTokenLimit configures (or re-configures) the token bucket. Caller must hold t.mu
// unless called from the constructor. When fill is true the bucket starts full.
func (t *DualTokenBucketTracker) setTokenLimit(tpm, burst float64, fill bool) {
	t.tokRefillTPS = tpm / 60.0
	if burst <= 0 {
		burst = tpm
		if burst < 100.0 {
			burst = 100.0
		}
	}
	t.maxTokTokens = burst
	if fill || t.tokTokens > burst {
		t.tokTokens = burst
	}
	t.tokEnabled = true
}

// Reserve attempts to acquire request and token capacity, pacing sequentially if within MaxPacingDelay and client deadline.
// pacingDelayLocked returns how long a reservation of estimatedTokens would have to wait.
// Caller holds t.mu and has called refill(now).
func (t *DualTokenBucketTracker) pacingDelayLocked(now time.Time, estimatedTokens int) time.Duration {
	var pacingDelay time.Duration

	// Upstream told us it is exhausted: nothing is admitted before that moment.
	if t.exhaustedUntil.After(now) {
		pacingDelay = t.exhaustedUntil.Sub(now)
	}

	if t.reqEnabled && t.reqTokens < 1.0 {
		deficit := 1.0 - t.reqTokens
		if delay := time.Duration(deficit / t.reqRefillRPS * float64(time.Second)); delay > pacingDelay {
			pacingDelay = delay
		}
	}

	if t.tokEnabled {
		needed := float64(estimatedTokens)
		if needed > t.maxTokTokens {
			needed = t.maxTokTokens
		}
		if t.tokTokens < needed {
			deficit := needed - t.tokTokens
			if delay := time.Duration(deficit / t.tokRefillTPS * float64(time.Second)); delay > pacingDelay {
				pacingDelay = delay
			}
		}
	}
	return pacingDelay
}

// deductLocked takes the reservation out of the buckets (balance may go negative to
// space arrivals). Caller holds t.mu.
func (t *DualTokenBucketTracker) deductLocked(estimatedTokens int) {
	if t.reqEnabled {
		t.reqTokens -= 1.0
	}
	if t.tokEnabled {
		needed := float64(estimatedTokens)
		if needed > t.maxTokTokens {
			needed = t.maxTokTokens
		}
		t.tokTokens -= needed
	}
	t.totalReservations.Add(1)
	t.totalTokensReserved.Add(int64(estimatedTokens))
}

// TryReserve admits immediately when no pacing is needed; otherwise returns false and
// leaves the buckets untouched so the caller can queue the request by priority.
func (t *DualTokenBucketTracker) TryReserve(id string, estimatedTokens int) (*Reservation, bool) {
	if estimatedTokens < 1 {
		estimatedTokens = 1
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	now := time.Now()
	t.refill(now)
	if t.pacingDelayLocked(now, estimatedTokens) > 0 {
		return nil, false
	}
	t.deductLocked(estimatedTokens)
	return &Reservation{ID: id, EstimatedTokens: estimatedTokens, ReservedAt: now, tracker: t}, true
}

func (t *DualTokenBucketTracker) Reserve(ctx context.Context, id string, estimatedTokens int) (*Reservation, error) {
	if estimatedTokens < 1 {
		estimatedTokens = 1
	}

	t.mu.Lock()
	now := time.Now()
	t.refill(now)
	pacingDelay := t.pacingDelayLocked(now, estimatedTokens)

	// Check deadline feasibility: request cannot plausibly be admitted before client deadline
	if deadline, ok := ctx.Deadline(); ok {
		if time.Until(deadline) < pacingDelay {
			t.mu.Unlock()
			return nil, ErrInsufficientCapacity
		}
	}

	// Check configured MaxPacingDelay (0 = no cap; the caller's deadline bounds the wait)
	if pacingDelay > 0 && t.cfg.MaxPacingDelay > 0 && pacingDelay > t.cfg.MaxPacingDelay {
		t.mu.Unlock()
		return nil, ErrInsufficientCapacity
	}

	t.deductLocked(estimatedTokens)
	if pacingDelay > 0 {
		t.totalPacedRequests.Add(1)
		t.totalPacingWaitMs.Add(pacingDelay.Milliseconds())
	}
	t.mu.Unlock()

	// If no pacing needed, return reservation immediately
	if pacingDelay <= 0 {
		return &Reservation{
			ID:              id,
			EstimatedTokens: estimatedTokens,
			ReservedAt:      time.Now(),
			tracker:         t,
		}, nil
	}

	// Sleep for calculated pacing delay
	timer := time.NewTimer(pacingDelay)
	defer timer.Stop()

	select {
	case <-timer.C:
		return &Reservation{
			ID:              id,
			EstimatedTokens: estimatedTokens,
			ReservedAt:      time.Now(),
			tracker:         t,
		}, nil
	case <-ctx.Done():
		// Release reserved quota back since request was canceled while pacing
		t.mu.Lock()
		if t.reqEnabled {
			t.reqTokens += 1.0
			if t.reqTokens > t.maxReqTokens {
				t.reqTokens = t.maxReqTokens
			}
		}
		if t.tokEnabled {
			needed := float64(estimatedTokens)
			if needed > t.maxTokTokens {
				needed = t.maxTokTokens
			}
			t.tokTokens += needed
			if t.tokTokens > t.maxTokTokens {
				t.tokTokens = t.maxTokTokens
			}
		}
		t.mu.Unlock()
		return nil, ctx.Err()
	}
}

// Reconcile adjusts the token bucket using provider-reported actual token usage.
func (t *DualTokenBucketTracker) Reconcile(res *Reservation, actualTokens int) {
	if res == nil || res.released.Swap(true) {
		return
	}

	t.mu.Lock()
	defer t.mu.Unlock()
	t.refill(time.Now())

	if t.tokEnabled {
		diff := float64(res.EstimatedTokens - actualTokens)
		t.tokTokens += diff
		if t.tokTokens > t.maxTokTokens {
			t.tokTokens = t.maxTokTokens
		}
		if t.tokTokens < 0 {
			t.tokTokens = 0
		}
	}

	t.totalTokensReconciled.Add(int64(actualTokens))
}

// Release yields all reserved capacity back if a request is canceled before upstream execution.
func (t *DualTokenBucketTracker) Release(res *Reservation) {
	if res == nil || res.released.Swap(true) {
		return
	}

	t.mu.Lock()
	defer t.mu.Unlock()
	t.refill(time.Now())

	if t.reqEnabled {
		t.reqTokens += 1.0
		if t.reqTokens > t.maxReqTokens {
			t.reqTokens = t.maxReqTokens
		}
	}
	if t.tokEnabled {
		t.tokTokens += float64(res.EstimatedTokens)
		if t.tokTokens > t.maxTokTokens {
			t.tokTokens = t.maxTokTokens
		}
	}
}

// ReconcileHeaders parses provider quota headers, learns limits when AutoLearn is on and no
// explicit limit was configured, and cautiously synchronizes local token buckets downwards.
func (t *DualTokenBucketTracker) ReconcileHeaders(h http.Header) {
	quota := upstream.ParseProviderQuota(h)
	if !quota.HasRequestsQuota && !quota.HasTokensQuota && quota.LimitRequests == 0 && quota.LimitTokens == 0 {
		return
	}

	t.mu.Lock()
	defer t.mu.Unlock()
	t.refill(time.Now())

	// Learn limits from the provider when the operator did not configure them.
	// Refill rate comes from the *-limit-* headers. Burst (bucket size) is learned as the
	// highest *-remaining-* value ever observed: a provider that reports remaining=4 right
	// after a request has a bucket of ~5; OpenAI reports limit-1, i.e. a full-minute bucket.
	// Starting small and growing is deliberate: conservative first, self-correcting on idle.
	if t.cfg.AutoLearn {
		if t.cfg.RequestsPerMinute <= 0 && quota.LimitRequests > 0 {
			rpm := float64(quota.LimitRequests)
			burst := t.maxReqTokens
			if quota.HasRequestsQuota && float64(quota.RemainingRequests)+1 > burst {
				burst = float64(quota.RemainingRequests) + 1
			}
			if burst > rpm {
				burst = rpm
			}
			if burst < 1 {
				burst = 1
			}
			if !t.reqEnabled || rpm != t.reqRefillRPS*60.0 || burst != t.maxReqTokens {
				t.setRequestLimit(rpm, burst, !t.reqEnabled)
				t.learned = true
			}
		}
		if t.cfg.TokensPerMinute <= 0 && quota.LimitTokens > 0 {
			tpm := float64(quota.LimitTokens)
			burst := t.maxTokTokens
			if quota.HasTokensQuota && float64(quota.RemainingTokens) > burst {
				burst = float64(quota.RemainingTokens)
			}
			if burst > tpm {
				burst = tpm
			}
			if burst < 100 {
				burst = 100
			}
			if !t.tokEnabled || tpm != t.tokRefillTPS*60.0 || burst != t.maxTokTokens {
				t.setTokenLimit(tpm, burst, !t.tokEnabled)
				t.learned = true
			}
		}
	}

	// If provider reports lower remaining requests than local tracker, synchronize downwards
	if quota.HasRequestsQuota && t.reqEnabled {
		reported := float64(quota.RemainingRequests)
		if reported < t.reqTokens {
			t.reqTokens = reported
		}
	}

	// If provider reports lower remaining tokens than local tracker, synchronize downwards
	if quota.HasTokensQuota && t.tokEnabled {
		reported := float64(quota.RemainingTokens)
		if reported < t.tokTokens {
			t.tokTokens = reported
		}
	}
}

// MarkExhausted records an upstream 429. The buckets are drained and, when the provider
// supplied a Retry-After, no reservation is admitted before that time has passed.
func (t *DualTokenBucketTracker) MarkExhausted(retryAfter time.Duration) {
	t.mu.Lock()
	defer t.mu.Unlock()
	now := time.Now()
	t.refill(now)

	if t.reqEnabled && t.reqTokens > 0 {
		t.reqTokens = 0
	}
	if t.tokEnabled && t.tokTokens > 0 {
		t.tokTokens = 0
	}
	if retryAfter > 0 {
		until := now.Add(retryAfter)
		if until.After(t.exhaustedUntil) {
			t.exhaustedUntil = until
			// The provider said "nothing until then": do not accrue refill during the hold,
			// otherwise every request that reserved meanwhile fires at once when it ends.
			t.lastRefill = until
		}
	}
}

func (t *DualTokenBucketTracker) refill(now time.Time) {
	elapsed := now.Sub(t.lastRefill).Seconds()
	if elapsed <= 0 {
		return
	}
	t.lastRefill = now

	if t.reqEnabled {
		t.reqTokens += elapsed * t.reqRefillRPS
		if t.reqTokens > t.maxReqTokens {
			t.reqTokens = t.maxReqTokens
		}
	}

	if t.tokEnabled {
		t.tokTokens += elapsed * t.tokRefillTPS
		if t.tokTokens > t.maxTokTokens {
			t.tokTokens = t.maxTokTokens
		}
	}
}

// Stats returns a copy of current capacity tracking metrics.
func (t *DualTokenBucketTracker) Stats() TrackerStats {
	t.mu.Lock()
	defer t.mu.Unlock()

	return TrackerStats{
		AvailableRequestTokens: t.reqTokens,
		MaxRequestCapacity:     t.maxReqTokens,
		AvailableTokenTokens:   t.tokTokens,
		MaxTokenCapacity:       t.maxTokTokens,
		TotalReservations:      t.totalReservations.Load(),
		TotalPacedRequests:     t.totalPacedRequests.Load(),
		TotalPacingWaitMs:      t.totalPacingWaitMs.Load(),
		TotalTokensReserved:    t.totalTokensReserved.Load(),
		TotalTokensReconciled:  t.totalTokensReconciled.Load(),
		RequestsPerMinute:      t.reqRefillRPS * 60.0,
		TokensPerMinute:        t.tokRefillTPS * 60.0,
		LimitsLearned:          t.learned,
		ExhaustedUntilMs:       exhaustedMs(t.exhaustedUntil),
	}
}

func exhaustedMs(until time.Time) int64 {
	if until.IsZero() {
		return 0
	}
	ms := time.Until(until).Milliseconds()
	if ms < 0 {
		return 0
	}
	return ms
}
