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
	MaxPacingDelay    time.Duration `json:"max_pacing_delay"`    // Max duration to pause a request to satisfy pacing
}

// DefaultConfig provides conservative capacity defaults.
func DefaultConfig() Config {
	return Config{
		RequestsPerMinute: 0, // Default unconstrained
		RequestBurst:      0,
		TokensPerMinute:   0, // Default unconstrained
		TokenBurst:        0,
		MaxPacingDelay:    500 * time.Millisecond,
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

	// Telemetry
	totalReservations     atomic.Int64
	totalPacedRequests    atomic.Int64
	totalPacingWaitMs     atomic.Int64
	totalTokensReserved   atomic.Int64
	totalTokensReconciled atomic.Int64
}

// NewDualTokenBucketTracker initializes a new capacity tracker.
func NewDualTokenBucketTracker(cfg Config) *DualTokenBucketTracker {
	if cfg.MaxPacingDelay <= 0 {
		cfg.MaxPacingDelay = 500 * time.Millisecond
	}

	t := &DualTokenBucketTracker{
		cfg:        cfg,
		lastRefill: time.Now(),
	}

	if cfg.RequestsPerMinute > 0 {
		t.reqRefillRPS = cfg.RequestsPerMinute / 60.0
		burst := float64(cfg.RequestBurst)
		if burst <= 0 {
			burst = t.reqRefillRPS // Default burst to 1-second equivalent
			if burst < 1.0 {
				burst = 1.0
			}
		}
		t.maxReqTokens = burst
		t.reqTokens = burst
	}

	if cfg.TokensPerMinute > 0 {
		t.tokRefillTPS = cfg.TokensPerMinute / 60.0
		burst := float64(cfg.TokenBurst)
		if burst <= 0 {
			burst = t.tokRefillTPS
			if burst < 100.0 {
				burst = 100.0
			}
		}
		t.maxTokTokens = burst
		t.tokTokens = burst
	}

	return t
}

// Reserve attempts to acquire request and token capacity, pacing sequentially if within MaxPacingDelay and client deadline.
func (t *DualTokenBucketTracker) Reserve(ctx context.Context, id string, estimatedTokens int) (*Reservation, error) {
	if estimatedTokens < 1 {
		estimatedTokens = 1
	}

	t.mu.Lock()
	t.refill(time.Now())

	var pacingDelay time.Duration

	// Check RPM deficit
	if t.cfg.RequestsPerMinute > 0 {
		if t.reqTokens < 1.0 {
			deficit := 1.0 - t.reqTokens
			waitSec := deficit / t.reqRefillRPS
			delay := time.Duration(waitSec * float64(time.Second))
			if delay > pacingDelay {
				pacingDelay = delay
			}
		}
	}

	// Check TPM deficit
	if t.cfg.TokensPerMinute > 0 {
		needed := float64(estimatedTokens)
		if needed > t.maxTokTokens {
			needed = t.maxTokTokens
		}
		if t.tokTokens < needed {
			deficit := needed - t.tokTokens
			waitSec := deficit / t.tokRefillTPS
			delay := time.Duration(waitSec * float64(time.Second))
			if delay > pacingDelay {
				pacingDelay = delay
			}
		}
	}

	// 1. Check deadline feasibility: request cannot plausibly be admitted before client deadline
	if deadline, ok := ctx.Deadline(); ok {
		if time.Until(deadline) < pacingDelay {
			t.mu.Unlock()
			return nil, ErrInsufficientCapacity
		}
	}

	// 2. Check configured MaxPacingDelay
	if pacingDelay > 0 && t.cfg.MaxPacingDelay > 0 && pacingDelay > t.cfg.MaxPacingDelay {
		t.mu.Unlock()
		return nil, ErrInsufficientCapacity
	}

	// Sequenced reservation: deduct capacity (allowing temporary negative balance to space arrivals)
	if t.cfg.RequestsPerMinute > 0 {
		t.reqTokens -= 1.0
	}
	if t.cfg.TokensPerMinute > 0 {
		needed := float64(estimatedTokens)
		if needed > t.maxTokTokens {
			needed = t.maxTokTokens
		}
		t.tokTokens -= needed
	}

	t.totalReservations.Add(1)
	t.totalTokensReserved.Add(int64(estimatedTokens))
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
		if t.cfg.RequestsPerMinute > 0 {
			t.reqTokens += 1.0
			if t.reqTokens > t.maxReqTokens {
				t.reqTokens = t.maxReqTokens
			}
		}
		if t.cfg.TokensPerMinute > 0 {
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

	if t.cfg.TokensPerMinute > 0 {
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

	if t.cfg.RequestsPerMinute > 0 {
		t.reqTokens += 1.0
		if t.reqTokens > t.maxReqTokens {
			t.reqTokens = t.maxReqTokens
		}
	}
	if t.cfg.TokensPerMinute > 0 {
		t.tokTokens += float64(res.EstimatedTokens)
		if t.tokTokens > t.maxTokTokens {
			t.tokTokens = t.maxTokTokens
		}
	}
}

// ReconcileHeaders parses provider quota headers and cautiously synchronizes local token buckets.
func (t *DualTokenBucketTracker) ReconcileHeaders(h http.Header) {
	quota := upstream.ParseProviderQuota(h)
	if !quota.HasRequestsQuota && !quota.HasTokensQuota {
		return
	}

	t.mu.Lock()
	defer t.mu.Unlock()
	t.refill(time.Now())

	// If provider reports lower remaining requests than local tracker, synchronize downwards
	if quota.HasRequestsQuota && t.cfg.RequestsPerMinute > 0 {
		reported := float64(quota.RemainingRequests)
		if reported < t.reqTokens {
			t.reqTokens = reported
		}
	}

	// If provider reports lower remaining tokens than local tracker, synchronize downwards
	if quota.HasTokensQuota && t.cfg.TokensPerMinute > 0 {
		reported := float64(quota.RemainingTokens)
		if reported < t.tokTokens {
			t.tokTokens = reported
		}
	}
}

func (t *DualTokenBucketTracker) refill(now time.Time) {
	elapsed := now.Sub(t.lastRefill).Seconds()
	if elapsed <= 0 {
		return
	}
	t.lastRefill = now

	if t.cfg.RequestsPerMinute > 0 {
		t.reqTokens += elapsed * t.reqRefillRPS
		if t.reqTokens > t.maxReqTokens {
			t.reqTokens = t.maxReqTokens
		}
	}

	if t.cfg.TokensPerMinute > 0 {
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
	}
}
