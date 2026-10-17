package capacity

import (
	"context"
	"fmt"
	"net/http"
	"sync"
	"testing"
	"time"
)

func TestTracker_RPMAndPacing(t *testing.T) {
	// 60 RPM = 1 RPS, burst = 2
	cfg := Config{
		RequestsPerMinute: 60,
		RequestBurst:      2,
		MaxPacingDelay:    1500 * time.Millisecond,
	}
	tr := NewDualTokenBucketTracker(cfg)

	// First 2 requests should succeed immediately from burst
	r1, err := tr.Reserve(context.Background(), "req-1", 10)
	if err != nil {
		t.Fatalf("req-1 failed: %v", err)
	}
	r2, err := tr.Reserve(context.Background(), "req-2", 10)
	if err != nil {
		t.Fatalf("req-2 failed: %v", err)
	}

	// 3rd request has to pace (wait ~1 second for refill)
	start := time.Now()
	r3, err := tr.Reserve(context.Background(), "req-3", 10)
	if err != nil {
		t.Fatalf("req-3 failed: %v", err)
	}
	elapsed := time.Since(start)

	if elapsed < 800*time.Millisecond {
		t.Fatalf("expected req-3 to be paced for ~1s, got %v", elapsed)
	}

	tr.Reconcile(r1, 10)
	tr.Reconcile(r2, 10)
	tr.Reconcile(r3, 10)
}

func TestTracker_TPMAndReconciliation(t *testing.T) {
	// 6000 TPM = 100 TPS, burst = 500 tokens
	cfg := Config{
		TokensPerMinute: 6000,
		TokenBurst:      500,
		MaxPacingDelay:  500 * time.Millisecond,
	}
	tr := NewDualTokenBucketTracker(cfg)

	// Reserve 300 tokens
	r1, err := tr.Reserve(context.Background(), "req-1", 300)
	if err != nil {
		t.Fatalf("failed to reserve: %v", err)
	}

	stats := tr.Stats()
	if stats.AvailableTokenTokens > 205 || stats.AvailableTokenTokens < 195 {
		t.Fatalf("expected ~200 tokens remaining, got %f", stats.AvailableTokenTokens)
	}

	// Provider returns actual usage = 100 tokens (we over-estimated by 200)
	tr.Reconcile(r1, 100)

	statsAfter := tr.Stats()
	// Should have credited back the 200 unused tokens -> ~400 available
	if statsAfter.AvailableTokenTokens < 390 {
		t.Fatalf("expected ~400 tokens after reconciliation, got %f", statsAfter.AvailableTokenTokens)
	}
}

func TestTracker_ConcurrentRaceForLastQuota(t *testing.T) {
	// Exactly 3 requests allowed in burst, refill rate 0
	cfg := Config{
		RequestsPerMinute: 0.0001, // Extremely slow refill
		RequestBurst:      3,
		MaxPacingDelay:    10 * time.Millisecond,
	}
	tr := NewDualTokenBucketTracker(cfg)

	var wg sync.WaitGroup
	numCompetitors := 10
	var successCount sync.Mutex
	successful := 0

	for i := 0; i < numCompetitors; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			res, err := tr.Reserve(context.Background(), fmt.Sprintf("req-%d", id), 10)
			if err == nil {
				successCount.Lock()
				successful++
				successCount.Unlock()
				tr.Reconcile(res, 10)
			}
		}(i)
	}

	wg.Wait()

	// Exactly 3 must succeed, 7 must be rejected with ErrInsufficientCapacity
	if successful != 3 {
		t.Fatalf("expected exactly 3 successful reservations, got %d", successful)
	}
}

func TestTracker_ReleaseOnCancellation(t *testing.T) {
	cfg := Config{
		RequestsPerMinute: 60,
		RequestBurst:      1,
		TokensPerMinute:   6000,
		TokenBurst:        100,
		MaxPacingDelay:    100 * time.Millisecond,
	}
	tr := NewDualTokenBucketTracker(cfg)

	res, err := tr.Reserve(context.Background(), "req-1", 50)
	if err != nil {
		t.Fatalf("reservation failed: %v", err)
	}

	// Release without completing
	tr.Release(res)

	// We should be able to immediately reserve again
	res2, err := tr.Reserve(context.Background(), "req-2", 50)
	if err != nil {
		t.Fatalf("subsequent reservation failed after release: %v", err)
	}
	tr.Reconcile(res2, 50)
}

func TestTracker_ProviderHeaderReconciliation(t *testing.T) {
	cfg := Config{
		RequestsPerMinute: 600,
		RequestBurst:      10,
		TokensPerMinute:   60000,
		TokenBurst:        1000,
	}
	tr := NewDualTokenBucketTracker(cfg)

	// Simulate provider returning header reporting lower remaining quota
	hdr := http.Header{}
	hdr.Set("x-ratelimit-remaining-requests", "2")
	hdr.Set("x-ratelimit-remaining-tokens", "150")

	tr.ReconcileHeaders(hdr)

	stats := tr.Stats()
	if stats.AvailableRequestTokens > 2.1 {
		t.Errorf("expected request tokens synchronized down to ~2, got %f", stats.AvailableRequestTokens)
	}
	if stats.AvailableTokenTokens > 151.0 {
		t.Errorf("expected token tokens synchronized down to ~150, got %f", stats.AvailableTokenTokens)
	}
}

func TestTracker_DeadlineFeasibilityCheck(t *testing.T) {
	cfg := Config{
		RequestsPerMinute: 60, // 1 RPS -> 1s refill per token
		RequestBurst:      1,
		MaxPacingDelay:    5 * time.Second,
	}
	tr := NewDualTokenBucketTracker(cfg)

	// Consume the single burst token
	r1, err := tr.Reserve(context.Background(), "req-1", 10)
	if err != nil {
		t.Fatalf("first reservation failed: %v", err)
	}
	defer tr.Reconcile(r1, 10)

	// Second request has a deadline of only 50ms, but needs 1000ms refill
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	start := time.Now()
	_, err = tr.Reserve(ctx, "req-2", 10)
	elapsed := time.Since(start)

	// Must fail predictably with ErrInsufficientCapacity immediately without waiting for timeout
	if err != ErrInsufficientCapacity {
		t.Fatalf("expected ErrInsufficientCapacity, got %v", err)
	}
	if elapsed > 30*time.Millisecond {
		t.Fatalf("expected immediate predictable rejection, took %v", elapsed)
	}
}
