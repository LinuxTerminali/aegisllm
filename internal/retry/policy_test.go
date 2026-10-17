package retry

import (
	"math/rand"
	"net/http"
	"testing"
	"time"
)

func TestIsRetryable(t *testing.T) {
	// Retryable status codes
	if !IsRetryable(http.StatusTooManyRequests) {
		t.Error("expected 429 to be retryable")
	}
	if !IsRetryable(http.StatusBadGateway) {
		t.Error("expected 502 to be retryable")
	}
	if !IsRetryable(http.StatusServiceUnavailable) {
		t.Error("expected 503 to be retryable")
	}
	if !IsRetryable(http.StatusGatewayTimeout) {
		t.Error("expected 504 to be retryable")
	}

	// Permanent failures (must NOT be retried)
	nonRetryable := []int{
		http.StatusOK,
		http.StatusBadRequest,
		http.StatusUnauthorized,
		http.StatusForbidden,
		http.StatusNotFound,
		http.StatusUnprocessableEntity,
		http.StatusInternalServerError,
	}

	for _, code := range nonRetryable {
		if IsRetryable(code) {
			t.Errorf("expected %d to NOT be retryable", code)
		}
	}
}

func TestCalculateBackoff_ExponentialAndJitter(t *testing.T) {
	pol := Policy{
		MaxRetries:     3,
		InitialBackoff: 50 * time.Millisecond,
		MaxBackoff:     1 * time.Second,
		BackoffFactor:  2.0,
		JitterFraction: 0.20,
	}

	rng := rand.New(rand.NewSource(12345))

	// Attempt 0: ~50ms +- 20% (40ms to 60ms)
	d0 := pol.CalculateBackoff(0, 0, rng)
	if d0 < 40*time.Millisecond || d0 > 60*time.Millisecond {
		t.Errorf("attempt 0 unexpected duration: %v", d0)
	}

	// Attempt 1: ~100ms +- 20% (80ms to 120ms)
	d1 := pol.CalculateBackoff(1, 0, rng)
	if d1 < 80*time.Millisecond || d1 > 120*time.Millisecond {
		t.Errorf("attempt 1 unexpected duration: %v", d1)
	}

	// Attempt 10: must be bounded by MaxBackoff (1s +- 20%)
	d10 := pol.CalculateBackoff(10, 0, rng)
	if d10 > 1*time.Second {
		t.Errorf("expected bound by MaxBackoff (<=1s), got %v", d10)
	}
}

func TestCalculateBackoff_RespectsRetryAfter(t *testing.T) {
	pol := DefaultPolicy()
	rng := rand.New(rand.NewSource(999))

	retryAfter := 500 * time.Millisecond
	d := pol.CalculateBackoff(0, retryAfter, rng)

	// Should be around 500ms +- 25% (375ms to 625ms)
	if d < 375*time.Millisecond || d > 625*time.Millisecond {
		t.Errorf("expected backoff around Retry-After, got %v", d)
	}
}
