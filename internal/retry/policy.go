package retry

import (
	"math"
	"math/rand"
	"net/http"
	"time"
)

// Policy defines parameters for bounded exponential backoff with jitter.
type Policy struct {
	MaxRetries     int           `json:"max_retries"`
	InitialBackoff time.Duration `json:"initial_backoff"`
	MaxBackoff     time.Duration `json:"max_backoff"`
	BackoffFactor  float64       `json:"backoff_factor"`
	JitterFraction float64       `json:"jitter_fraction"` // e.g. 0.25 represents +-25%
}

// DefaultPolicy returns safe, production-grade retry defaults.
func DefaultPolicy() Policy {
	return Policy{
		MaxRetries:     3,
		InitialBackoff: 50 * time.Millisecond,
		MaxBackoff:     2 * time.Second,
		BackoffFactor:  2.0,
		JitterFraction: 0.25,
	}
}

// IsRetryable determines whether an upstream HTTP status code represents a transient, retryable failure.
// Permanent client errors (400, 401, 403, 404, 422) are never retried.
func IsRetryable(statusCode int) bool {
	switch statusCode {
	case http.StatusTooManyRequests: // 429
		return true
	case http.StatusBadGateway, // 502
		http.StatusServiceUnavailable, // 503
		http.StatusGatewayTimeout:     // 504
		return true
	default:
		return false
	}
}

// CalculateBackoff computes the sleep duration for a retry attempt.
// Respects provider Retry-After information when present, applying jitter to prevent synchronized waves.
func (p Policy) CalculateBackoff(attempt int, retryAfter time.Duration, rng *rand.Rand) time.Duration {
	if rng == nil {
		rng = rand.New(rand.NewSource(time.Now().UnixNano()))
	}

	if retryAfter > 0 {
		// Respect provider Retry-After with jitter
		jitterRange := float64(retryAfter) * p.JitterFraction
		jitter := (rng.Float64()*2 - 1) * jitterRange
		d := retryAfter + time.Duration(jitter)
		if d < 10*time.Millisecond {
			d = retryAfter
		}
		if d > p.MaxBackoff {
			d = p.MaxBackoff
		}
		return d
	}

	// Bounded exponential backoff
	factor := math.Pow(p.BackoffFactor, float64(attempt))
	base := float64(p.InitialBackoff) * factor
	if base > float64(p.MaxBackoff) {
		base = float64(p.MaxBackoff)
	}

	jitterRange := base * p.JitterFraction
	jitter := (rng.Float64()*2 - 1) * jitterRange
	finalDuration := time.Duration(base + jitter)

	if finalDuration < 5*time.Millisecond {
		finalDuration = p.InitialBackoff
	}
	if finalDuration > p.MaxBackoff {
		finalDuration = p.MaxBackoff
	}

	return finalDuration
}
