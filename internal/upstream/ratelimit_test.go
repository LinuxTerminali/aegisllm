package upstream

import (
	"net/http"
	"testing"
	"time"
)

func TestParseRetryAfter(t *testing.T) {
	// 1. Integer seconds
	d, ok := ParseRetryAfter("5")
	if !ok || d != 5*time.Second {
		t.Fatalf("expected 5s, got %v (ok=%v)", d, ok)
	}

	// 2. Float seconds
	d, ok = ParseRetryAfter("1.5")
	if !ok || d != 1500*time.Millisecond {
		t.Fatalf("expected 1.5s, got %v (ok=%v)", d, ok)
	}

	// 3. HTTP date in future
	futureTime := time.Now().Add(10 * time.Second).UTC().Format(time.RFC1123)
	d, ok = ParseRetryAfter(futureTime)
	if !ok || d < 8*time.Second || d > 12*time.Second {
		t.Fatalf("expected ~10s from RFC1123 date, got %v (ok=%v)", d, ok)
	}

	// 4. Invalid or empty
	_, ok = ParseRetryAfter("")
	if ok {
		t.Fatalf("expected false for empty header")
	}

	_, ok = ParseRetryAfter("invalid-date-string")
	if ok {
		t.Fatalf("expected false for invalid header")
	}
}

func TestParseProviderQuota_OpenAI(t *testing.T) {
	h := http.Header{}
	h.Set("x-ratelimit-remaining-requests", "42")
	h.Set("x-ratelimit-limit-requests", "500")
	h.Set("x-ratelimit-reset-requests", "1.2s")
	h.Set("x-ratelimit-remaining-tokens", "9800")
	h.Set("x-ratelimit-limit-tokens", "10000")
	h.Set("x-ratelimit-reset-tokens", "500ms")

	q := ParseProviderQuota(h)
	if !q.HasRequestsQuota || q.RemainingRequests != 42 || q.LimitRequests != 500 {
		t.Errorf("unexpected requests quota: %+v", q)
	}
	if !q.HasTokensQuota || q.RemainingTokens != 9800 || q.LimitTokens != 10000 {
		t.Errorf("unexpected tokens quota: %+v", q)
	}
	if q.ResetRequests != 1200*time.Millisecond || q.ResetTokens != 500*time.Millisecond {
		t.Errorf("unexpected reset times: %+v", q)
	}
}

func TestParseProviderQuota_Anthropic(t *testing.T) {
	h := http.Header{}
	h.Set("anthropic-ratelimit-requests-remaining", "15")
	h.Set("anthropic-ratelimit-requests-limit", "100")
	h.Set("anthropic-ratelimit-tokens-remaining", "45000")
	h.Set("anthropic-ratelimit-tokens-limit", "50000")

	q := ParseProviderQuota(h)
	if !q.HasRequestsQuota || q.RemainingRequests != 15 || q.LimitRequests != 100 {
		t.Errorf("unexpected Anthropic requests quota: %+v", q)
	}
	if !q.HasTokensQuota || q.RemainingTokens != 45000 || q.LimitTokens != 50000 {
		t.Errorf("unexpected Anthropic tokens quota: %+v", q)
	}
}

func TestParseProviderQuota_IETF(t *testing.T) {
	h := http.Header{}
	h.Set("ratelimit-remaining", "25")
	h.Set("ratelimit-limit", "100")
	h.Set("ratelimit-reset", "2")

	q := ParseProviderQuota(h)
	if !q.HasRequestsQuota || q.RemainingRequests != 25 || q.LimitRequests != 100 {
		t.Errorf("unexpected IETF quota: %+v", q)
	}
	if q.ResetRequests != 2*time.Second {
		t.Errorf("unexpected reset time: %v", q.ResetRequests)
	}
}
