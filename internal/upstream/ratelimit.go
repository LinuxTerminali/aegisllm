package upstream

import (
	"net/http"
	"strconv"
	"strings"
	"time"
)

// ProviderQuota holds parsed rate-limit state reported by an upstream LLM provider.
type ProviderQuota struct {
	HasRequestsQuota  bool
	RemainingRequests int
	LimitRequests     int
	ResetRequests     time.Duration

	HasTokensQuota  bool
	RemainingTokens int
	LimitTokens     int
	ResetTokens     time.Duration
}

// ParseRetryAfter parses a Retry-After header value into a time.Duration.
// Supports both integer seconds (e.g. "5") and HTTP-date strings (e.g. "Wed, 21 Oct 2026 07:28:00 GMT").
func ParseRetryAfter(val string) (time.Duration, bool) {
	trimmed := strings.TrimSpace(val)
	if trimmed == "" {
		return 0, false
	}

	// Try parsing integer seconds
	if sec, err := strconv.Atoi(trimmed); err == nil && sec >= 0 {
		return time.Duration(sec) * time.Second, true
	}

	// Try parsing float seconds (e.g. "1.5")
	if f, err := strconv.ParseFloat(trimmed, 64); err == nil && f >= 0 {
		return time.Duration(f * float64(time.Second)), true
	}

	// Try parsing HTTP Date formats (RFC1123, RFC850, ANSIC)
	formats := []string{
		time.RFC1123,
		time.RFC1123Z,
		time.RFC850,
		time.ANSIC,
	}

	for _, layout := range formats {
		if t, err := time.Parse(layout, trimmed); err == nil {
			wait := time.Until(t)
			if wait < 0 {
				wait = 0
			}
			return wait, true
		}
	}

	return 0, false
}

// ParseProviderQuota extracts rate-limit quotas from HTTP response headers.
// Handles OpenAI, Anthropic, and standard IETF rate-limit header conventions.
//
// NOTE: Quota headers are treated as hints, not as the source of truth.
// In real deployments, API keys or organizational quotas are often shared across multiple proxy replicas,
// worker pools, or interactive users. Header values may also be stale, contradictory, or malformed.
func ParseProviderQuota(h http.Header) ProviderQuota {
	q := ProviderQuota{}

	// OpenAI Style: x-ratelimit-remaining-requests, x-ratelimit-remaining-tokens
	if remReq := getHeaderCaseInsensitive(h, "x-ratelimit-remaining-requests"); remReq != "" {
		if v, err := strconv.Atoi(remReq); err == nil && v >= 0 && v <= 10_000_000 {
			q.RemainingRequests = v
			q.HasRequestsQuota = true
		}
	}
	if limReq := getHeaderCaseInsensitive(h, "x-ratelimit-limit-requests"); limReq != "" {
		if v, err := strconv.Atoi(limReq); err == nil && v >= 0 && v <= 10_000_000 {
			q.LimitRequests = v
		}
	}
	if resetReq := getHeaderCaseInsensitive(h, "x-ratelimit-reset-requests"); resetReq != "" {
		q.ResetRequests = parseDurationOrSeconds(resetReq)
	}

	if remTok := getHeaderCaseInsensitive(h, "x-ratelimit-remaining-tokens"); remTok != "" {
		if v, err := strconv.Atoi(remTok); err == nil && v >= 0 && v <= 1_000_000_000 {
			q.RemainingTokens = v
			q.HasTokensQuota = true
		}
	}
	if limTok := getHeaderCaseInsensitive(h, "x-ratelimit-limit-tokens"); limTok != "" {
		if v, err := strconv.Atoi(limTok); err == nil && v >= 0 && v <= 1_000_000_000 {
			q.LimitTokens = v
		}
	}
	if resetTok := getHeaderCaseInsensitive(h, "x-ratelimit-reset-tokens"); resetTok != "" {
		q.ResetTokens = parseDurationOrSeconds(resetTok)
	}

	// Anthropic Style: anthropic-ratelimit-requests-remaining, anthropic-ratelimit-tokens-remaining
	if !q.HasRequestsQuota {
		if remReq := getHeaderCaseInsensitive(h, "anthropic-ratelimit-requests-remaining"); remReq != "" {
			if v, err := strconv.Atoi(remReq); err == nil && v >= 0 && v <= 10_000_000 {
				q.RemainingRequests = v
				q.HasRequestsQuota = true
			}
		}
		if limReq := getHeaderCaseInsensitive(h, "anthropic-ratelimit-requests-limit"); limReq != "" {
			if v, err := strconv.Atoi(limReq); err == nil && v >= 0 && v <= 10_000_000 {
				q.LimitRequests = v
			}
		}
	}
	if !q.HasTokensQuota {
		if remTok := getHeaderCaseInsensitive(h, "anthropic-ratelimit-tokens-remaining"); remTok != "" {
			if v, err := strconv.Atoi(remTok); err == nil && v >= 0 && v <= 1_000_000_000 {
				q.RemainingTokens = v
				q.HasTokensQuota = true
			}
		}
		if limTok := getHeaderCaseInsensitive(h, "anthropic-ratelimit-tokens-limit"); limTok != "" {
			if v, err := strconv.Atoi(limTok); err == nil && v >= 0 && v <= 1_000_000_000 {
				q.LimitTokens = v
			}
		}
	}

	// Standard IETF Style: RateLimit-Remaining, RateLimit-Limit, RateLimit-Reset
	if !q.HasRequestsQuota {
		if remReq := getHeaderCaseInsensitive(h, "ratelimit-remaining"); remReq != "" {
			if v, err := strconv.Atoi(remReq); err == nil && v >= 0 && v <= 10_000_000 {
				q.RemainingRequests = v
				q.HasRequestsQuota = true
			}
		}
		if limReq := getHeaderCaseInsensitive(h, "ratelimit-limit"); limReq != "" {
			if v, err := strconv.Atoi(limReq); err == nil && v >= 0 && v <= 10_000_000 {
				q.LimitRequests = v
			}
		}
		if resetReq := getHeaderCaseInsensitive(h, "ratelimit-reset"); resetReq != "" {
			q.ResetRequests = parseDurationOrSeconds(resetReq)
		}
	}

	// Discard contradictory numbers where remaining greatly exceeds limit
	if q.LimitRequests > 0 && q.RemainingRequests > q.LimitRequests*5 {
		q.HasRequestsQuota = false
	}
	if q.LimitTokens > 0 && q.RemainingTokens > q.LimitTokens*5 {
		q.HasTokensQuota = false
	}

	return q
}

func getHeaderCaseInsensitive(h http.Header, name string) string {
	lower := strings.ToLower(name)
	for k, v := range h {
		if strings.ToLower(k) == lower && len(v) > 0 {
			return strings.TrimSpace(v[0])
		}
	}
	return ""
}

func parseDurationOrSeconds(val string) time.Duration {
	trimmed := strings.TrimSpace(val)
	if d, err := time.ParseDuration(trimmed); err == nil {
		return d
	}
	if sec, err := strconv.Atoi(trimmed); err == nil && sec >= 0 {
		return time.Duration(sec) * time.Second
	}
	if f, err := strconv.ParseFloat(trimmed, 64); err == nil && f >= 0 {
		return time.Duration(f * float64(time.Second))
	}
	return 0
}
