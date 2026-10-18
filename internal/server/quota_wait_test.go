package server

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/LinuxTerminali/aegisllm/internal/config"
	"github.com/LinuxTerminali/aegisllm/internal/upstream"
)

// The core promise: when the provider quota is the bottleneck, callers wait and succeed
// instead of receiving a 429 from the scheduler. The scheduler is NOT told the limit;
// it must learn it from the mock's x-ratelimit-limit-* headers.
func TestServer_WaitsForQuotaInsteadOfRejecting(t *testing.T) {
	mockUpstream := upstream.NewMockUpstream(upstream.MockConfig{
		BaseLatency:    2 * time.Millisecond,
		RateLimitRPS:   10, // 600 RPM upstream
		RateLimitBurst: 5,
	})
	mockServer := httptest.NewServer(mockUpstream)
	defer mockServer.Close()

	client, err := upstream.NewClient(upstream.ClientConfig{BaseURL: mockServer.URL})
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}

	cfg := config.ServerConfig{
		ListenAddr:      "127.0.0.1:0",
		UpstreamURL:     mockServer.URL,
		MaxConcurrency:  8,
		MaxQueueSize:    100,
		MaxQueueWait:    15 * time.Second,
		AutoLearnLimits: true, // no -rpm/-tpm: learn from headers
		MaxRetries:      3,
		InitialBackoff:  20 * time.Millisecond,
		MaxBackoff:      200 * time.Millisecond,
	}
	srv, err := NewServer(cfg, client, nil)
	if err != nil {
		t.Fatalf("failed to create server: %v", err)
	}
	testServer := httptest.NewServer(srv.Handler())
	defer testServer.Close()

	const total = 30 // 30 requests at 10 RPS with burst 5 needs ~2.5s
	var wg sync.WaitGroup
	codes := make([]int, total)
	body := []byte(`{"model":"gpt-4o","messages":[{"role":"user","content":"hi"}]}`)
	for i := 0; i < total; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			resp, err := http.Post(testServer.URL+"/v1/chat/completions", "application/json", bytes.NewReader(body))
			if err != nil {
				codes[i] = -1
				return
			}
			resp.Body.Close()
			codes[i] = resp.StatusCode
		}(i)
	}
	wg.Wait()

	ok := 0
	for _, c := range codes {
		if c == http.StatusOK {
			ok++
		}
	}
	if ok != total {
		t.Fatalf("expected all %d requests to succeed by waiting for quota, got %d OK: %v", total, ok, codes)
	}

	stats := srv.Scheduler().Stats()
	if !stats.Capacity.LimitsLearned || stats.Capacity.RequestsPerMinute != 600 {
		t.Fatalf("expected 600 RPM learned from headers, got %+v", stats.Capacity)
	}
	if stats.TotalRejectedCapacity != 0 {
		t.Fatalf("expected no capacity rejections, got %d", stats.TotalRejectedCapacity)
	}
}

// Anthropic Messages API: /v1/messages is routed, and usage.input_tokens/output_tokens
// reconcile the token bucket.
func TestServer_AnthropicMessagesRouteAndUsage(t *testing.T) {
	anthropic := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/messages" {
			http.Error(w, "wrong path: "+r.URL.Path, http.StatusNotFound)
			return
		}
		if r.Header.Get("x-api-key") == "" {
			http.Error(w, "missing x-api-key", http.StatusUnauthorized)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("anthropic-ratelimit-requests-limit", "50")
		w.Header().Set("anthropic-ratelimit-requests-remaining", "49")
		w.Header().Set("anthropic-ratelimit-tokens-limit", "40000")
		w.Header().Set("anthropic-ratelimit-tokens-remaining", "39000")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"id":      "msg_01",
			"type":    "message",
			"role":    "assistant",
			"content": []map[string]any{{"type": "text", "text": "hello"}},
			"usage":   map[string]int{"input_tokens": 120, "output_tokens": 30},
		})
	}))
	defer anthropic.Close()

	client, err := upstream.NewClient(upstream.ClientConfig{
		BaseURL:      anthropic.URL,
		APIKey:       "test-key",
		ProviderType: "anthropic",
	})
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}
	cfg := config.ServerConfig{
		ListenAddr:      "127.0.0.1:0",
		UpstreamURL:     anthropic.URL,
		ProviderType:    "anthropic",
		MaxConcurrency:  4,
		MaxQueueSize:    10,
		MaxQueueWait:    2 * time.Second,
		AutoLearnLimits: true,
	}
	srv, err := NewServer(cfg, client, nil)
	if err != nil {
		t.Fatalf("failed to create server: %v", err)
	}
	testServer := httptest.NewServer(srv.Handler())
	defer testServer.Close()

	body := []byte(`{"model":"claude-sonnet-4-5","max_tokens":256,"system":"be brief","messages":[{"role":"user","content":[{"type":"text","text":"hi"}]}]}`)
	resp, err := http.Post(testServer.URL+"/v1/messages", "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("post failed: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 from /v1/messages, got %d", resp.StatusCode)
	}

	stats := srv.Scheduler().Stats()
	if stats.Capacity.TotalTokensReconciled != 150 {
		t.Fatalf("expected 150 tokens reconciled from Anthropic usage, got %d", stats.Capacity.TotalTokensReconciled)
	}
	if !stats.Capacity.LimitsLearned || stats.Capacity.RequestsPerMinute != 50 || stats.Capacity.TokensPerMinute != 40000 {
		t.Fatalf("expected Anthropic limits learned (50 RPM / 40000 TPM), got %+v", stats.Capacity)
	}
}

// An upstream 429 with Retry-After holds back the queue instead of letting everyone pile on.
func TestServer_Upstream429DrainsSharedBucket(t *testing.T) {
	var mu sync.Mutex
	calls := 0
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		calls++
		n := calls
		mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("x-ratelimit-limit-requests", "6000")
		if n == 1 {
			w.Header().Set("Retry-After", "1")
			w.WriteHeader(http.StatusTooManyRequests)
			_, _ = w.Write([]byte(`{"error":{"message":"slow down","type":"rate_limit_error"}}`))
			return
		}
		_, _ = w.Write([]byte(`{"choices":[],"usage":{"total_tokens":5}}`))
	}))
	defer up.Close()

	client, err := upstream.NewClient(upstream.ClientConfig{BaseURL: up.URL})
	if err != nil {
		t.Fatalf("client: %v", err)
	}
	cfg := config.ServerConfig{
		ListenAddr:      "127.0.0.1:0",
		UpstreamURL:     up.URL,
		MaxConcurrency:  4,
		MaxQueueSize:    10,
		MaxQueueWait:    5 * time.Second,
		AutoLearnLimits: true,
		MaxRetries:      2,
		InitialBackoff:  10 * time.Millisecond,
		MaxBackoff:      50 * time.Millisecond,
	}
	srv, err := NewServer(cfg, client, nil)
	if err != nil {
		t.Fatalf("server: %v", err)
	}
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()

	body := []byte(`{"model":"gpt-4o","messages":[{"role":"user","content":"hi"}]}`)
	start := time.Now()
	resp, err := http.Post(ts.URL+"/v1/chat/completions", "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("post: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected retry to succeed, got %d", resp.StatusCode)
	}

	// The second request arrives after the 429 and must be held for the Retry-After window.
	resp2, err := http.Post(ts.URL+"/v1/chat/completions", "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("post 2: %v", err)
	}
	resp2.Body.Close()
	if resp2.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 for second request, got %d", resp2.StatusCode)
	}
	if el := time.Since(start); el < 900*time.Millisecond {
		t.Fatalf("expected the shared bucket to hold requests for Retry-After (~1s), total elapsed %v", el)
	}
}

// -rpm 30 with no burst must allow 30 requests at once, as the provider does. A 10% rule
// in config once reduced this to 3 (seen live against Groq: 3 immediate, then 1 per 2s).
func TestServer_ConfiguredRPMUsesFullWindowBurst(t *testing.T) {
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"choices":[],"usage":{"total_tokens":3}}`))
	}))
	defer up.Close()
	client, _ := upstream.NewClient(upstream.ClientConfig{BaseURL: up.URL})
	cfg := config.ServerConfig{
		ListenAddr: "127.0.0.1:0", UpstreamURL: up.URL,
		MaxConcurrency: 32, MaxQueueSize: 100, MaxQueueWait: 5 * time.Second,
		RequestsPerMinute: 30, AutoLearnLimits: false,
	}
	if err := cfg.Validate(); err != nil {
		t.Fatal(err)
	}
	srv, err := NewServer(cfg, client, nil)
	if err != nil {
		t.Fatal(err)
	}
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()

	if got := srv.Scheduler().Stats().Capacity.MaxRequestCapacity; got != 30 {
		t.Fatalf("expected burst = full window (30), got %v", got)
	}

	body := []byte(`{"model":"m","messages":[{"role":"user","content":"hi"}]}`)
	start := time.Now()
	var wg sync.WaitGroup
	for i := 0; i < 30; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			resp, err := http.Post(ts.URL+"/v1/chat/completions", "application/json", bytes.NewReader(body))
			if err == nil {
				resp.Body.Close()
			}
		}()
	}
	wg.Wait()
	if el := time.Since(start); el > 1500*time.Millisecond {
		t.Fatalf("30 requests within the window should not be paced; took %v", el)
	}
	if st := srv.Scheduler().Stats(); st.Capacity.TotalPacedRequests != 0 {
		t.Fatalf("expected no pacing inside the burst, got %d paced", st.Capacity.TotalPacedRequests)
	}
}
