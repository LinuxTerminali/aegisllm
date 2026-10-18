package server

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/LinuxTerminali/aegisllm/internal/capacity"
	"github.com/LinuxTerminali/aegisllm/internal/config"
	"github.com/LinuxTerminali/aegisllm/internal/model"
	"github.com/LinuxTerminali/aegisllm/internal/scheduler"
)

// Test concurrent requests competing for the final available capacity slot
func TestServer_ConcurrentCapacityCompetition(t *testing.T) {
	upstreamHit := atomic.Int64{}
	upBlock := make(chan struct{})

	mockUpstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		upstreamHit.Add(1)
		<-upBlock
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"choices":[{"message":{"content":"ok"}}],"usage":{"total_tokens":10}}`))
	}))
	defer mockUpstream.Close()
	defer close(upBlock)

	cfg := config.DefaultConfig()
	cfg.ListenAddr = "127.0.0.1:0"
	cfg.UpstreamURL = mockUpstream.URL
	cfg.MaxConcurrency = 2
	cfg.MaxQueueSize = 2
	cfg.MaxQueueWait = 10 * time.Second
	cfg.SchedulingPolicy = "fifo"

	srv, err := NewServer(cfg, nil, nil)
	if err != nil {
		t.Fatalf("failed to create server: %v", err)
	}

	proxyServer := httptest.NewServer(srv.Handler())
	defer proxyServer.Close()

	// Launch 5 concurrent requests: 2 should acquire execution, 2 should queue, 1 should be rejected (503)
	const concurrent = 5
	var wg sync.WaitGroup
	statusCodes := make([]int, concurrent)

	for i := 0; i < concurrent; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			reqBody := bytes.NewReader([]byte(`{"model":"gpt-4o","messages":[{"role":"user","content":"test"}]}`))
			req, _ := http.NewRequest(http.MethodPost, proxyServer.URL+"/v1/chat/completions", reqBody)
			req.Header.Set("Content-Type", "application/json")

			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				t.Errorf("request %d failed: %v", idx, err)
				return
			}
			defer resp.Body.Close()
			statusCodes[idx] = resp.StatusCode
		}(i)
	}

	// Give a short window for all 5 requests to arrive and compete
	time.Sleep(100 * time.Millisecond)

	// Unblock upstream
	upBlock <- struct{}{}
	upBlock <- struct{}{}
	upBlock <- struct{}{}
	upBlock <- struct{}{}

	wg.Wait()

	rejection503Count := 0
	success200Count := 0
	for _, sc := range statusCodes {
		if sc == http.StatusServiceUnavailable {
			rejection503Count++
		} else if sc == http.StatusOK {
			success200Count++
		}
	}

	if rejection503Count != 1 {
		t.Errorf("expected exactly 1 rejection (503 queue_full), got %d (all: %v)", rejection503Count, statusCodes)
	}
	if success200Count != 4 {
		t.Errorf("expected 4 successful completions, got %d", success200Count)
	}
}

// Cancellation during pacing, queue wait, upstream execution, and retry backoff
func TestServer_CancellationAtEachStage(t *testing.T) {
	t.Run("CancellationDuringPacing", func(t *testing.T) {
		// Setup tracker with tight RPM so next request is forced to pace
		capCfg := capacity.Config{
			RequestsPerMinute: 60, // 1 req/sec
			RequestBurst:      1,
			MaxPacingDelay:    5 * time.Second,
		}
		tracker := capacity.NewDualTokenBucketTracker(capCfg)
		sched := scheduler.NewSchedulerWithDependencies(scheduler.Config{
			MaxConcurrency: 5,
			MaxQueueSize:   10,
			MaxQueueWait:   10 * time.Second,
		}, tracker, nil)

		// Exhaust burst token
		ctx := context.Background()
		l1, err := sched.Acquire(ctx, model.RequestMetadata{RequestID: "r1"}, []byte(`{}`))
		if err != nil {
			t.Fatalf("first acquire failed: %v", err)
		}
		defer l1.Release()

		// Second request must pace ~1s. Cancel context while pacing.
		ctxCancel, cancel := context.WithCancel(context.Background())
		time.AfterFunc(30*time.Millisecond, cancel)

		_, err = sched.Acquire(ctxCancel, model.RequestMetadata{RequestID: "r2"}, []byte(`{}`))
		if err == nil {
			t.Fatalf("expected cancellation error during pacing, got nil")
		}

		// Verify concurrency slot was NOT leaked
		stats := sched.Stats()
		if stats.ActiveRequests != 1 {
			t.Errorf("expected 1 active request, got %d", stats.ActiveRequests)
		}
		if stats.TotalCanceled != 1 {
			t.Errorf("expected total canceled = 1, got %d", stats.TotalCanceled)
		}
	})

	t.Run("CancellationDuringQueueWait", func(t *testing.T) {
		sched := scheduler.NewScheduler(scheduler.Config{
			MaxConcurrency: 1,
			MaxQueueSize:   5,
			MaxQueueWait:   10 * time.Second,
		})

		l1, _ := sched.Acquire(context.Background(), model.RequestMetadata{RequestID: "r1"}, []byte(`{}`))
		defer l1.Release()

		ctxCancel, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
		defer cancel()

		_, err := sched.Acquire(ctxCancel, model.RequestMetadata{RequestID: "r2"}, []byte(`{}`))
		if err == nil {
			t.Fatalf("expected cancellation error while queued, got nil")
		}

		stats := sched.Stats()
		if stats.QueuedRequests != 0 {
			t.Errorf("expected 0 queued requests after cancellation, got %d", stats.QueuedRequests)
		}
	})

	t.Run("CancellationDuringUpstreamExecution", func(t *testing.T) {
		blockUpstream := make(chan struct{})
		mock := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			select {
			case <-blockUpstream:
				w.WriteHeader(200)
			case <-r.Context().Done():
				// Request canceled upstream
				return
			}
		}))
		defer mock.Close()
		defer close(blockUpstream)

		cfg := config.DefaultConfig()
		cfg.ListenAddr = "127.0.0.1:0"
		cfg.UpstreamURL = mock.URL
		cfg.MaxConcurrency = 1
		cfg.MaxQueueSize = 5
		cfg.SchedulingPolicy = "fifo"

		srv, err := NewServer(cfg, nil, nil)
		if err != nil {
			t.Fatalf("failed to create server: %v", err)
		}
		proxy := httptest.NewServer(srv.Handler())
		defer proxy.Close()

		ctxCancel, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
		defer cancel()

		req, _ := http.NewRequestWithContext(ctxCancel, http.MethodPost, proxy.URL+"/v1/chat/completions", bytes.NewReader([]byte(`{}`)))
		_, _ = http.DefaultClient.Do(req)

		// Wait briefly and verify slot is freed
		time.Sleep(50 * time.Millisecond)
		stats := srv.Scheduler().Stats()
		if stats.ActiveRequests != 0 {
			t.Errorf("expected 0 active requests after client cancellation during upstream execution, got %d", stats.ActiveRequests)
		}
	})

	t.Run("CancellationDuringRetryBackoff", func(t *testing.T) {
		mock := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Retry-After", "5") // 5 second backoff
			w.WriteHeader(http.StatusTooManyRequests)
		}))
		defer mock.Close()

		cfg := config.DefaultConfig()
		cfg.ListenAddr = "127.0.0.1:0"
		cfg.UpstreamURL = mock.URL
		cfg.MaxConcurrency = 2
		cfg.MaxQueueSize = 5
		cfg.SchedulingPolicy = "fifo"
		cfg.MaxRetries = 3

		srv, err := NewServer(cfg, nil, nil)
		if err != nil {
			t.Fatalf("failed to create server: %v", err)
		}
		proxy := httptest.NewServer(srv.Handler())
		defer proxy.Close()

		ctxCancel, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		defer cancel()

		start := time.Now()
		req, _ := http.NewRequestWithContext(ctxCancel, http.MethodPost, proxy.URL+"/v1/chat/completions", bytes.NewReader([]byte(`{}`)))
		_, _ = http.DefaultClient.Do(req)

		// Must abort without waiting for the full 5 second Retry-After!
		if dur := time.Since(start); dur > 1*time.Second {
			t.Errorf("retry backoff did not abort promptly on cancellation, took %v", dur)
		}
	})
}

// Provider quota headers disagree with local estimates or are malformed
func TestServer_MalformedQuotaHeadersIgnored(t *testing.T) {
	var respHeaders http.Header

	mock := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		for k, vv := range respHeaders {
			for _, v := range vv {
				w.Header().Add(k, v)
			}
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"choices":[{"message":{"content":"ok"}}],"usage":{"total_tokens":10}}`))
	}))
	defer mock.Close()

	cfg := config.DefaultConfig()
	cfg.ListenAddr = "127.0.0.1:0"
	cfg.UpstreamURL = mock.URL
	cfg.MaxConcurrency = 5
	cfg.MaxQueueSize = 10
	cfg.SchedulingPolicy = "fifo"
	cfg.RequestsPerMinute = 600
	cfg.RequestBurst = 50
	cfg.TokensPerMinute = 60000
	cfg.TokenBurst = 5000

	srv, _ := NewServer(cfg, nil, nil)
	proxy := httptest.NewServer(srv.Handler())
	defer proxy.Close()

	// Scenario A: Provider reports lower quota (downward synchronization)
	respHeaders = http.Header{
		"x-ratelimit-remaining-tokens": []string{"200"},
	}
	req, _ := http.NewRequest(http.MethodPost, proxy.URL+"/v1/chat/completions", bytes.NewReader([]byte(`{}`)))
	resp, err := http.DefaultClient.Do(req)
	if err != nil || resp.StatusCode != 200 {
		t.Fatalf("request failed: %v", err)
	}
	_ = resp.Body.Close()

	capStats := srv.Scheduler().Stats().Capacity
	if capStats.AvailableTokenTokens > 500 {
		t.Errorf("expected downward synchronization from 5000 to ~200 tokens, got %f", capStats.AvailableTokenTokens)
	}

	// Scenario B: Provider returns contradictory or malformed quota headers
	respHeaders = http.Header{
		"x-ratelimit-remaining-tokens":   []string{"-999"},
		"x-ratelimit-remaining-requests": []string{"not-a-number"},
		"x-ratelimit-limit-tokens":       []string{"100"},
	}
	req, _ = http.NewRequest(http.MethodPost, proxy.URL+"/v1/chat/completions", bytes.NewReader([]byte(`{}`)))
	resp, err = http.DefaultClient.Do(req)
	if err != nil || resp.StatusCode != 200 {
		t.Fatalf("request failed: %v", err)
	}
	_ = resp.Body.Close()

	// Ensure local state was not corrupted into negative numbers or NaN
	capStats = srv.Scheduler().Stats().Capacity
	if capStats.AvailableTokenTokens < 0 {
		t.Errorf("token capacity corrupted into negative value: %f", capStats.AvailableTokenTokens)
	}
}

// Missing or malformed usage metadata
func TestServer_MissingUsageMetadata(t *testing.T) {
	mock := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		// Missing usage object entirely
		_, _ = w.Write([]byte(`{"choices":[{"message":{"content":"ok"}}]}`))
	}))
	defer mock.Close()

	cfg := config.DefaultConfig()
	cfg.ListenAddr = "127.0.0.1:0"
	cfg.UpstreamURL = mock.URL
	cfg.MaxConcurrency = 2
	cfg.MaxQueueSize = 5
	cfg.SchedulingPolicy = "fifo"
	srv, _ := NewServer(cfg, nil, nil)
	proxy := httptest.NewServer(srv.Handler())
	defer proxy.Close()

	req, _ := http.NewRequest(http.MethodPost, proxy.URL+"/v1/chat/completions", bytes.NewReader([]byte(`{}`)))
	resp, err := http.DefaultClient.Do(req)
	if err != nil || resp.StatusCode != 200 {
		t.Fatalf("request failed: %v", err)
	}
	_ = resp.Body.Close()

	// Verify lease was successfully released despite absent usage metadata
	stats := srv.Scheduler().Stats()
	if stats.ActiveRequests != 0 {
		t.Errorf("expected active requests = 0, got %d", stats.ActiveRequests)
	}
	if stats.TotalCompleted != 1 {
		t.Errorf("expected total completed = 1, got %d", stats.TotalCompleted)
	}
}

// Provider returning repeated 429s with Retry-After
func TestServer_Repeated429sWithRetryAfter(t *testing.T) {
	calls := atomic.Int64{}
	mock := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.Header().Set("Retry-After", "0") // 0 sec for instant deterministic test
		w.WriteHeader(http.StatusTooManyRequests)
		_, _ = w.Write([]byte(`{"error":{"message":"Rate limit exceeded","type":"tokens"}}`))
	}))
	defer mock.Close()

	const maxRetries = 2
	cfg := config.DefaultConfig()
	cfg.ListenAddr = "127.0.0.1:0"
	cfg.UpstreamURL = mock.URL
	cfg.MaxConcurrency = 2
	cfg.MaxQueueSize = 5
	cfg.SchedulingPolicy = "fifo"
	cfg.MaxRetries = maxRetries
	cfg.InitialBackoff = 10 * time.Millisecond
	cfg.MaxBackoff = 50 * time.Millisecond

	srv, _ := NewServer(cfg, nil, nil)
	proxy := httptest.NewServer(srv.Handler())
	defer proxy.Close()

	req, _ := http.NewRequest(http.MethodPost, proxy.URL+"/v1/chat/completions", bytes.NewReader([]byte(`{}`)))
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusTooManyRequests {
		t.Errorf("expected final status 429, got %d", resp.StatusCode)
	}

	// Initial attempt (1) + retries (2) = 3 total upstream calls
	if calls.Load() != int64(1+maxRetries) {
		t.Errorf("expected %d calls, got %d", 1+maxRetries, calls.Load())
	}

	retriesHdr := resp.Header.Get("X-Scheduler-Retries")
	if retriesHdr != "2" {
		t.Errorf("expected X-Scheduler-Retries: 2, got %s", retriesHdr)
	}
}

// Permanent errors (400, 401, 403) are never retried
func TestServer_PermanentErrorsNotRetried(t *testing.T) {
	calls := atomic.Int64{}
	mock := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.WriteHeader(http.StatusBadRequest) // 400 Bad Request
		_, _ = w.Write([]byte(`{"error":{"message":"Invalid parameter model","type":"invalid_request_error"}}`))
	}))
	defer mock.Close()

	cfg := config.DefaultConfig()
	cfg.ListenAddr = "127.0.0.1:0"
	cfg.UpstreamURL = mock.URL
	cfg.MaxConcurrency = 2
	cfg.MaxQueueSize = 5
	cfg.SchedulingPolicy = "fifo"
	cfg.MaxRetries = 3
	srv, _ := NewServer(cfg, nil, nil)
	proxy := httptest.NewServer(srv.Handler())
	defer proxy.Close()

	req, _ := http.NewRequest(http.MethodPost, proxy.URL+"/v1/chat/completions", bytes.NewReader([]byte(`{}`)))
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("expected status 400, got %d", resp.StatusCode)
	}

	// Must NOT retry: calls should be exactly 1
	if calls.Load() != 1 {
		t.Errorf("permanent error was retried! expected 1 call, got %d", calls.Load())
	}
}

// Transient 502/503/504 errors are retried and can recover
func TestServer_Transient5xxRetried(t *testing.T) {
	calls := atomic.Int64{}
	mock := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c := calls.Add(1)
		if c < 3 {
			w.WriteHeader(http.StatusBadGateway) // 502 on attempts 1 and 2
			return
		}
		// Attempt 3 succeeds
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"choices":[{"message":{"content":"recovered"}}],"usage":{"total_tokens":5}}`))
	}))
	defer mock.Close()

	cfg := config.DefaultConfig()
	cfg.ListenAddr = "127.0.0.1:0"
	cfg.UpstreamURL = mock.URL
	cfg.MaxConcurrency = 2
	cfg.MaxQueueSize = 5
	cfg.SchedulingPolicy = "fifo"
	cfg.MaxRetries = 3
	cfg.InitialBackoff = 10 * time.Millisecond
	cfg.MaxBackoff = 50 * time.Millisecond

	srv, _ := NewServer(cfg, nil, nil)
	proxy := httptest.NewServer(srv.Handler())
	defer proxy.Close()

	req, _ := http.NewRequest(http.MethodPost, proxy.URL+"/v1/chat/completions", bytes.NewReader([]byte(`{}`)))
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Errorf("expected recovered status 200, got %d", resp.StatusCode)
	}
	if calls.Load() != 3 {
		t.Errorf("expected 3 attempts to recover, got %d", calls.Load())
	}
}

// Queue saturation with mixed priorities and multiple runs
func TestServer_QueueSaturationMixedRuns(t *testing.T) {
	upBlock := make(chan struct{})
	mock := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		<-upBlock
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"choices":[],"usage":{"total_tokens":5}}`))
	}))
	defer mock.Close()
	defer close(upBlock)

	cfg := config.DefaultConfig()
	cfg.ListenAddr = "127.0.0.1:0"
	cfg.UpstreamURL = mock.URL
	cfg.MaxConcurrency = 1 // Occupier takes the 1 slot
	cfg.MaxQueueSize = 3   // Exactly 3 slots in queue
	cfg.SchedulingPolicy = "fair"

	srv, _ := NewServer(cfg, nil, nil)
	proxy := httptest.NewServer(srv.Handler())
	defer proxy.Close()

	// Occupier request
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		req, _ := http.NewRequest(http.MethodPost, proxy.URL+"/v1/chat/completions", bytes.NewReader([]byte(`{}`)))
		req.Header.Set("X-Run-ID", "occupier")
		_, _ = http.DefaultClient.Do(req)
	}()

	time.Sleep(50 * time.Millisecond)

	// Queue 3 requests with mixed priorities across 2 runs
	runConfigs := []struct {
		runID string
		class string
	}{
		{"run-A", "batch"},
		{"run-A", "production"},
		{"run-B", "interactive"},
	}

	for _, rc := range runConfigs {
		wg.Add(1)
		go func(runID, class string) {
			defer wg.Done()
			req, _ := http.NewRequest(http.MethodPost, proxy.URL+"/v1/chat/completions", bytes.NewReader([]byte(`{}`)))
			req.Header.Set("X-Run-ID", runID)
			req.Header.Set("X-Workload-Class", class)
			resp, _ := http.DefaultClient.Do(req)
			if resp != nil {
				_ = resp.Body.Close()
			}
		}(rc.runID, rc.class)
	}

	time.Sleep(100 * time.Millisecond)

	// 5th request arrives when queue is completely full
	overflowReq, _ := http.NewRequest(http.MethodPost, proxy.URL+"/v1/chat/completions", bytes.NewReader([]byte(`{}`)))
	overflowResp, err := http.DefaultClient.Do(overflowReq)
	if err != nil {
		t.Fatalf("overflow request failed: %v", err)
	}
	defer overflowResp.Body.Close()

	if overflowResp.StatusCode != http.StatusServiceUnavailable {
		t.Errorf("expected 503 queue_full for overflow request, got %d", overflowResp.StatusCode)
	}

	// Unblock upstream for remaining 4 requests
	for i := 0; i < 4; i++ {
		upBlock <- struct{}{}
	}
	wg.Wait()
}

// Slow / non-responsive upstream with timeout
func TestServer_SlowUpstreamTimeout(t *testing.T) {
	mock := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(1 * time.Second) // Slow upstream
	}))
	defer mock.Close()

	cfg := config.DefaultConfig()
	cfg.ListenAddr = "127.0.0.1:0"
	cfg.UpstreamURL = mock.URL
	cfg.MaxConcurrency = 2
	cfg.MaxQueueSize = 5
	cfg.SchedulingPolicy = "fifo"
	cfg.UpstreamTimeout = 100 * time.Millisecond // 100ms timeout
	cfg.MaxRetries = 0

	srv, _ := NewServer(cfg, nil, nil)
	proxy := httptest.NewServer(srv.Handler())
	defer proxy.Close()

	req, _ := http.NewRequest(http.MethodPost, proxy.URL+"/v1/chat/completions", bytes.NewReader([]byte(`{}`)))
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusBadGateway {
		t.Errorf("expected 502 bad gateway on upstream timeout, got %d", resp.StatusCode)
	}

	// Ensure lease was released
	if srv.Scheduler().Stats().ActiveRequests != 0 {
		t.Errorf("active requests not freed after timeout")
	}
}

// Graceful shutdown while requests are queued and executing
func TestServer_ShutdownDrainsInFlight(t *testing.T) {
	upBlock := make(chan struct{})
	mock := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		<-upBlock
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"choices":[],"usage":{"total_tokens":5}}`))
	}))
	defer mock.Close()
	defer close(upBlock)

	cfg := config.DefaultConfig()
	cfg.ListenAddr = "127.0.0.1:0"
	cfg.UpstreamURL = mock.URL
	cfg.MaxConcurrency = 2
	cfg.MaxQueueSize = 5
	cfg.SchedulingPolicy = "fifo"

	srv, err := NewServer(cfg, nil, nil)
	if err != nil {
		t.Fatalf("failed to create server: %v", err)
	}

	if err := srv.Listen(); err != nil {
		t.Fatalf("failed to bind listener: %v", err)
	}

	go func() {
		_ = srv.Start()
	}()

	// Send an in-flight request
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		req, _ := http.NewRequest(http.MethodPost, "http://"+srv.Addr()+"/v1/chat/completions", bytes.NewReader([]byte(`{}`)))
		resp, err := http.DefaultClient.Do(req)
		if err == nil {
			_ = resp.Body.Close()
		}
	}()

	time.Sleep(50 * time.Millisecond)

	// Initiate shutdown in background with 2s timeout
	shutdownDone := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		shutdownDone <- srv.Shutdown(ctx)
	}()

	// Unblock upstream so active request can drain cleanly
	upBlock <- struct{}{}
	wg.Wait()

	select {
	case err := <-shutdownDone:
		if err != nil {
			t.Errorf("shutdown returned error: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatalf("shutdown timed out")
	}
}

// Operational Observability /status and /metrics validation
func TestServer_ObservabilityEndpoints(t *testing.T) {
	mock := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"choices":[],"usage":{"total_tokens":10}}`))
	}))
	defer mock.Close()

	cfg := config.DefaultConfig()
	cfg.ListenAddr = "127.0.0.1:0"
	cfg.UpstreamURL = mock.URL
	cfg.MaxConcurrency = 5
	cfg.MaxQueueSize = 10
	cfg.SchedulingPolicy = "fair"
	srv, _ := NewServer(cfg, nil, nil)
	proxy := httptest.NewServer(srv.Handler())
	defer proxy.Close()

	// Send completion request
	req, _ := http.NewRequest(http.MethodPost, proxy.URL+"/v1/chat/completions", bytes.NewReader([]byte(`{}`)))
	req.Header.Set("X-Workload-Class", "production")
	resp, _ := http.DefaultClient.Do(req)
	_ = resp.Body.Close()

	// Query /status
	statusResp, err := http.Get(proxy.URL + "/status")
	if err != nil || statusResp.StatusCode != 200 {
		t.Fatalf("GET /status failed: %v", err)
	}
	defer statusResp.Body.Close()

	var st StatusResponse
	if err := json.NewDecoder(statusResp.Body).Decode(&st); err != nil {
		t.Fatalf("failed to decode /status: %v", err)
	}

	if st.Metrics.RequestsAccepted != 1 || st.Metrics.RequestsCompleted != 1 {
		t.Errorf("expected 1 accepted and completed request in /status, got %+v", st.Metrics)
	}

	// Query /metrics
	metricsResp, err := http.Get(proxy.URL + "/metrics")
	if err != nil || metricsResp.StatusCode != 200 {
		t.Fatalf("GET /metrics failed: %v", err)
	}
	defer metricsResp.Body.Close()

	metricsBytes, _ := io.ReadAll(metricsResp.Body)
	metricsStr := string(metricsBytes)

	if !bytes.Contains(metricsBytes, []byte("scheduler_requests_total{status=\"accepted\"} 1")) {
		t.Errorf("expected /metrics to contain accepted counter, got:\n%s", metricsStr)
	}
	if !bytes.Contains(metricsBytes, []byte("scheduler_active_concurrency")) {
		t.Errorf("expected /metrics to contain active concurrency gauge")
	}
}
