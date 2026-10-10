package server

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/model-scheduler/model-scheduler/internal/config"
	"github.com/model-scheduler/model-scheduler/internal/model"
	"github.com/model-scheduler/model-scheduler/internal/scheduler"
	"github.com/model-scheduler/model-scheduler/internal/upstream"
)

func TestServer_HealthzAndStatus(t *testing.T) {
	mockUpstream := upstream.NewMockUpstream(upstream.MockConfig{})
	mockServer := httptest.NewServer(mockUpstream)
	defer mockServer.Close()

	cfg := config.ServerConfig{
		ListenAddr:     "127.0.0.1:0",
		UpstreamURL:    mockServer.URL,
		MaxConcurrency: 2,
		MaxQueueSize:   5,
		MaxQueueWait:   1 * time.Second,
	}

	client, err := upstream.NewClient(upstream.ClientConfig{BaseURL: mockServer.URL})
	if err != nil {
		t.Fatalf("failed to create upstream client: %v", err)
	}

	sched := scheduler.NewScheduler(scheduler.Config{
		MaxConcurrency: cfg.MaxConcurrency,
		MaxQueueSize:   cfg.MaxQueueSize,
		MaxQueueWait:   cfg.MaxQueueWait,
	})

	srv, err := NewServer(cfg, client, sched)
	if err != nil {
		t.Fatalf("failed to create server: %v", err)
	}

	testServer := httptest.NewServer(srv.httpServer.Handler)
	defer testServer.Close()

	// 1. Test /healthz
	resp, err := http.Get(testServer.URL + "/healthz")
	if err != nil {
		t.Fatalf("healthz request failed: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("healthz status %d, expected 200", resp.StatusCode)
	}

	var healthBody map[string]string
	_ = json.NewDecoder(resp.Body).Decode(&healthBody)
	if healthBody["status"] != "ok" {
		t.Errorf("expected health status 'ok', got '%s'", healthBody["status"])
	}

	// 2. Test /status
	statusResp, err := http.Get(testServer.URL + "/status")
	if err != nil {
		t.Fatalf("status request failed: %v", err)
	}
	defer statusResp.Body.Close()
	if statusResp.StatusCode != http.StatusOK {
		t.Errorf("status response %d, expected 200", statusResp.StatusCode)
	}

	var stats scheduler.Stats
	if err := json.NewDecoder(statusResp.Body).Decode(&stats); err != nil {
		t.Fatalf("failed to decode stats: %v", err)
	}
	if stats.MaxConcurrency != 2 || stats.MaxQueueCapacity != 5 {
		t.Errorf("unexpected stats limits: %+v", stats)
	}
}

func TestServer_ChatCompletions_Success(t *testing.T) {
	mockUpstream := upstream.NewMockUpstream(upstream.MockConfig{
		BaseLatency: 10 * time.Millisecond,
	})
	mockServer := httptest.NewServer(mockUpstream)
	defer mockServer.Close()

	client, err := upstream.NewClient(upstream.ClientConfig{BaseURL: mockServer.URL})
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}

	sched := scheduler.NewScheduler(scheduler.Config{
		MaxConcurrency: 5,
		MaxQueueSize:   10,
		MaxQueueWait:   2 * time.Second,
	})

	srv, err := NewServer(config.DefaultConfig(), client, sched)
	if err != nil {
		t.Fatalf("failed to create server: %v", err)
	}

	testServer := httptest.NewServer(srv.httpServer.Handler)
	defer testServer.Close()

	reqBody := []byte(`{"model":"gpt-4o","messages":[{"role":"user","content":"ping"}]}`)
	httpReq, _ := http.NewRequest(http.MethodPost, testServer.URL+"/v1/chat/completions", bytes.NewReader(reqBody))
	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set("X-Run-ID", "agent-run-123")
	httpReq.Header.Set("X-Workload-Class", "production")

	resp, err := http.DefaultClient.Do(httpReq)
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 OK, got %d", resp.StatusCode)
	}

	// Verify headers injected by scheduler
	if resp.Header.Get("X-Scheduler-Run-ID") != "agent-run-123" {
		t.Errorf("expected X-Scheduler-Run-ID 'agent-run-123', got '%s'", resp.Header.Get("X-Scheduler-Run-ID"))
	}
	if resp.Header.Get("X-Scheduler-Priority") != "100" {
		t.Errorf("expected X-Scheduler-Priority '100', got '%s'", resp.Header.Get("X-Scheduler-Priority"))
	}
	if resp.Header.Get("X-Scheduler-Wait-Ms") == "" {
		t.Errorf("expected X-Scheduler-Wait-Ms header present")
	}

	var chatResp upstream.ChatCompletionResponse
	if err := json.NewDecoder(resp.Body).Decode(&chatResp); err != nil {
		t.Fatalf("failed to decode completion response: %v", err)
	}
	if chatResp.Model != "gpt-4o" {
		t.Errorf("expected model gpt-4o, got %s", chatResp.Model)
	}
}

func TestServer_PriorityScheduling(t *testing.T) {
	// Upstream with 100ms latency to create backpressure
	mockUpstream := upstream.NewMockUpstream(upstream.MockConfig{
		BaseLatency: 80 * time.Millisecond,
	})
	mockServer := httptest.NewServer(mockUpstream)
	defer mockServer.Close()

	client, err := upstream.NewClient(upstream.ClientConfig{BaseURL: mockServer.URL})
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}

	// MaxConcurrency: 1 slot so subsequent requests queue up
	sched := scheduler.NewScheduler(scheduler.Config{
		MaxConcurrency: 1,
		MaxQueueSize:   10,
		MaxQueueWait:   5 * time.Second,
	})

	srv, err := NewServer(config.DefaultConfig(), client, sched)
	if err != nil {
		t.Fatalf("failed to create server: %v", err)
	}

	testServer := httptest.NewServer(srv.httpServer.Handler)
	defer testServer.Close()

	var completionOrder []string
	var mu sync.Mutex
	var wg sync.WaitGroup

	sendRequest := func(runID, class string, priority int) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			reqBody := []byte(`{"model":"gpt-4o","messages":[{"role":"user","content":"test"}]}`)
			req, _ := http.NewRequest(http.MethodPost, testServer.URL+"/v1/chat/completions", bytes.NewReader(reqBody))
			req.Header.Set("Content-Type", "application/json")
			req.Header.Set("X-Run-ID", runID)
			if class != "" {
				req.Header.Set("X-Workload-Class", class)
			}
			if priority > 0 {
				req.Header.Set("X-Priority", fmt.Sprintf("%d", priority))
			}

			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				t.Errorf("request %s failed: %v", runID, err)
				return
			}
			defer resp.Body.Close()
			_, _ = io.ReadAll(resp.Body)

			mu.Lock()
			completionOrder = append(completionOrder, runID)
			mu.Unlock()
		}()
	}

	// Request 1: occupies the single concurrency slot
	sendRequest("occupier", "batch", 25)
	time.Sleep(20 * time.Millisecond) // Ensure occupier acquires slot

	// Queued requests: batch, background, production
	sendRequest("batch-job", "batch", 25)
	time.Sleep(10 * time.Millisecond)

	sendRequest("background-job", "background", 50)
	time.Sleep(10 * time.Millisecond)

	sendRequest("production-job", "production", 100)
	time.Sleep(10 * time.Millisecond)

	wg.Wait()

	// Occupier finishes first, then queued items MUST finish in priority order:
	// production (100) -> background (50) -> batch (25)
	if len(completionOrder) != 4 {
		t.Fatalf("expected 4 completed requests, got %d", len(completionOrder))
	}
	if completionOrder[0] != "occupier" {
		t.Errorf("expected occupier first, got %s", completionOrder[0])
	}
	if completionOrder[1] != "production-job" {
		t.Errorf("expected production-job second, got %s", completionOrder[1])
	}
	if completionOrder[2] != "background-job" {
		t.Errorf("expected background-job third, got %s", completionOrder[2])
	}
	if completionOrder[3] != "batch-job" {
		t.Errorf("expected batch-job fourth, got %s", completionOrder[3])
	}
}

func TestServer_QueueFullRejection(t *testing.T) {
	mockUpstream := upstream.NewMockUpstream(upstream.MockConfig{
		BaseLatency: 100 * time.Millisecond,
	})
	mockServer := httptest.NewServer(mockUpstream)
	defer mockServer.Close()

	client, err := upstream.NewClient(upstream.ClientConfig{BaseURL: mockServer.URL})
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}

	// MaxConcurrency: 1, MaxQueueSize: 1
	sched := scheduler.NewScheduler(scheduler.Config{
		MaxConcurrency: 1,
		MaxQueueSize:   1,
		MaxQueueWait:   2 * time.Second,
	})

	srv, err := NewServer(config.DefaultConfig(), client, sched)
	if err != nil {
		t.Fatalf("failed to create server: %v", err)
	}

	testServer := httptest.NewServer(srv.httpServer.Handler)
	defer testServer.Close()

	var wg sync.WaitGroup
	var rejCount atomic.Int32

	// Launch 3 requests simultaneously: 1 active, 1 queued, 1 rejected
	for i := 0; i < 3; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			reqBody := []byte(`{"model":"gpt-4o","messages":[{"role":"user","content":"test"}]}`)
			req, _ := http.NewRequest(http.MethodPost, testServer.URL+"/v1/chat/completions", bytes.NewReader(reqBody))
			req.Header.Set("Content-Type", "application/json")
			req.Header.Set("X-Run-ID", fmt.Sprintf("run-%d", id))

			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				return
			}
			defer resp.Body.Close()

			if resp.StatusCode == http.StatusServiceUnavailable {
				rejCount.Add(1)
				var errResp model.OpenAIErrorEnvelope
				_ = json.NewDecoder(resp.Body).Decode(&errResp)
				if errResp.Error.Type != "capacity_exceeded" {
					t.Errorf("expected type capacity_exceeded, got %s", errResp.Error.Type)
				}
			}
		}(i)
	}

	wg.Wait()

	if rejCount.Load() != 1 {
		t.Fatalf("expected exactly 1 rejection, got %d", rejCount.Load())
	}
}
