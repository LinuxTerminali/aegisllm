package server

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"strconv"
	"time"

	"github.com/model-scheduler/model-scheduler/internal/model"
	"github.com/model-scheduler/model-scheduler/internal/scheduler"
	"github.com/model-scheduler/model-scheduler/internal/upstream"
)

const maxRequestBodySize = 10 * 1024 * 1024 // 10MB limit

// Handlers holds dependencies for HTTP route handlers.
type Handlers struct {
	scheduler      *scheduler.Scheduler
	upstreamClient *upstream.Client
}

// NewHandlers creates a new Handlers container.
func NewHandlers(s *scheduler.Scheduler, client *upstream.Client) *Handlers {
	return &Handlers{
		scheduler:      s,
		upstreamClient: client,
	}
}

// Healthz returns service liveness status.
func (h *Handlers) Healthz(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		model.WriteJSONError(w, http.StatusMethodNotAllowed, "Method not allowed", "invalid_request_error", 405)
		return
	}
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(`{"status":"ok"}`))
}

// Status returns current scheduler operational metrics and queue depths.
func (h *Handlers) Status(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		model.WriteJSONError(w, http.StatusMethodNotAllowed, "Method not allowed", "invalid_request_error", 405)
		return
	}
	stats := h.scheduler.Stats()
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(stats)
}

// ChatCompletions coordinates admission and proxies OpenAI chat completion requests.
func (h *Handlers) ChatCompletions(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		model.WriteJSONError(w, http.StatusMethodNotAllowed, "Method not allowed", "invalid_request_error", 405)
		return
	}

	start := time.Now()

	// 1. Generate or extract Request ID
	reqID := r.Header.Get("X-Request-ID")
	if reqID == "" {
		reqID = generateRequestID()
	}

	// 2. Extract scheduling metadata from headers
	meta := model.ParseRequestMetadata(
		reqID,
		r.Header.Get("X-Run-ID"),
		r.Header.Get("X-Workload-Class"),
		r.Header.Get("X-Priority"),
	)

	// 3. Read bounded request body
	bodyBytes, err := io.ReadAll(io.LimitReader(r.Body, maxRequestBodySize+1))
	if err != nil {
		model.WriteJSONError(w, http.StatusBadRequest, "Failed to read request body", "invalid_request_error", 400)
		return
	}
	if len(bodyBytes) > maxRequestBodySize {
		model.WriteJSONError(w, http.StatusRequestEntityTooLarge, "Request body exceeds maximum size", "invalid_request_error", 413)
		return
	}
	_ = r.Body.Close()

	// 4. Acquire execution lease from scheduler
	lease, err := h.scheduler.Acquire(r.Context(), meta)
	if err != nil {
		if errors.Is(err, scheduler.ErrQueueFull) {
			model.WriteJSONError(w, http.StatusServiceUnavailable, "Scheduler queue is full, capacity exceeded", "capacity_exceeded", 503)
			return
		}
		if errors.Is(err, scheduler.ErrWaitTimeout) {
			model.WriteJSONError(w, http.StatusGatewayTimeout, "Request exceeded maximum queue wait time", "timeout_error", 504)
			return
		}
		if errors.Is(err, r.Context().Err()) {
			// Client disconnected while waiting
			return
		}
		model.WriteJSONError(w, http.StatusInternalServerError, fmt.Sprintf("Scheduling error: %v", err), "server_error", 500)
		return
	}
	defer lease.Release()

	// 5. Forward request to upstream LLM provider
	upstreamStart := time.Now()
	resp, err := h.upstreamClient.Forward(r.Context(), r.URL.Path, r.Header, bodyBytes)
	if err != nil {
		if errors.Is(err, r.Context().Err()) {
			return
		}
		model.WriteJSONError(w, http.StatusBadGateway, fmt.Sprintf("Upstream provider error: %v", err), "bad_gateway", 502)
		return
	}
	defer resp.Body.Close()

	// 6. Copy upstream response headers
	for k, vv := range resp.Header {
		for _, v := range vv {
			w.Header().Add(k, v)
		}
	}

	// Attach scheduler telemetry headers
	w.Header().Set("X-Scheduler-Wait-Ms", strconv.FormatInt(lease.WaitDuration().Milliseconds(), 10))
	w.Header().Set("X-Scheduler-Run-ID", meta.RunID)
	w.Header().Set("X-Scheduler-Priority", strconv.Itoa(meta.Priority))

	w.WriteHeader(resp.StatusCode)
	_, _ = io.Copy(w, resp.Body)

	// Safe structured logging: NEVER log API keys, prompts, or response bodies!
	totalDur := time.Since(start)
	upstreamDur := time.Since(upstreamStart)
	log.Printf("[SCHEDULER] req_id=%s run_id=%s class=%s priority=%d wait_ms=%d upstream_ms=%d total_ms=%d status=%d",
		meta.RequestID,
		meta.RunID,
		meta.WorkloadClass,
		meta.Priority,
		lease.WaitDuration().Milliseconds(),
		upstreamDur.Milliseconds(),
		totalDur.Milliseconds(),
		resp.StatusCode,
	)
}

func generateRequestID() string {
	b := make([]byte, 8)
	if _, err := rand.Read(b); err != nil {
		return fmt.Sprintf("req-%d", time.Now().UnixNano())
	}
	return "req-" + hex.EncodeToString(b)
}
