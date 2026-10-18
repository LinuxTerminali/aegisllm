package server

import (
	"bufio"
	crand "crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	mathrand "math/rand"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/LinuxTerminali/aegisllm/internal/capacity"
	"github.com/LinuxTerminali/aegisllm/internal/metrics"
	"github.com/LinuxTerminali/aegisllm/internal/model"
	"github.com/LinuxTerminali/aegisllm/internal/retry"
	"github.com/LinuxTerminali/aegisllm/internal/scheduler"
	"github.com/LinuxTerminali/aegisllm/internal/upstream"
)

const (
	maxRequestBodySize  = 10 * 1024 * 1024 // 10MB limit
	maxResponseBodySize = 10 * 1024 * 1024 // 10MB limit
)

// StatusResponse encapsulates complete runtime health, capacity, queue depths, and metrics.
type StatusResponse struct {
	scheduler.Stats
	Metrics  metrics.StatsSnapshot `json:"metrics"`
	Provider string                `json:"provider"`
}

// Handlers holds dependencies for HTTP route handlers.
type Handlers struct {
	scheduler      *scheduler.Scheduler
	upstreamClient *upstream.Client
	retryPolicy    retry.Policy
	metrics        *metrics.Collector
}

// NewHandlers creates a new Handlers container.
func NewHandlers(s *scheduler.Scheduler, client *upstream.Client, rp retry.Policy, m *metrics.Collector) *Handlers {
	if m == nil {
		m = metrics.NewCollector()
	}
	return &Handlers{
		scheduler:      s,
		upstreamClient: client,
		retryPolicy:    rp,
		metrics:        m,
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

// Status returns current scheduler operational metrics, queue depths, and observability snapshots.
func (h *Handlers) Status(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		model.WriteJSONError(w, http.StatusMethodNotAllowed, "Method not allowed", "invalid_request_error", 405)
		return
	}
	stats := h.scheduler.Stats()
	provName := "unknown"
	if h.upstreamClient != nil && h.upstreamClient.Provider() != nil {
		provName = h.upstreamClient.Provider().Name()
	}

	resp := StatusResponse{
		Stats:    stats,
		Metrics:  h.metrics.Snapshot(),
		Provider: provName,
	}

	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(resp)
}

// Metrics exports Prometheus-compatible exposition metrics with bounded cardinality.
func (h *Handlers) Metrics(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		model.WriteJSONError(w, http.StatusMethodNotAllowed, "Method not allowed", "invalid_request_error", 405)
		return
	}

	stats := h.scheduler.Stats()
	out := h.metrics.FormatPrometheus(
		stats.ActiveRequests,
		stats.MaxConcurrency,
		stats.QueuedRequests,
		stats.MaxQueueCapacity,
		stats.Capacity.AvailableRequestTokens,
		stats.Capacity.MaxRequestCapacity,
		stats.Capacity.AvailableTokenTokens,
		stats.Capacity.MaxTokenCapacity,
		stats.Capacity.TotalReservations,
		stats.Capacity.TotalPacedRequests,
		stats.Capacity.TotalTokensReserved,
		stats.Capacity.TotalTokensReconciled,
	)

	w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(out))
}

// ChatCompletions coordinates admission and proxies OpenAI chat completion requests.
func (h *Handlers) ChatCompletions(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		model.WriteJSONError(w, http.StatusMethodNotAllowed, "Method not allowed", "invalid_request_error", 405)
		return
	}

	start := time.Now()

	// The queue key is always ours: a client-supplied X-Request-ID may be reused across
	// retries or shared by two clients, and a duplicate key corrupts the queue index.
	// The client's ID is echoed back for correlation only.
	reqID := generateRequestID()
	if clientID := r.Header.Get("X-Request-ID"); clientID != "" {
		w.Header().Set("X-Request-ID", clientID)
	}

	// Extract scheduling metadata from headers
	meta := model.ParseRequestMetadata(
		reqID,
		r.Header.Get("X-Run-ID"),
		r.Header.Get("X-Workload-Class"),
		r.Header.Get("X-Priority"),
	)

	// Read bounded request body
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
	wantsStream := upstream.IsStreamingRequest(bodyBytes)

	// Acquire execution lease from scheduler (with body for token estimation)
	lease, err := h.scheduler.Acquire(r.Context(), meta, bodyBytes)
	if err != nil {
		if errors.Is(err, scheduler.ErrQueueFull) {
			h.metrics.RecordRejection("queue_full")
			w.Header().Set("X-Scheduler-Error", "queue_full")
			w.Header().Set("Retry-After", "1")
			model.WriteJSONError(w, http.StatusServiceUnavailable, "Scheduler queue is full, capacity exceeded", "capacity_exceeded", 503)
			return
		}
		if errors.Is(err, capacity.ErrInsufficientCapacity) {
			h.metrics.RecordRejection("capacity_exceeded")
			w.Header().Set("X-Scheduler-Error", "capacity_exceeded")
			w.Header().Set("Retry-After", retryAfterSeconds(h.scheduler.Stats().Capacity.ExhaustedUntilMs, 2))
			model.WriteJSONError(w, http.StatusTooManyRequests, "Capacity rate limit quota exhausted and pacing delay exceeded", "rate_limit_exceeded", 429)
			return
		}
		if errors.Is(err, scheduler.ErrWaitTimeout) {
			h.metrics.RecordRejection("queue_timeout")
			w.Header().Set("X-Scheduler-Error", "queue_timeout")
			w.Header().Set("Retry-After", "5")
			model.WriteJSONError(w, http.StatusGatewayTimeout, "Request exceeded maximum queue wait time", "timeout_error", 504)
			return
		}
		if errors.Is(err, r.Context().Err()) {
			h.metrics.RecordCanceled()
			return
		}
		model.WriteJSONError(w, http.StatusInternalServerError, fmt.Sprintf("Scheduling error: %v", err), "server_error", 500)
		return
	}
	defer lease.Release()

	h.metrics.RecordAccepted(meta.Priority, lease.WaitDuration())

	// Forward with coordinated retry loop
	upstreamStart := time.Now()
	rng := mathrand.New(mathrand.NewSource(time.Now().UnixNano()))
	var resp *http.Response
	var forwardErr error
	retriesCount := 0

	for attempt := 0; attempt <= h.retryPolicy.MaxRetries; attempt++ {
		if r.Context().Err() != nil {
			h.metrics.RecordCanceled()
			return
		}

		if attempt > 0 {
			retriesCount++
			h.metrics.RecordRetry()

			// Parse Retry-After from previous response
			var retryAfter time.Duration
			if resp != nil {
				if ra, ok := upstream.ParseRetryAfter(resp.Header.Get("Retry-After")); ok {
					retryAfter = ra
				}
			}
			// After a 429 the retry must re-enter the quota gate; otherwise it races the
			// paced queue with blind backoff and can lose indefinitely.
			if resp != nil && resp.StatusCode == http.StatusTooManyRequests {
				if err := lease.RePace(r.Context()); err != nil {
					if errors.Is(err, r.Context().Err()) {
						h.metrics.RecordCanceled()
						return
					}
					h.metrics.RecordRejection("capacity_exceeded")
					w.Header().Set("X-Scheduler-Error", "capacity_exceeded")
					w.Header().Set("Retry-After", retryAfterSeconds(h.scheduler.Stats().Capacity.ExhaustedUntilMs, 2))
					model.WriteJSONError(w, http.StatusTooManyRequests, "Capacity rate limit quota exhausted while retrying", "rate_limit_exceeded", 429)
					return
				}
				retryAfter = 0 // the hold was already served by RePace
			}

			backoff := h.retryPolicy.CalculateBackoff(attempt-1, retryAfter, rng)
			timer := time.NewTimer(backoff)
			select {
			case <-timer.C:
			case <-r.Context().Done():
				timer.Stop()
				h.metrics.RecordCanceled()
				return
			}
		}

		resp, forwardErr = h.upstreamClient.Forward(r.Context(), r.URL.RequestURI(), r.Header, bodyBytes)
		if forwardErr != nil {
			if errors.Is(forwardErr, r.Context().Err()) {
				h.metrics.RecordCanceled()
				return
			}
			// Network error on non-first attempt - keep retrying
			if attempt < h.retryPolicy.MaxRetries {
				continue
			}
			h.metrics.RecordFailed(http.StatusBadGateway)
			model.WriteJSONError(w, http.StatusBadGateway, fmt.Sprintf("Upstream provider error: %v", forwardErr), "bad_gateway", 502)
			return
		}

		// Reconcile provider headers after every upstream response
		h.scheduler.ReconcileHeaders(resp.Header)

		// An upstream 429 is the strongest signal that the shared quota is gone:
		// hold back everything queued behind us for the provider's Retry-After.
		if resp.StatusCode == http.StatusTooManyRequests {
			ra, _ := upstream.ParseRetryAfter(resp.Header.Get("Retry-After"))
			h.scheduler.MarkUpstreamRateLimited(ra)
		}

		if !retry.IsRetryable(resp.StatusCode) || attempt == h.retryPolicy.MaxRetries {
			break
		}

		// Retryable status (429, 502, 503, 504) - close body before retry
		_, _ = io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
	}

	if forwardErr != nil {
		h.metrics.RecordFailed(http.StatusBadGateway)
		model.WriteJSONError(w, http.StatusBadGateway, fmt.Sprintf("Upstream provider error: %v", forwardErr), "bad_gateway", 502)
		return
	}
	defer resp.Body.Close()

	// Streaming: pass events through as they arrive. Retries above only ever happen on
	// non-2xx responses, i.e. before the first byte of a stream reaches the client.
	if wantsStream && resp.StatusCode == http.StatusOK {
		h.streamResponse(w, r, resp, lease, meta, start, upstreamStart, retriesCount)
		return
	}

	// Read bounded response body
	respBody, readErr := io.ReadAll(io.LimitReader(resp.Body, maxResponseBodySize))
	if readErr == nil && resp.StatusCode == http.StatusOK {
		if used := upstream.ParseUsageTokens(respBody); used > 0 {
			lease.Reconcile(used)
		}
	}

	// Copy upstream response headers (stripping hop-by-hop headers)
	copyUpstreamHeaders(w.Header(), resp.Header)

	// Attach scheduler telemetry headers
	w.Header().Set("X-Scheduler-Wait-Ms", strconv.FormatInt(lease.WaitDuration().Milliseconds(), 10))
	w.Header().Set("X-Scheduler-Run-ID", meta.RunID)
	w.Header().Set("X-Scheduler-Priority", strconv.Itoa(meta.Priority))
	w.Header().Set("X-Scheduler-Retries", strconv.Itoa(retriesCount))

	totalDur := time.Since(start)
	upstreamDur := time.Since(upstreamStart)

	if resp.StatusCode == http.StatusOK {
		w.WriteHeader(resp.StatusCode)
		if readErr == nil {
			if _, writeErr := w.Write(respBody); writeErr != nil {
				h.metrics.RecordCanceled()
			}
		}
		h.metrics.RecordCompleted(totalDur, upstreamDur, resp.StatusCode)
	} else {
		h.metrics.RecordFailed(resp.StatusCode)
		// Sanitize error before returning to client so no API keys, internal paths, or credentials leak
		safeMsg, errType := h.upstreamClient.SanitizeError(resp.StatusCode, respBody)
		model.WriteJSONError(w, resp.StatusCode, safeMsg, errType, resp.StatusCode)
	}

	// Safe structured logging: NEVER log API keys, prompts, or response bodies!
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

// copyUpstreamHeaders copies response headers to the client, dropping hop-by-hop ones.
func copyUpstreamHeaders(dst, src http.Header) {
	for k, vv := range src {
		lower := strings.ToLower(k)
		if lower == "connection" || lower == "transfer-encoding" || lower == "content-length" {
			continue
		}
		for _, v := range vv {
			dst.Add(k, v)
		}
	}
}

// streamResponse relays an SSE body line by line, flushing on every event boundary, and
// reconciles token usage from the usage events the provider emits.
func (h *Handlers) streamResponse(w http.ResponseWriter, r *http.Request, resp *http.Response, lease *scheduler.Lease, meta model.RequestMetadata, start, upstreamStart time.Time, retriesCount int) {
	copyUpstreamHeaders(w.Header(), resp.Header)
	w.Header().Set("X-Scheduler-Wait-Ms", strconv.FormatInt(lease.WaitDuration().Milliseconds(), 10))
	w.Header().Set("X-Scheduler-Run-ID", meta.RunID)
	w.Header().Set("X-Scheduler-Priority", strconv.Itoa(meta.Priority))
	w.Header().Set("X-Scheduler-Retries", strconv.Itoa(retriesCount))
	w.Header().Set("X-Accel-Buffering", "no")
	w.WriteHeader(http.StatusOK)

	flusher, _ := w.(http.Flusher)
	flush := func() {
		if flusher != nil {
			flusher.Flush()
		}
	}
	flush()

	var usage upstream.StreamUsage
	reader := bufio.NewReaderSize(resp.Body, 64*1024)
	canceled := false
	for {
		line, err := reader.ReadBytes('\n')
		if len(line) > 0 {
			if _, werr := w.Write(line); werr != nil {
				canceled = true
				break
			}
			trimmed := strings.TrimRight(string(line), "\r\n")
			if strings.HasPrefix(trimmed, "data:") {
				usage.Observe([]byte(strings.TrimSpace(trimmed[len("data:"):])))
			}
			if trimmed == "" {
				flush() // event boundary
			}
		}
		if err != nil {
			if err != io.EOF && r.Context().Err() != nil {
				canceled = true
			}
			break
		}
	}
	flush()

	if used := usage.Total(); used > 0 {
		lease.Reconcile(used)
	}

	totalDur := time.Since(start)
	upstreamDur := time.Since(upstreamStart)
	if canceled {
		h.metrics.RecordCanceled()
	} else {
		h.metrics.RecordCompleted(totalDur, upstreamDur, http.StatusOK)
	}

	log.Printf("[SCHEDULER] req_id=%s run_id=%s class=%s priority=%d wait_ms=%d upstream_ms=%d total_ms=%d status=200 stream=true tokens=%d",
		meta.RequestID, meta.RunID, meta.WorkloadClass, meta.Priority,
		lease.WaitDuration().Milliseconds(), upstreamDur.Milliseconds(), totalDur.Milliseconds(), usage.Total())
}

// retryAfterSeconds turns a hold (ms) into a Retry-After value, with a floor so clients
// never retry instantly against a proxy that just told them it is full.
func retryAfterSeconds(holdMs int64, minSeconds int64) string {
	sec := (holdMs + 999) / 1000
	if sec < minSeconds {
		sec = minSeconds
	}
	return strconv.FormatInt(sec, 10)
}

func generateRequestID() string {
	b := make([]byte, 8)
	if _, err := crand.Read(b); err != nil {
		return fmt.Sprintf("req-%d", time.Now().UnixNano())
	}
	return "req-" + hex.EncodeToString(b)
}
