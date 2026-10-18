package upstream

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

// MockConfig holds configuration for the mock upstream server.
type MockConfig struct {
	BaseLatency         time.Duration
	MaxAllowedInFlight  int     // Returns 429 if active concurrency exceeds this (0 = unlimited)
	RateLimitRPS        float64 // Returns 429 if requests/sec exceed token bucket rate (0 = disabled)
	RateLimitBurst      int     // Request token bucket capacity
	RateLimitTPS        float64 // Returns 429 if tokens/sec exceed token bucket rate (0 = disabled)
	RateLimitTokenBurst int     // Token rate bucket capacity
	RetryAfterSeconds   int     // Value for Retry-After header on 429 (default 1)
}

// MockUpstream simulates an OpenAI-compatible upstream LLM provider.
type MockUpstream struct {
	config          MockConfig
	activeInFlight  atomic.Int64
	peakInFlight    atomic.Int64
	totalRequests   atomic.Int64
	statusCodesSeen sync.Map

	mu           sync.Mutex
	reqTokens    float64
	maxReqTokens float64
	tokTokens    float64
	maxTokTokens float64
	lastRefill   time.Time
}

// NewMockUpstream creates a new MockUpstream instance.
func NewMockUpstream(cfg MockConfig) *MockUpstream {
	reqBurst := cfg.RateLimitBurst
	if reqBurst <= 0 && cfg.RateLimitRPS > 0 {
		reqBurst = int(cfg.RateLimitRPS)
		if reqBurst < 1 {
			reqBurst = 1
		}
	}

	tokBurst := cfg.RateLimitTokenBurst
	if tokBurst <= 0 && cfg.RateLimitTPS > 0 {
		tokBurst = int(cfg.RateLimitTPS)
		if tokBurst < 100 {
			tokBurst = 100
		}
	}

	retryAfter := cfg.RetryAfterSeconds
	if retryAfter <= 0 {
		cfg.RetryAfterSeconds = 1
	}

	return &MockUpstream{
		config:       cfg,
		reqTokens:    float64(reqBurst),
		maxReqTokens: float64(reqBurst),
		tokTokens:    float64(tokBurst),
		maxTokTokens: float64(tokBurst),
		lastRefill:   time.Now(),
	}
}

// ChatCompletionRequest is a subset of the OpenAI chat completion payload.
type ChatCompletionRequest struct {
	Model     string `json:"model"`
	Messages  []any  `json:"messages"`
	Stream    bool   `json:"stream"`
	MaxTokens *int   `json:"max_tokens"`
}

// ChatCompletionChoice represents a single choice in the completion response.
type ChatCompletionChoice struct {
	Index   int `json:"index"`
	Message struct {
		Role    string `json:"role"`
		Content string `json:"content"`
	} `json:"message"`
	FinishReason string `json:"finish_reason"`
}

// ChatCompletionUsage represents token usage.
type ChatCompletionUsage struct {
	PromptTokens     int `json:"prompt_tokens"`
	CompletionTokens int `json:"completion_tokens"`
	TotalTokens      int `json:"total_tokens"`
}

// ChatCompletionResponse represents a standard OpenAI chat completion JSON response.
type ChatCompletionResponse struct {
	ID      string                 `json:"id"`
	Object  string                 `json:"object"`
	Created int64                  `json:"created"`
	Model   string                 `json:"model"`
	Choices []ChatCompletionChoice `json:"choices"`
	Usage   ChatCompletionUsage    `json:"usage"`
}

// ServeHTTP handles requests to the mock upstream.
func (m *MockUpstream) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	reqNum := m.totalRequests.Add(1)
	current := m.activeInFlight.Add(1)
	defer m.activeInFlight.Add(-1)

	// Update peak concurrency
	for {
		peak := m.peakInFlight.Load()
		if current <= peak {
			break
		}
		if m.peakInFlight.CompareAndSwap(peak, current) {
			break
		}
	}

	// Check if path is valid
	if r.URL.Path != "/v1/chat/completions" && r.URL.Path != "/chat/completions" {
		if r.URL.Path == "/healthz" {
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"status":"ok"}`))
			return
		}
		http.NotFound(w, r)
		return
	}

	// Read body to compute tokens and model
	bodyBytes, _ := io.ReadAll(r.Body)
	r.Body = io.NopCloser(bytes.NewReader(bodyBytes))

	var chatReq ChatCompletionRequest
	_ = json.Unmarshal(bodyBytes, &chatReq)
	modelName := chatReq.Model
	if modelName == "" {
		modelName = "gpt-4o-mock"
	}

	// Token usage calculation
	promptTokens := len(bodyBytes) / 4
	if promptTokens < 5 {
		promptTokens = 10
	}
	completionTokens := 12
	if chatReq.MaxTokens != nil && *chatReq.MaxTokens > 0 {
		completionTokens = *chatReq.MaxTokens
		if completionTokens > 50 {
			completionTokens = 50
		}
	}
	totalTokens := promptTokens + completionTokens

	retryAfterStr := strconv.Itoa(m.config.RetryAfterSeconds)
	if retryAfterStr == "0" {
		retryAfterStr = "1"
	}

	// 1. Check for caller-injected status code override
	if overrideCodeStr := r.Header.Get("X-Mock-Status"); overrideCodeStr != "" {
		if code, err := strconv.Atoi(overrideCodeStr); err == nil && code >= 100 && code < 600 {
			w.Header().Set("Content-Type", "application/json; charset=utf-8")
			if code == http.StatusTooManyRequests {
				w.Header().Set("Retry-After", retryAfterStr)
			}
			w.WriteHeader(code)
			_ = json.NewEncoder(w).Encode(map[string]any{
				"error": map[string]any{
					"message": fmt.Sprintf("Mock injected error status %d", code),
					"type":    "mock_error",
					"code":    code,
				},
			})
			return
		}
	}

	// 2. Refill and check token bucket rate limits (RPM and TPM)
	m.mu.Lock()
	m.refill(time.Now())

	// Check TPM limit
	if m.config.RateLimitTPS > 0 {
		neededTokens := float64(totalTokens)
		if m.tokTokens < neededTokens {
			m.mu.Unlock()
			w.Header().Set("Content-Type", "application/json; charset=utf-8")
			w.Header().Set("Retry-After", retryAfterStr)
			m.setLimitHeaders(w)
			w.Header().Set("x-ratelimit-remaining-tokens", "0")
			w.Header().Set("x-ratelimit-reset-tokens", "1s")
			w.WriteHeader(http.StatusTooManyRequests)
			_ = json.NewEncoder(w).Encode(map[string]any{
				"error": map[string]any{
					"message": "Mock upstream token rate limit exceeded (TPM bucket exhausted)",
					"type":    "rate_limit_error",
					"code":    429,
				},
			})
			return
		}
		m.tokTokens -= neededTokens
	}

	// Check RPM limit
	if m.config.RateLimitRPS > 0 {
		if m.reqTokens < 1.0 {
			m.mu.Unlock()
			w.Header().Set("Content-Type", "application/json; charset=utf-8")
			w.Header().Set("Retry-After", retryAfterStr)
			m.setLimitHeaders(w)
			w.Header().Set("x-ratelimit-remaining-requests", "0")
			w.Header().Set("x-ratelimit-reset-requests", "1s")
			w.WriteHeader(http.StatusTooManyRequests)
			_ = json.NewEncoder(w).Encode(map[string]any{
				"error": map[string]any{
					"message": "Mock upstream request rate limit reached (RPM bucket exhausted)",
					"type":    "rate_limit_error",
					"code":    429,
				},
			})
			return
		}
		m.reqTokens -= 1.0
	}

	remReq := int(m.reqTokens)
	remTok := int(m.tokTokens)
	m.mu.Unlock()

	// 3. Check for concurrency overload in mock
	if m.config.MaxAllowedInFlight > 0 && current > int64(m.config.MaxAllowedInFlight) {
		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		w.Header().Set("Retry-After", retryAfterStr)
		w.WriteHeader(http.StatusTooManyRequests)
		_ = json.NewEncoder(w).Encode(map[string]any{
			"error": map[string]any{
				"message": fmt.Sprintf("Mock upstream concurrency exceeded: %d > %d", current, m.config.MaxAllowedInFlight),
				"type":    "rate_limit_error",
				"code":    429,
			},
		})
		return
	}

	// 4. Determine latency
	latency := m.config.BaseLatency
	if delayOverride := r.Header.Get("X-Mock-Delay"); delayOverride != "" {
		if d, err := time.ParseDuration(delayOverride); err == nil {
			latency = d
		}
	}

	// Sleep for configured duration, respecting context cancellation
	if latency > 0 {
		select {
		case <-time.After(latency):
		case <-r.Context().Done():
			return
		}
	}

	// 5. Return valid OpenAI chat completion response with rate limit headers
	resp := ChatCompletionResponse{
		ID:      fmt.Sprintf("chatcmpl-mock-%d", reqNum),
		Object:  "chat.completion",
		Created: time.Now().Unix(),
		Model:   modelName,
		Choices: []ChatCompletionChoice{
			{
				Index: 0,
				Message: struct {
					Role    string `json:"role"`
					Content string `json:"content"`
				}{
					Role:    "assistant",
					Content: fmt.Sprintf("Response from mock LLM (request #%d)", reqNum),
				},
				FinishReason: "stop",
			},
		},
		Usage: ChatCompletionUsage{
			PromptTokens:     promptTokens,
			CompletionTokens: completionTokens,
			TotalTokens:      totalTokens,
		},
	}

	m.setLimitHeaders(w)
	if m.config.RateLimitRPS > 0 {
		w.Header().Set("x-ratelimit-remaining-requests", strconv.Itoa(remReq))
		w.Header().Set("x-ratelimit-reset-requests", "1s")
	}
	if m.config.RateLimitTPS > 0 {
		w.Header().Set("x-ratelimit-remaining-tokens", strconv.Itoa(remTok))
		w.Header().Set("x-ratelimit-reset-tokens", "1s")
	}

	// Streaming: emit OpenAI-style SSE chunks, a final usage chunk, then [DONE].
	if chatReq.Stream {
		w.Header().Set("Content-Type", "text/event-stream")
		w.Header().Set("Cache-Control", "no-cache")
		w.WriteHeader(http.StatusOK)
		flusher, _ := w.(http.Flusher)
		words := []string{"Response", "from", "mock", "stream"}
		for i, word := range words {
			chunk := map[string]any{
				"id": resp.ID, "object": "chat.completion.chunk", "created": resp.Created, "model": modelName,
				"choices": []map[string]any{{"index": 0, "delta": map[string]any{"content": word + " "}, "finish_reason": nil}},
			}
			if i == len(words)-1 {
				chunk["choices"] = []map[string]any{{"index": 0, "delta": map[string]any{}, "finish_reason": "stop"}}
			}
			b, _ := json.Marshal(chunk)
			_, _ = fmt.Fprintf(w, "data: %s\n\n", b)
			if flusher != nil {
				flusher.Flush()
			}
		}
		usageChunk, _ := json.Marshal(map[string]any{
			"id": resp.ID, "object": "chat.completion.chunk", "created": resp.Created, "model": modelName,
			"choices": []any{}, "usage": resp.Usage,
		})
		_, _ = fmt.Fprintf(w, "data: %s\n\ndata: [DONE]\n\n", usageChunk)
		if flusher != nil {
			flusher.Flush()
		}
		return
	}

	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(resp)
}

// setLimitHeaders mirrors what OpenAI and Anthropic send on every response: the per-minute limits.
func (m *MockUpstream) setLimitHeaders(w http.ResponseWriter) {
	if m.config.RateLimitRPS > 0 {
		w.Header().Set("x-ratelimit-limit-requests", strconv.Itoa(int(m.config.RateLimitRPS*60)))
	}
	if m.config.RateLimitTPS > 0 {
		w.Header().Set("x-ratelimit-limit-tokens", strconv.Itoa(int(m.config.RateLimitTPS*60)))
	}
}

func (m *MockUpstream) refill(now time.Time) {
	elapsed := now.Sub(m.lastRefill).Seconds()
	if elapsed <= 0 {
		return
	}
	m.lastRefill = now

	if m.config.RateLimitRPS > 0 {
		m.reqTokens += elapsed * m.config.RateLimitRPS
		if m.reqTokens > m.maxReqTokens {
			m.reqTokens = m.maxReqTokens
		}
	}

	if m.config.RateLimitTPS > 0 {
		m.tokTokens += elapsed * m.config.RateLimitTPS
		if m.tokTokens > m.maxTokTokens {
			m.tokTokens = m.maxTokTokens
		}
	}
}

// Stats returns the current execution statistics of the mock upstream.
func (m *MockUpstream) Stats() (active int64, peak int64, total int64) {
	return m.activeInFlight.Load(), m.peakInFlight.Load(), m.totalRequests.Load()
}

// ResetStats resets the mock upstream statistics.
func (m *MockUpstream) ResetStats() {
	m.activeInFlight.Store(0)
	m.peakInFlight.Store(0)
	m.totalRequests.Store(0)
	m.mu.Lock()
	m.reqTokens = m.maxReqTokens
	m.tokTokens = m.maxTokTokens
	m.lastRefill = time.Now()
	m.mu.Unlock()
}
