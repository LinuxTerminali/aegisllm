package upstream

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

// MockConfig holds configuration for the mock upstream server.
type MockConfig struct {
	BaseLatency        time.Duration
	MaxAllowedInFlight int // Returns 429 if active concurrency exceeds this (0 = unlimited)
}

// MockUpstream simulates an OpenAI-compatible upstream LLM provider.
type MockUpstream struct {
	config          MockConfig
	activeInFlight  atomic.Int64
	peakInFlight    atomic.Int64
	totalRequests   atomic.Int64
	statusCodesSeen sync.Map
}

// NewMockUpstream creates a new MockUpstream instance.
func NewMockUpstream(cfg MockConfig) *MockUpstream {
	return &MockUpstream{
		config: cfg,
	}
}

// ChatCompletionRequest is a subset of the OpenAI chat completion payload.
type ChatCompletionRequest struct {
	Model    string `json:"model"`
	Messages []any  `json:"messages"`
	Stream   bool   `json:"stream"`
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

	// 1. Check for caller-injected status code override
	if overrideCodeStr := r.Header.Get("X-Mock-Status"); overrideCodeStr != "" {
		if code, err := strconv.Atoi(overrideCodeStr); err == nil && code >= 100 && code < 600 {
			w.Header().Set("Content-Type", "application/json; charset=utf-8")
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

	// 2. Check for concurrency overload in mock
	if m.config.MaxAllowedInFlight > 0 && current > int64(m.config.MaxAllowedInFlight) {
		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		w.Header().Set("Retry-After", "1")
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

	// 3. Determine latency
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

	// Parse model name from body if present
	var chatReq ChatCompletionRequest
	_ = json.NewDecoder(r.Body).Decode(&chatReq)
	modelName := chatReq.Model
	if modelName == "" {
		modelName = "gpt-4o-mock"
	}

	// 4. Return valid OpenAI chat completion response
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
			PromptTokens:     10,
			CompletionTokens: 12,
			TotalTokens:      22,
		},
	}

	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(resp)
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
}
