package estimation

import (
	"encoding/json"
)

// TokenEstimate holds the estimated prompt, completion, and total tokens for a request.
type TokenEstimate struct {
	PromptTokens     int `json:"prompt_tokens"`
	CompletionTokens int `json:"completion_tokens"`
	TotalTokens      int `json:"total_tokens"`
}

// Estimator estimates token counts from raw request bodies.
type Estimator interface {
	Estimate(body []byte) TokenEstimate
}

// Config configures the HeuristicEstimator.
type Config struct {
	DefaultCompletionTokens int
	CharsPerToken           float64
	// MaxCompletionReserve caps how many completion tokens a single request may reserve
	// up front from max_tokens. Agent calls commonly set 8k-32k; under-reserving is how
	// the shared quota still gets blown. The capacity tracker further clamps to its burst size.
	MaxCompletionReserve int
}

// DefaultConfig returns safe defaults for heuristic token estimation.
func DefaultConfig() Config {
	return Config{
		DefaultCompletionTokens: 64,
		CharsPerToken:           4.0,
		MaxCompletionReserve:    32768,
	}
}

// HeuristicEstimator implements token estimation based on character counts and payload parsing.
type HeuristicEstimator struct {
	cfg Config
}

// NewHeuristicEstimator creates an Estimator with the given configuration.
func NewHeuristicEstimator(cfg Config) *HeuristicEstimator {
	if cfg.DefaultCompletionTokens <= 0 {
		cfg.DefaultCompletionTokens = 64
	}
	if cfg.CharsPerToken <= 0 {
		cfg.CharsPerToken = 4.0
	}
	if cfg.MaxCompletionReserve <= 0 {
		cfg.MaxCompletionReserve = 32768
	}
	return &HeuristicEstimator{cfg: cfg}
}

type incomingChatPayload struct {
	Model    string `json:"model"`
	System   any    `json:"system"` // Anthropic Messages API: string or array of text blocks
	Messages []struct {
		Role    string `json:"role"`
		Content any    `json:"content"` // Can be string or array of multimodal parts
	} `json:"messages"`
	MaxTokens *int `json:"max_tokens"`
}

// Estimate analyzes the raw JSON body and returns estimated token consumption.
func (e *HeuristicEstimator) Estimate(body []byte) TokenEstimate {
	if len(body) == 0 {
		return TokenEstimate{
			PromptTokens:     1,
			CompletionTokens: e.cfg.DefaultCompletionTokens,
			TotalTokens:      1 + e.cfg.DefaultCompletionTokens,
		}
	}

	var payload incomingChatPayload
	err := json.Unmarshal(body, &payload)
	if err != nil || len(payload.Messages) == 0 {
		// Fallback to raw byte length heuristic
		promptTokens := int(float64(len(body)) / e.cfg.CharsPerToken)
		if promptTokens < 1 {
			promptTokens = 1
		}
		completionTokens := e.cfg.DefaultCompletionTokens
		return TokenEstimate{
			PromptTokens:     promptTokens,
			CompletionTokens: completionTokens,
			TotalTokens:      promptTokens + completionTokens,
		}
	}

	totalChars := 0
	switch sys := payload.System.(type) {
	case string:
		totalChars += len(sys)
	case []any:
		for _, part := range sys {
			if m, ok := part.(map[string]any); ok {
				if text, hasText := m["text"].(string); hasText {
					totalChars += len(text)
				}
			}
		}
	}
	for _, msg := range payload.Messages {
		totalChars += len(msg.Role)
		switch c := msg.Content.(type) {
		case string:
			totalChars += len(c)
		case []any:
			// Array of content blocks
			for _, part := range c {
				if m, ok := part.(map[string]any); ok {
					if text, hasText := m["text"].(string); hasText {
						totalChars += len(text)
					}
				}
			}
		}
		// Overhead per message formatting (role, brackets, colons)
		totalChars += 12
	}

	promptTokens := int(float64(totalChars) / e.cfg.CharsPerToken)
	if promptTokens < 1 {
		promptTokens = 1
	}

	completionTokens := e.cfg.DefaultCompletionTokens
	if payload.MaxTokens != nil && *payload.MaxTokens > 0 {
		completionTokens = *payload.MaxTokens
		// Bound reservation to prevent a single request monopolizing the bucket
		if completionTokens > e.cfg.MaxCompletionReserve {
			completionTokens = e.cfg.MaxCompletionReserve
		}
	}

	return TokenEstimate{
		PromptTokens:     promptTokens,
		CompletionTokens: completionTokens,
		TotalTokens:      promptTokens + completionTokens,
	}
}
