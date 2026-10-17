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
}

// DefaultConfig returns safe defaults for heuristic token estimation.
func DefaultConfig() Config {
	return Config{
		DefaultCompletionTokens: 64,
		CharsPerToken:           4.0,
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
	return &HeuristicEstimator{cfg: cfg}
}

type incomingChatPayload struct {
	Model    string `json:"model"`
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
		// Bound conservative reservation to prevent single request monopolizing huge bucket
		if completionTokens > 2048 {
			completionTokens = 2048
		}
	}

	return TokenEstimate{
		PromptTokens:     promptTokens,
		CompletionTokens: completionTokens,
		TotalTokens:      promptTokens + completionTokens,
	}
}
