package upstream

import "encoding/json"

// ParseUsageTokens extracts the total tokens consumed from a provider response body.
// It understands the OpenAI shape (usage.total_tokens, or prompt_tokens+completion_tokens)
// and the Anthropic Messages shape (usage.input_tokens+output_tokens). Returns 0 if unknown.
func ParseUsageTokens(body []byte) int {
	if len(body) == 0 {
		return 0
	}
	var resp struct {
		Usage struct {
			TotalTokens      int `json:"total_tokens"`
			PromptTokens     int `json:"prompt_tokens"`
			CompletionTokens int `json:"completion_tokens"`
			InputTokens      int `json:"input_tokens"`
			OutputTokens     int `json:"output_tokens"`
		} `json:"usage"`
	}
	if err := json.Unmarshal(body, &resp); err != nil {
		return 0
	}
	u := resp.Usage
	switch {
	case u.TotalTokens > 0:
		return u.TotalTokens
	case u.PromptTokens+u.CompletionTokens > 0:
		return u.PromptTokens + u.CompletionTokens
	case u.InputTokens+u.OutputTokens > 0:
		return u.InputTokens + u.OutputTokens
	}
	return 0
}
