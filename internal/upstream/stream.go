package upstream

import (
	"bytes"
	"encoding/json"
)

// IsStreamingRequest reports whether the request body asks for a streamed response.
func IsStreamingRequest(body []byte) bool {
	var p struct {
		Stream bool `json:"stream"`
	}
	return json.Unmarshal(body, &p) == nil && p.Stream
}

// StreamUsage accumulates token usage reported inside a server-sent-events stream.
//
// OpenAI: a final chunk carries "usage" (when stream_options.include_usage is set).
// Anthropic: message_start carries message.usage.input_tokens; message_delta events
// carry a cumulative usage.output_tokens.
type StreamUsage struct {
	input, output, total int
}

// Observe inspects one SSE "data:" payload and records any usage it contains.
func (u *StreamUsage) Observe(data []byte) {
	data = bytes.TrimSpace(data)
	if len(data) == 0 || bytes.Equal(data, []byte("[DONE]")) {
		return
	}
	var ev struct {
		Usage *struct {
			TotalTokens      int `json:"total_tokens"`
			PromptTokens     int `json:"prompt_tokens"`
			CompletionTokens int `json:"completion_tokens"`
			InputTokens      int `json:"input_tokens"`
			OutputTokens     int `json:"output_tokens"`
		} `json:"usage"`
		Message *struct {
			Usage struct {
				InputTokens  int `json:"input_tokens"`
				OutputTokens int `json:"output_tokens"`
			} `json:"usage"`
		} `json:"message"`
	}
	if json.Unmarshal(data, &ev) != nil {
		return
	}
	if ev.Message != nil {
		if ev.Message.Usage.InputTokens > u.input {
			u.input = ev.Message.Usage.InputTokens
		}
		if ev.Message.Usage.OutputTokens > u.output {
			u.output = ev.Message.Usage.OutputTokens
		}
	}
	if ev.Usage != nil {
		if ev.Usage.TotalTokens > u.total {
			u.total = ev.Usage.TotalTokens
		}
		if s := ev.Usage.PromptTokens + ev.Usage.CompletionTokens; s > u.total {
			u.total = s
		}
		if ev.Usage.InputTokens > u.input {
			u.input = ev.Usage.InputTokens
		}
		if ev.Usage.OutputTokens > u.output {
			u.output = ev.Usage.OutputTokens
		}
	}
}

// Total returns the best estimate of tokens consumed, or 0 if the stream reported none.
func (u *StreamUsage) Total() int {
	if u.total > 0 {
		return u.total
	}
	return u.input + u.output
}
