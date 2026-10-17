package estimation

import (
	"testing"
)

func TestHeuristicEstimator_Estimate(t *testing.T) {
	est := NewHeuristicEstimator(DefaultConfig())

	// 1. Empty body
	res := est.Estimate(nil)
	if res.PromptTokens != 1 || res.CompletionTokens != 64 || res.TotalTokens != 65 {
		t.Fatalf("unexpected empty body estimation: %+v", res)
	}

	// 2. Standard OpenAI JSON with string content
	body := []byte(`{
		"model": "gpt-4o",
		"messages": [
			{"role": "system", "content": "You are a helpful coding assistant."},
			{"role": "user", "content": "Write a quicksort in Go."}
		],
		"max_tokens": 150
	}`)

	res = est.Estimate(body)
	if res.PromptTokens <= 0 {
		t.Fatalf("expected positive prompt tokens, got %d", res.PromptTokens)
	}
	if res.CompletionTokens != 150 {
		t.Fatalf("expected 150 completion tokens from max_tokens, got %d", res.CompletionTokens)
	}
	if res.TotalTokens != res.PromptTokens+150 {
		t.Fatalf("total tokens mismatch: %+v", res)
	}

	// 3. Multimodal content array
	bodyMultimodal := []byte(`{
		"model": "gpt-4o",
		"messages": [
			{
				"role": "user",
				"content": [
					{"type": "text", "text": "Describe this code snippet:"},
					{"type": "text", "text": "package main; func main() {}"}
				]
			}
		]
	}`)

	resMulti := est.Estimate(bodyMultimodal)
	if resMulti.PromptTokens <= 0 {
		t.Fatalf("expected positive prompt tokens for multimodal, got %d", resMulti.PromptTokens)
	}
	if resMulti.CompletionTokens != 64 {
		t.Fatalf("expected default 64 completion tokens, got %d", resMulti.CompletionTokens)
	}
}
