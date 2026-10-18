package upstream

import "testing"

func TestIsStreamingRequest(t *testing.T) {
	if !IsStreamingRequest([]byte(`{"model":"x","stream":true}`)) {
		t.Fatal("expected stream=true detected")
	}
	if IsStreamingRequest([]byte(`{"model":"x"}`)) || IsStreamingRequest([]byte(`garbage`)) {
		t.Fatal("expected non-stream")
	}
}

func TestStreamUsage_OpenAIAndAnthropic(t *testing.T) {
	var o StreamUsage
	o.Observe([]byte(`{"id":"c","choices":[{"delta":{"content":"hi"}}]}`))
	o.Observe([]byte(`{"id":"c","choices":[],"usage":{"prompt_tokens":10,"completion_tokens":4,"total_tokens":14}}`))
	o.Observe([]byte(`[DONE]`))
	if o.Total() != 14 {
		t.Fatalf("openai total want 14 got %d", o.Total())
	}

	var a StreamUsage
	a.Observe([]byte(`{"type":"message_start","message":{"id":"m","usage":{"input_tokens":120,"output_tokens":1}}}`))
	a.Observe([]byte(`{"type":"content_block_delta","delta":{"type":"text_delta","text":"hi"}}`))
	a.Observe([]byte(`{"type":"message_delta","delta":{"stop_reason":"end_turn"},"usage":{"output_tokens":30}}`))
	if a.Total() != 150 {
		t.Fatalf("anthropic total want 150 got %d", a.Total())
	}

	var n StreamUsage
	n.Observe([]byte(`{"id":"c","choices":[{"delta":{"content":"hi"}}]}`))
	if n.Total() != 0 {
		t.Fatalf("no usage should give 0, got %d", n.Total())
	}
}
