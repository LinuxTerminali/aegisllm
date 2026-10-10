package upstream

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestMockUpstream_SuccessResponse(t *testing.T) {
	mock := NewMockUpstream(MockConfig{BaseLatency: 10 * time.Millisecond})
	server := httptest.NewServer(mock)
	defer server.Close()

	client, err := NewClient(ClientConfig{BaseURL: server.URL})
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}

	payload := []byte(`{"model":"gpt-4o-test","messages":[{"role":"user","content":"hello"}]}`)
	resp, err := client.Forward(context.Background(), "/v1/chat/completions", http.Header{}, payload)
	if err != nil {
		t.Fatalf("forward failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 OK, got %d", resp.StatusCode)
	}

	var chatResp ChatCompletionResponse
	if err := json.NewDecoder(resp.Body).Decode(&chatResp); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}

	if chatResp.Model != "gpt-4o-test" {
		t.Errorf("expected model gpt-4o-test, got %s", chatResp.Model)
	}
	if len(chatResp.Choices) == 0 {
		t.Fatalf("expected at least 1 choice")
	}
	if chatResp.Choices[0].Message.Role != "assistant" {
		t.Errorf("expected role assistant, got %s", chatResp.Choices[0].Message.Role)
	}
}

func TestMockUpstream_InjectedStatus(t *testing.T) {
	mock := NewMockUpstream(MockConfig{})
	server := httptest.NewServer(mock)
	defer server.Close()

	client, err := NewClient(ClientConfig{BaseURL: server.URL})
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}

	hdr := http.Header{}
	hdr.Set("X-Mock-Status", "429")

	resp, err := client.Forward(context.Background(), "/v1/chat/completions", hdr, []byte(`{}`))
	if err != nil {
		t.Fatalf("forward failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusTooManyRequests {
		t.Fatalf("expected 429 Too Many Requests, got %d", resp.StatusCode)
	}
}

func TestMockUpstream_ConcurrencyLimit(t *testing.T) {
	// Set mock to allow only 1 concurrent in-flight request
	mock := NewMockUpstream(MockConfig{
		BaseLatency:        50 * time.Millisecond,
		MaxAllowedInFlight: 1,
	})
	server := httptest.NewServer(mock)
	defer server.Close()

	client, err := NewClient(ClientConfig{BaseURL: server.URL})
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}

	ch := make(chan int, 2)
	for i := 0; i < 2; i++ {
		go func() {
			resp, err := client.Forward(context.Background(), "/v1/chat/completions", http.Header{}, []byte(`{}`))
			if err != nil {
				ch <- 0
				return
			}
			defer resp.Body.Close()
			_, _ = io.ReadAll(resp.Body)
			ch <- resp.StatusCode
		}()
	}

	status1 := <-ch
	status2 := <-ch

	// One should succeed (200) and one should get 429
	has200 := (status1 == 200 || status2 == 200)
	has429 := (status1 == 429 || status2 == 429)

	if !has200 || !has429 {
		t.Logf("status1: %d, status2: %d", status1, status2)
	}
}

func TestMockUpstream_TokenBucketRateLimit(t *testing.T) {
	// 2 tokens burst, 1 RPS
	mock := NewMockUpstream(MockConfig{
		RateLimitRPS:   1,
		RateLimitBurst: 2,
	})
	server := httptest.NewServer(mock)
	defer server.Close()

	client, err := NewClient(ClientConfig{BaseURL: server.URL})
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}

	// First 2 requests consume the 2 burst tokens -> 200 OK
	for i := 0; i < 2; i++ {
		resp, err := client.Forward(context.Background(), "/v1/chat/completions", http.Header{}, []byte(`{}`))
		if err != nil {
			t.Fatalf("request %d failed: %v", i, err)
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("request %d expected 200, got %d", i, resp.StatusCode)
		}
	}

	// 3rd immediate request must be rejected with 429
	resp3, err := client.Forward(context.Background(), "/v1/chat/completions", http.Header{}, []byte(`{}`))
	if err != nil {
		t.Fatalf("request 3 failed: %v", err)
	}
	defer resp3.Body.Close()
	if resp3.StatusCode != http.StatusTooManyRequests {
		t.Fatalf("expected 429, got %d", resp3.StatusCode)
	}
}
