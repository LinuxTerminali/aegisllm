package upstream

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"
)

// TestRealProvider_Smoke validates compatibility with a real live LLM provider.
// This test is OPT-IN: it skips cleanly unless PROVIDER_TEST_API_KEY or OPENAI_API_KEY is set.
// It sends minimal tokens (max_tokens: 5) to minimize cost and execution time.
func TestRealProvider_Smoke(t *testing.T) {
	apiKey := os.Getenv("PROVIDER_TEST_API_KEY")
	if apiKey == "" {
		apiKey = os.Getenv("OPENAI_API_KEY")
	}

	if apiKey == "" {
		t.Skip("skipping real-provider smoke test: PROVIDER_TEST_API_KEY or OPENAI_API_KEY not set")
	}

	baseURL := os.Getenv("PROVIDER_TEST_BASE_URL")
	if baseURL == "" {
		baseURL = "https://api.openai.com/v1"
	}

	modelName := os.Getenv("PROVIDER_TEST_MODEL")
	if modelName == "" {
		modelName = "gpt-4o-mini"
	}

	client, err := NewClient(ClientConfig{
		BaseURL:      baseURL,
		APIKey:       apiKey,
		ProviderType: "openai",
		Timeout:      30 * time.Second,
	})
	if err != nil {
		t.Fatalf("failed to initialize real provider client: %v", err)
	}

	t.Run("SuccessCompletion", func(t *testing.T) {
		reqBody := []byte(`{"model":"` + modelName + `","messages":[{"role":"user","content":"Say 1"}],"max_tokens":3}`)

		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()

		start := time.Now()
		resp, err := client.Forward(ctx, "/chat/completions", http.Header{}, reqBody)
		latency := time.Since(start)

		if err != nil {
			t.Fatalf("upstream forward failed: %v", err)
		}
		defer resp.Body.Close()

		respBytes, err := io.ReadAll(io.LimitReader(resp.Body, 1024*1024))
		if err != nil {
			t.Fatalf("failed to read response: %v", err)
		}

		if resp.StatusCode != http.StatusOK {
			safeMsg, errType := client.SanitizeError(resp.StatusCode, respBytes)
			t.Fatalf("upstream returned unexpected status %d (type: %s, msg: %s)", resp.StatusCode, errType, safeMsg)
		}

		var chatResp ChatCompletionResponse
		if err := json.Unmarshal(respBytes, &chatResp); err != nil {
			t.Fatalf("failed to parse response JSON: %v", err)
		}

		if len(chatResp.Choices) == 0 {
			t.Errorf("expected at least 1 completion choice, got 0")
		}

		quota := client.Provider().ParseQuota(resp.Header)

		// Structured, safe telemetry log: NEVER logs API key or prompt!
		t.Logf("[REAL_PROVIDER_SMOKE] success=true status=%d latency_ms=%d total_tokens=%d req_quota_known=%v tok_quota_known=%v",
			resp.StatusCode, latency.Milliseconds(), chatResp.Usage.TotalTokens, quota.HasRequestsQuota, quota.HasTokensQuota)

		if chatResp.Usage.TotalTokens <= 0 {
			t.Errorf("expected usage.total_tokens > 0, got %d", chatResp.Usage.TotalTokens)
		}
	})

	t.Run("ErrorPropagation", func(t *testing.T) {
		// Use an intentionally invalid model name to test safe error propagation
		badReqBody := []byte(`{"model":"non-existent-adversarial-model-404","messages":[{"role":"user","content":"hi"}]}`)

		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()

		resp, err := client.Forward(ctx, "/chat/completions", http.Header{}, badReqBody)
		if err != nil {
			t.Fatalf("unexpected transport error: %v", err)
		}
		defer resp.Body.Close()

		respBytes, _ := io.ReadAll(io.LimitReader(resp.Body, 1024*1024))

		if resp.StatusCode == http.StatusOK {
			t.Errorf("expected non-200 status for invalid model, got 200")
		}

		safeMsg, errType := client.SanitizeError(resp.StatusCode, respBytes)
		if safeMsg == "" {
			t.Errorf("expected non-empty safe error message")
		}
		if strings.Contains(safeMsg, apiKey) {
			t.Errorf("CRITICAL: API key was found in sanitized error message!")
		}

		t.Logf("[REAL_PROVIDER_SMOKE] error_propagation status=%d err_type=%s safe_msg=%s", resp.StatusCode, errType, safeMsg)
	})

	t.Run("Cancellation", func(t *testing.T) {
		reqBody := []byte(`{"model":"` + modelName + `","messages":[{"role":"user","content":"hi"}]}`)

		ctx, cancel := context.WithCancel(context.Background())
		cancel() // Cancel immediately

		_, err := client.Forward(ctx, "/chat/completions", http.Header{}, reqBody)
		if err == nil {
			t.Errorf("expected cancellation error, got nil")
		}

		t.Logf("[REAL_PROVIDER_SMOKE] cancellation_handled=true err=%v", err)
	})
}
