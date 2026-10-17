package upstream

import (
	"context"
	"net/http"
	"net/url"
	"strings"
	"testing"
)

func TestProvider_SecretRedaction(t *testing.T) {
	input := "Invalid API key: sk-proj-1234567890abcdefABCDEF provided for user"
	redacted := RedactSecrets(input)
	if strings.Contains(redacted, "sk-proj") {
		t.Errorf("API key pattern was not redacted: %s", redacted)
	}
	if !strings.Contains(redacted, "[REDACTED]") {
		t.Errorf("expected [REDACTED] in result: %s", redacted)
	}

	bearerInput := "Authorization header Bearer secret_token_value_12345 is invalid"
	redactedBearer := RedactSecrets(bearerInput)
	if strings.Contains(redactedBearer, "secret_token_value") {
		t.Errorf("Bearer token was not redacted: %s", redactedBearer)
	}
}

func TestProvider_OpenAI(t *testing.T) {
	p := NewOpenAIProvider("sk-my-test-key", "gpt-4o")
	if p.Name() != "openai" {
		t.Errorf("expected name openai, got %s", p.Name())
	}

	targetURL, _ := url.Parse("https://api.openai.com/v1")
	h := http.Header{}
	h.Set("X-Run-ID", "run-123") // Should be stripped
	h.Set("Custom-Header", "value")

	req, err := p.PrepareRequest(context.Background(), targetURL, "/chat/completions", h, []byte(`{"model":"gpt-4o"}`))
	if err != nil {
		t.Fatalf("PrepareRequest failed: %v", err)
	}

	if req.Header.Get("X-Run-ID") != "" {
		t.Errorf("X-Run-ID was not stripped")
	}
	if req.Header.Get("Authorization") != "Bearer sk-my-test-key" {
		t.Errorf("Authorization header incorrect: %s", req.Header.Get("Authorization"))
	}
	if req.Header.Get("Custom-Header") != "value" {
		t.Errorf("Custom-Header not preserved")
	}

	// Test Error Sanitization
	rawErr := []byte(`{"error":{"message":"Incorrect API key provided: sk-proj-1234567890abcdef. You can find your API key at https://platform.openai.com/account/api-keys.","type":"invalid_request_error","code":"invalid_api_key"}}`)
	safeMsg, errType := p.SanitizeError(http.StatusUnauthorized, rawErr)
	if strings.Contains(safeMsg, "sk-proj") {
		t.Errorf("API key leaked in sanitized error: %s", safeMsg)
	}
	if errType != "authentication_error" {
		t.Errorf("expected authentication_error, got %s", errType)
	}
}

func TestProvider_Anthropic(t *testing.T) {
	p := NewAnthropicProvider("ant-key-12345678", "claude-3-opus")
	if p.Name() != "anthropic" {
		t.Errorf("expected name anthropic, got %s", p.Name())
	}

	targetURL, _ := url.Parse("https://api.anthropic.com")
	req, err := p.PrepareRequest(context.Background(), targetURL, "/v1/messages", http.Header{}, []byte(`{}`))
	if err != nil {
		t.Fatalf("PrepareRequest failed: %v", err)
	}

	if req.Header.Get("x-api-key") != "ant-key-12345678" {
		t.Errorf("expected x-api-key header, got %s", req.Header.Get("x-api-key"))
	}
	if req.Header.Get("anthropic-version") != "2023-06-01" {
		t.Errorf("expected anthropic-version 2023-06-01, got %s", req.Header.Get("anthropic-version"))
	}

	// Error sanitization
	rawErr := []byte(`{"type":"error","error":{"type":"authentication_error","message":"Invalid x-api-key: ant-key-12345678"}}`)
	safeMsg, errType := p.SanitizeError(http.StatusUnauthorized, rawErr)
	if strings.Contains(safeMsg, "ant-key") {
		t.Errorf("API key leaked in sanitized error: %s", safeMsg)
	}
	if errType != "authentication_error" {
		t.Errorf("expected authentication_error, got %s", errType)
	}
}

func TestProvider_Azure(t *testing.T) {
	p := NewAzureOpenAIProvider("azure-secret-key-1234", "")
	if p.Name() != "azure" {
		t.Errorf("expected name azure, got %s", p.Name())
	}

	targetURL, _ := url.Parse("https://my-resource.openai.azure.com")
	req, err := p.PrepareRequest(context.Background(), targetURL, "/openai/deployments/test/chat/completions", http.Header{}, []byte(`{}`))
	if err != nil {
		t.Fatalf("PrepareRequest failed: %v", err)
	}

	if req.Header.Get("api-key") != "azure-secret-key-1234" {
		t.Errorf("expected api-key header, got %s", req.Header.Get("api-key"))
	}
}

func TestProvider_Generic(t *testing.T) {
	p := NewGenericProvider("custom-token", "X-Custom-Auth", "Token ", "")
	if p.Name() != "generic" {
		t.Errorf("expected name generic, got %s", p.Name())
	}

	targetURL, _ := url.Parse("http://localhost:8000")
	req, err := p.PrepareRequest(context.Background(), targetURL, "/v1/chat/completions", http.Header{}, []byte(`{}`))
	if err != nil {
		t.Fatalf("PrepareRequest failed: %v", err)
	}

	if req.Header.Get("X-Custom-Auth") != "Token custom-token" {
		t.Errorf("expected Token custom-token, got %s", req.Header.Get("X-Custom-Auth"))
	}
}
