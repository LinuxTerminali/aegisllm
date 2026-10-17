package upstream

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"regexp"
	"strings"
)

// Provider defines the interface for an upstream LLM provider adapter.
type Provider interface {
	// Name returns the provider identifier (e.g. "openai", "anthropic", "azure", "generic").
	Name() string
	// PrepareRequest creates and decorates the upstream HTTP request for forwarding.
	PrepareRequest(ctx context.Context, targetURL *url.URL, path string, incomingHeader http.Header, body []byte) (*http.Request, error)
	// ParseQuota extracts rate limit quota hints defensively from response headers.
	ParseQuota(h http.Header) ProviderQuota
	// SanitizeError returns safe error message and error type for client responses,
	// ensuring no auth keys, credentials, or sensitive headers are leaked.
	SanitizeError(statusCode int, body []byte) (safeMessage string, errType string)
}

// Regex to detect and redact API keys or authorization tokens (e.g., sk-..., Bearer ...)
var (
	apiKeyPattern = regexp.MustCompile(`(?i)(sk-[a-zA-Z0-9_\-]{8,}|bearer\s+[a-zA-Z0-9_\-\.]{8,}|key-[a-zA-Z0-9_\-]{8,})`)
)

// RedactSecrets replaces sensitive patterns in error messages.
func RedactSecrets(s string) string {
	return apiKeyPattern.ReplaceAllString(s, "[REDACTED]")
}

// BaseProvider implements common request preparation and header sanitization.
type BaseProvider struct {
	name             string
	apiKey           string
	authHeaderName   string
	authHeaderPrefix string
	defaultModel     string
}

// NewBaseProvider creates a BaseProvider with configured authentication headers.
func NewBaseProvider(name, apiKey, headerName, headerPrefix, defaultModel string) *BaseProvider {
	if headerName == "" {
		headerName = "Authorization"
	}
	if headerPrefix == "" && headerName == "Authorization" {
		headerPrefix = "Bearer "
	}
	return &BaseProvider{
		name:             name,
		apiKey:           apiKey,
		authHeaderName:   headerName,
		authHeaderPrefix: headerPrefix,
		defaultModel:     defaultModel,
	}
}

func (p *BaseProvider) Name() string {
	return p.name
}

// PrepareRequest builds an http.Request stripping hop-by-hop & internal scheduler headers.
func (p *BaseProvider) PrepareRequest(ctx context.Context, targetURL *url.URL, path string, incomingHeader http.Header, body []byte) (*http.Request, error) {
	u := *targetURL
	u.Path = strings.TrimRight(u.Path, "/") + path

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, u.String(), bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("failed to create upstream request: %w", err)
	}

	// Copy allowed incoming headers
	for k, vv := range incomingHeader {
		lower := strings.ToLower(k)
		// Skip internal scheduling headers and hop-by-hop headers
		if strings.HasPrefix(lower, "x-run-id") ||
			strings.HasPrefix(lower, "x-workload-class") ||
			strings.HasPrefix(lower, "x-priority") ||
			strings.HasPrefix(lower, "x-scheduler-") ||
			lower == "host" ||
			lower == "connection" ||
			lower == "content-length" {
			continue
		}
		for _, val := range vv {
			req.Header.Add(k, val)
		}
	}

	req.Header.Set("Content-Type", "application/json")

	// Inject authentication header if configured and not already set by caller
	if p.apiKey != "" && req.Header.Get(p.authHeaderName) == "" {
		req.Header.Set(p.authHeaderName, p.authHeaderPrefix+p.apiKey)
	}

	return req, nil
}

func (p *BaseProvider) ParseQuota(h http.Header) ProviderQuota {
	return ParseProviderQuota(h)
}

func (p *BaseProvider) SanitizeError(statusCode int, body []byte) (string, string) {
	var errResp struct {
		Error struct {
			Message string `json:"message"`
			Type    string `json:"type"`
			Code    any    `json:"code"`
		} `json:"error"`
	}

	if len(body) > 0 && json.Unmarshal(body, &errResp) == nil && errResp.Error.Message != "" {
		msg := RedactSecrets(errResp.Error.Message)
		// On 401/403, never echo detailed credential errors
		if statusCode == http.StatusUnauthorized {
			return "Upstream provider authentication failed", "authentication_error"
		}
		if statusCode == http.StatusForbidden {
			return "Upstream provider access forbidden", "permission_error"
		}
		errType := errResp.Error.Type
		if errType == "" {
			errType = "upstream_error"
		}
		return msg, errType
	}

	// Fallback based on HTTP status code
	switch statusCode {
	case http.StatusBadRequest:
		return "Invalid request sent to upstream provider", "invalid_request_error"
	case http.StatusUnauthorized:
		return "Upstream provider authentication failed", "authentication_error"
	case http.StatusForbidden:
		return "Upstream provider access forbidden", "permission_error"
	case http.StatusNotFound:
		return "Upstream resource or model not found", "not_found_error"
	case http.StatusTooManyRequests:
		return "Upstream provider rate limit exceeded", "rate_limit_exceeded"
	case http.StatusBadGateway:
		return "Upstream provider bad gateway", "bad_gateway"
	case http.StatusServiceUnavailable:
		return "Upstream provider service unavailable", "service_unavailable"
	case http.StatusGatewayTimeout:
		return "Upstream provider gateway timeout", "gateway_timeout"
	default:
		return fmt.Sprintf("Upstream provider returned HTTP status %d", statusCode), "upstream_error"
	}
}

// OpenAIProvider handles OpenAI API conventions.
type OpenAIProvider struct {
	*BaseProvider
}

// NewOpenAIProvider creates an adapter for OpenAI.
func NewOpenAIProvider(apiKey string, defaultModel string) *OpenAIProvider {
	return &OpenAIProvider{
		BaseProvider: NewBaseProvider("openai", apiKey, "Authorization", "Bearer ", defaultModel),
	}
}

// AnthropicProvider handles Anthropic API conventions.
type AnthropicProvider struct {
	*BaseProvider
}

// NewAnthropicProvider creates an adapter for Anthropic.
func NewAnthropicProvider(apiKey string, defaultModel string) *AnthropicProvider {
	return &AnthropicProvider{
		BaseProvider: NewBaseProvider("anthropic", apiKey, "x-api-key", "", defaultModel),
	}
}

func (p *AnthropicProvider) PrepareRequest(ctx context.Context, targetURL *url.URL, path string, incomingHeader http.Header, body []byte) (*http.Request, error) {
	req, err := p.BaseProvider.PrepareRequest(ctx, targetURL, path, incomingHeader, body)
	if err != nil {
		return nil, err
	}
	if req.Header.Get("anthropic-version") == "" {
		req.Header.Set("anthropic-version", "2023-06-01")
	}
	return req, nil
}

func (p *AnthropicProvider) SanitizeError(statusCode int, body []byte) (string, string) {
	var errResp struct {
		Type  string `json:"type"`
		Error struct {
			Type    string `json:"type"`
			Message string `json:"message"`
		} `json:"error"`
	}

	if len(body) > 0 && json.Unmarshal(body, &errResp) == nil && errResp.Error.Message != "" {
		if statusCode == http.StatusUnauthorized {
			return "Upstream Anthropic authentication failed", "authentication_error"
		}
		if statusCode == http.StatusForbidden {
			return "Upstream Anthropic access forbidden", "permission_error"
		}
		return RedactSecrets(errResp.Error.Message), errResp.Error.Type
	}

	return p.BaseProvider.SanitizeError(statusCode, body)
}

// AzureOpenAIProvider handles Azure OpenAI API conventions.
type AzureOpenAIProvider struct {
	*BaseProvider
}

// NewAzureOpenAIProvider creates an adapter for Azure OpenAI.
func NewAzureOpenAIProvider(apiKey string, defaultModel string) *AzureOpenAIProvider {
	return &AzureOpenAIProvider{
		BaseProvider: NewBaseProvider("azure", apiKey, "api-key", "", defaultModel),
	}
}

// GenericProvider handles generic OpenAI-compatible providers (vLLM, Ollama, Groq, OpenRouter, Together).
type GenericProvider struct {
	*BaseProvider
}

// NewGenericProvider creates an adapter for generic OpenAI-compatible backends.
func NewGenericProvider(apiKey, headerName, headerPrefix, defaultModel string) *GenericProvider {
	return &GenericProvider{
		BaseProvider: NewBaseProvider("generic", apiKey, headerName, headerPrefix, defaultModel),
	}
}

// ResolveProvider selects the appropriate provider adapter given type and configuration.
func ResolveProvider(providerType, apiKey, authHeader, authPrefix, defaultModel string) Provider {
	switch strings.ToLower(strings.TrimSpace(providerType)) {
	case "anthropic":
		return NewAnthropicProvider(apiKey, defaultModel)
	case "azure", "azure-openai":
		return NewAzureOpenAIProvider(apiKey, defaultModel)
	case "generic", "openai-compatible", "local":
		return NewGenericProvider(apiKey, authHeader, authPrefix, defaultModel)
	case "openai", "":
		fallthrough
	default:
		return NewOpenAIProvider(apiKey, defaultModel)
	}
}
