package upstream

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// ClientConfig holds configuration for the upstream client.
type ClientConfig struct {
	BaseURL          string
	APIKey           string
	ProviderType     string // "openai" (default), "anthropic", "azure", "generic"
	AuthHeaderName   string
	AuthHeaderPrefix string
	DefaultModel     string
	HTTPClient       *http.Client
	Timeout          time.Duration
	Provider         Provider
}

// Client forwards requests to an upstream LLM provider using a provider adapter.
type Client struct {
	baseURL    *url.URL
	apiKey     string
	provider   Provider
	httpClient *http.Client
}

// NewClient creates a new upstream Client with the resolved provider adapter.
func NewClient(cfg ClientConfig) (*Client, error) {
	parsedURL, err := url.Parse(strings.TrimRight(cfg.BaseURL, "/"))
	if err != nil {
		return nil, fmt.Errorf("invalid upstream base URL: %w", err)
	}

	httpClient := cfg.HTTPClient
	if httpClient == nil {
		timeout := cfg.Timeout
		if timeout <= 0 {
			timeout = 60 * time.Second
		}
		// No http.Client.Timeout: it would cap the whole response, cutting off any stream
		// longer than it. Instead bound connect + time-to-first-byte here, and let the
		// request context bound the rest (the proxy cancels when the client goes away).
		transport := http.DefaultTransport.(*http.Transport).Clone()
		transport.ResponseHeaderTimeout = timeout
		transport.MaxIdleConns = 256
		transport.MaxIdleConnsPerHost = 128
		transport.IdleConnTimeout = 90 * time.Second
		httpClient = &http.Client{Transport: transport}
	}

	prov := cfg.Provider
	if prov == nil {
		prov = ResolveProvider(cfg.ProviderType, cfg.APIKey, cfg.AuthHeaderName, cfg.AuthHeaderPrefix, cfg.DefaultModel)
	}

	return &Client{
		baseURL:    parsedURL,
		apiKey:     cfg.APIKey,
		provider:   prov,
		httpClient: httpClient,
	}, nil
}

// Provider returns the configured provider adapter.
func (c *Client) Provider() Provider {
	return c.provider
}

// SanitizeError delegates error sanitization to the provider adapter.
func (c *Client) SanitizeError(statusCode int, body []byte) (string, string) {
	if c.provider != nil {
		return c.provider.SanitizeError(statusCode, body)
	}
	return "Upstream provider error", "upstream_error"
}

// Forward sends the incoming request to the upstream LLM provider via the adapter.
func (c *Client) Forward(ctx context.Context, path string, incomingHeader http.Header, body []byte) (*http.Response, error) {
	req, err := c.provider.PrepareRequest(ctx, c.baseURL, path, incomingHeader, body)
	if err != nil {
		return nil, err
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, err
	}

	return resp, nil
}
