package upstream

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// ClientConfig holds configuration for the upstream client.
type ClientConfig struct {
	BaseURL    string
	APIKey     string
	HTTPClient *http.Client
	Timeout    time.Duration
}

// Client forwards requests to an upstream LLM provider.
type Client struct {
	baseURL    *url.URL
	apiKey     string
	httpClient *http.Client
}

// NewClient creates a new upstream Client.
func NewClient(cfg ClientConfig) (*Client, error) {
	parsedURL, err := url.Parse(strings.TrimRight(cfg.BaseURL, "/"))
	if err != nil {
		return nil, fmt.Errorf("invalid upstream base URL: %w", err)
	}

	httpClient := cfg.HTTPClient
	if httpClient == nil {
		httpClient = &http.Client{
			Timeout: cfg.Timeout,
		}
	}

	return &Client{
		baseURL:    parsedURL,
		apiKey:     cfg.APIKey,
		httpClient: httpClient,
	}, nil
}

// Forward sends the incoming request to the upstream LLM provider.
// It preserves request body, headers (omitting internal hop-by-hop headers),
// and adds Authorization if an API key is configured.
func (c *Client) Forward(ctx context.Context, path string, incomingHeader http.Header, body []byte) (*http.Response, error) {
	targetURL := *c.baseURL
	targetURL.Path = strings.TrimRight(targetURL.Path, "/") + path

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, targetURL.String(), bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("failed to create upstream request: %w", err)
	}

	// Copy headers
	for k, v := range incomingHeader {
		// Skip internal scheduling headers and hop-by-hop headers
		lower := strings.ToLower(k)
		if strings.HasPrefix(lower, "x-run-id") ||
			strings.HasPrefix(lower, "x-workload-class") ||
			strings.HasPrefix(lower, "x-priority") ||
			lower == "host" ||
			lower == "connection" ||
			lower == "content-length" {
			continue
		}
		for _, val := range v {
			req.Header.Add(k, val)
		}
	}

	req.Header.Set("Content-Type", "application/json")

	// Inject upstream API key if configured and not already set
	if c.apiKey != "" && req.Header.Get("Authorization") == "" {
		req.Header.Set("Authorization", "Bearer "+c.apiKey)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, err
	}

	return resp, nil
}
