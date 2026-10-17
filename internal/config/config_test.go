package config

import (
	"testing"
	"time"
)

func TestConfig_Validation(t *testing.T) {
	tests := []struct {
		name    string
		mutate  func(*ServerConfig)
		wantErr bool
	}{
		{
			name:    "valid default config",
			mutate:  func(c *ServerConfig) {},
			wantErr: false,
		},
		{
			name: "empty listen addr",
			mutate: func(c *ServerConfig) {
				c.ListenAddr = ""
			},
			wantErr: true,
		},
		{
			name: "invalid upstream URL scheme",
			mutate: func(c *ServerConfig) {
				c.UpstreamURL = "ftp://localhost:8080"
			},
			wantErr: true,
		},
		{
			name: "relative upstream URL",
			mutate: func(c *ServerConfig) {
				c.UpstreamURL = "localhost:8080"
			},
			wantErr: true,
		},
		{
			name: "non-positive concurrency",
			mutate: func(c *ServerConfig) {
				c.MaxConcurrency = 0
			},
			wantErr: true,
		},
		{
			name: "non-positive queue size",
			mutate: func(c *ServerConfig) {
				c.MaxQueueSize = 0
			},
			wantErr: true,
		},
		{
			name: "invalid scheduling policy",
			mutate: func(c *ServerConfig) {
				c.SchedulingPolicy = "random"
			},
			wantErr: true,
		},
		{
			name: "negative RPM",
			mutate: func(c *ServerConfig) {
				c.RequestsPerMinute = -5
			},
			wantErr: true,
		},
		{
			name: "negative retries",
			mutate: func(c *ServerConfig) {
				c.MaxRetries = -1
			},
			wantErr: true,
		},
		{
			name: "auto defaults burst when RPM set",
			mutate: func(c *ServerConfig) {
				c.RequestsPerMinute = 60
				c.RequestBurst = 0
			},
			wantErr: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cfg := DefaultConfig()
			tc.mutate(&cfg)
			err := cfg.Validate()
			if (err != nil) != tc.wantErr {
				t.Fatalf("Validate() err = %v, wantErr %v", err, tc.wantErr)
			}
		})
	}
}

func TestConfig_RedactedAPIKey(t *testing.T) {
	cfg := DefaultConfig()
	if got := cfg.RedactedAPIKey(); got != "(none)" {
		t.Errorf("expected (none), got %s", got)
	}

	cfg.UpstreamAPIKey = "short"
	if got := cfg.RedactedAPIKey(); got != "****" {
		t.Errorf("expected ****, got %s", got)
	}

	cfg.UpstreamAPIKey = "sk-proj-1234567890abcdef"
	got := cfg.RedactedAPIKey()
	if got != "sk-...cdef" {
		t.Errorf("expected sk-...cdef, got %s", got)
	}
}

func TestConfig_ParseFlags(t *testing.T) {
	args := []string{
		"-listen-addr", ":9090",
		"-upstream-url", "https://api.openai.com",
		"-provider-type", "openai",
		"-max-concurrency", "10",
		"-scheduling-policy", "priority",
		"-upstream-timeout", "45s",
	}

	cfg, err := ParseFlags(args)
	if err != nil {
		t.Fatalf("ParseFlags failed: %v", err)
	}

	if cfg.ListenAddr != ":9090" {
		t.Errorf("expected :9090, got %s", cfg.ListenAddr)
	}
	if cfg.UpstreamURL != "https://api.openai.com" {
		t.Errorf("expected https://api.openai.com, got %s", cfg.UpstreamURL)
	}
	if cfg.MaxConcurrency != 10 {
		t.Errorf("expected 10, got %d", cfg.MaxConcurrency)
	}
	if cfg.SchedulingPolicy != "priority" {
		t.Errorf("expected priority, got %s", cfg.SchedulingPolicy)
	}
	if cfg.UpstreamTimeout != 45*time.Second {
		t.Errorf("expected 45s, got %v", cfg.UpstreamTimeout)
	}
}
