package config

import (
	"errors"
	"flag"
	"fmt"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"
)

// ServerConfig stores the complete configuration for the scheduler service.
type ServerConfig struct {
	ListenAddr        string
	UpstreamURL       string
	UpstreamAPIKey    string
	ProviderType      string // "openai" (default), "anthropic", "azure", "generic"
	AuthHeaderName    string
	AuthHeaderPrefix  string
	DefaultModel      string
	UpstreamTimeout   time.Duration
	MaxConcurrency    int
	MaxQueueSize      int
	MaxQueueWait      time.Duration
	RunMockUpstream   bool
	MockAddr          string
	MockLatency       time.Duration
	SchedulingPolicy  string
	RequestsPerMinute float64
	RequestBurst      int
	TokensPerMinute   float64
	TokenBurst        int
	MaxPacingDelay    time.Duration
	AutoLearnLimits   bool
	MaxRetries        int
	InitialBackoff    time.Duration
	MaxBackoff        time.Duration
	WeightProduction  int
	WeightInteractive int
	WeightBackground  int
	WeightBatch       int
}

// DefaultConfig returns safe default settings.
func DefaultConfig() ServerConfig {
	return ServerConfig{
		ListenAddr:        ":8080",
		UpstreamURL:       "http://127.0.0.1:8081",
		UpstreamAPIKey:    "",
		ProviderType:      "openai",
		AuthHeaderName:    "",
		AuthHeaderPrefix:  "",
		DefaultModel:      "",
		UpstreamTimeout:   60 * time.Second,
		MaxConcurrency:    32,
		MaxQueueSize:      100,
		MaxQueueWait:      30 * time.Second,
		RunMockUpstream:   false,
		MockAddr:          ":8081",
		MockLatency:       30 * time.Millisecond,
		SchedulingPolicy:  "fair",
		RequestsPerMinute: 0, // 0 = unconstrained
		RequestBurst:      0,
		TokensPerMinute:   0, // 0 = unconstrained
		TokenBurst:        0,
		MaxPacingDelay:    0, // 0 = wait for quota up to max-queue-wait instead of rejecting
		AutoLearnLimits:   true,
		MaxRetries:        3,
		InitialBackoff:    50 * time.Millisecond,
		MaxBackoff:        2 * time.Second,
		WeightProduction:  8,
		WeightInteractive: 4,
		WeightBackground:  2,
		WeightBatch:       1,
	}
}

// Validate checks the configuration for semantic correctness and returns an error if invalid.
func (c *ServerConfig) Validate() error {
	if strings.TrimSpace(c.ListenAddr) == "" {
		return errors.New("listen-addr cannot be empty")
	}

	u, err := url.Parse(c.UpstreamURL)
	if err != nil || u.Scheme == "" || u.Host == "" {
		return fmt.Errorf("invalid upstream-url %q: must be absolute HTTP/HTTPS URL", c.UpstreamURL)
	}
	if u.Scheme != "http" && u.Scheme != "https" {
		return fmt.Errorf("invalid upstream-url scheme %q: must be http or https", u.Scheme)
	}

	if c.MaxConcurrency <= 0 {
		return fmt.Errorf("max-concurrency must be > 0, got %d", c.MaxConcurrency)
	}
	if c.MaxQueueSize <= 0 {
		return fmt.Errorf("max-queue-size must be > 0, got %d", c.MaxQueueSize)
	}
	if c.MaxQueueWait <= 0 {
		return fmt.Errorf("max-queue-wait must be > 0, got %v", c.MaxQueueWait)
	}

	policy := strings.ToLower(strings.TrimSpace(c.SchedulingPolicy))
	if policy == "" {
		policy = "fair"
		c.SchedulingPolicy = "fair"
	}
	if policy != "fair" && policy != "priority" && policy != "fifo" {
		return fmt.Errorf("invalid scheduling-policy %q: must be 'fair', 'priority', or 'fifo'", c.SchedulingPolicy)
	}

	if c.RequestsPerMinute < 0 {
		return fmt.Errorf("rpm must be >= 0, got %f", c.RequestsPerMinute)
	}
	if c.TokensPerMinute < 0 {
		return fmt.Errorf("tpm must be >= 0, got %f", c.TokensPerMinute)
	}

	if c.RequestsPerMinute > 0 && c.RequestBurst <= 0 {
		// Auto-default burst to 10% of RPM or 1
		c.RequestBurst = int(c.RequestsPerMinute / 10)
		if c.RequestBurst < 1 {
			c.RequestBurst = 1
		}
	}
	if c.TokensPerMinute > 0 && c.TokenBurst <= 0 {
		// Auto-default burst to 10% of TPM or 100
		c.TokenBurst = int(c.TokensPerMinute / 10)
		if c.TokenBurst < 100 {
			c.TokenBurst = 100
		}
	}

	if c.MaxRetries < 0 {
		return fmt.Errorf("max-retries must be >= 0, got %d", c.MaxRetries)
	}
	if c.InitialBackoff <= 0 {
		c.InitialBackoff = 50 * time.Millisecond
	}
	if c.MaxBackoff < c.InitialBackoff {
		c.MaxBackoff = c.InitialBackoff * 10
	}
	if c.UpstreamTimeout <= 0 {
		c.UpstreamTimeout = 60 * time.Second
	}

	return nil
}

// RedactedAPIKey returns a safe, masked representation of the upstream API key for logging.
func (c *ServerConfig) RedactedAPIKey() string {
	if c.UpstreamAPIKey == "" {
		return "(none)"
	}
	if len(c.UpstreamAPIKey) <= 8 {
		return "****"
	}
	return c.UpstreamAPIKey[:3] + "..." + c.UpstreamAPIKey[len(c.UpstreamAPIKey)-4:]
}

// ParseFlags parses command-line flags and environment variables into ServerConfig.
func ParseFlags(args []string) (ServerConfig, error) {
	cfg := DefaultConfig()

	fs := flag.NewFlagSet("aegis", flag.ContinueOnError)

	fs.StringVar(&cfg.ListenAddr, "listen-addr", getEnv("SCHEDULER_LISTEN_ADDR", cfg.ListenAddr), "Address to listen on for proxy traffic")
	fs.StringVar(&cfg.UpstreamURL, "upstream-url", getEnv("SCHEDULER_UPSTREAM_URL", cfg.UpstreamURL), "Upstream LLM provider base URL")
	fs.StringVar(&cfg.UpstreamAPIKey, "upstream-api-key", getEnv("SCHEDULER_UPSTREAM_API_KEY", cfg.UpstreamAPIKey), "Optional API key for upstream provider")
	fs.StringVar(&cfg.ProviderType, "provider-type", getEnv("SCHEDULER_PROVIDER_TYPE", cfg.ProviderType), "Upstream provider adapter: 'openai', 'anthropic', 'azure', or 'generic'")
	fs.StringVar(&cfg.AuthHeaderName, "auth-header-name", getEnv("SCHEDULER_AUTH_HEADER_NAME", cfg.AuthHeaderName), "Custom authentication header name (for generic provider)")
	fs.StringVar(&cfg.AuthHeaderPrefix, "auth-header-prefix", getEnv("SCHEDULER_AUTH_HEADER_PREFIX", cfg.AuthHeaderPrefix), "Custom authentication header prefix (e.g. 'Bearer ')")
	fs.StringVar(&cfg.DefaultModel, "default-model", getEnv("SCHEDULER_DEFAULT_MODEL", cfg.DefaultModel), "Default model name if omitted in request")
	fs.DurationVar(&cfg.UpstreamTimeout, "upstream-timeout", getEnvDuration("SCHEDULER_UPSTREAM_TIMEOUT", cfg.UpstreamTimeout), "Upstream HTTP request timeout")
	fs.IntVar(&cfg.MaxConcurrency, "max-concurrency", getEnvInt("SCHEDULER_MAX_CONCURRENCY", cfg.MaxConcurrency), "Maximum concurrent in-flight requests to upstream")
	fs.IntVar(&cfg.MaxQueueSize, "max-queue-size", getEnvInt("SCHEDULER_MAX_QUEUE_SIZE", cfg.MaxQueueSize), "Maximum queued requests before rejection")
	fs.DurationVar(&cfg.MaxQueueWait, "max-queue-wait", getEnvDuration("SCHEDULER_MAX_QUEUE_WAIT", cfg.MaxQueueWait), "Maximum time a request may wait in queue before timing out")
	fs.BoolVar(&cfg.RunMockUpstream, "run-mock", getEnvBool("SCHEDULER_RUN_MOCK", cfg.RunMockUpstream), "Start embedded mock upstream provider")
	fs.StringVar(&cfg.MockAddr, "mock-addr", getEnv("SCHEDULER_MOCK_ADDR", cfg.MockAddr), "Address for embedded mock upstream server")
	fs.DurationVar(&cfg.MockLatency, "mock-latency", getEnvDuration("SCHEDULER_MOCK_LATENCY", cfg.MockLatency), "Simulated inference latency for mock provider")
	fs.StringVar(&cfg.SchedulingPolicy, "scheduling-policy", getEnv("SCHEDULER_SCHEDULING_POLICY", cfg.SchedulingPolicy), "Queue scheduling policy: 'fair', 'priority', or 'fifo'")
	fs.Float64Var(&cfg.RequestsPerMinute, "rpm", getEnvFloat("SCHEDULER_RPM", cfg.RequestsPerMinute), "Maximum requests per minute quota (0 = unconstrained)")
	fs.IntVar(&cfg.RequestBurst, "request-burst", getEnvInt("SCHEDULER_REQUEST_BURST", cfg.RequestBurst), "Request token bucket capacity")
	fs.Float64Var(&cfg.TokensPerMinute, "tpm", getEnvFloat("SCHEDULER_TPM", cfg.TokensPerMinute), "Maximum tokens per minute quota (0 = unconstrained)")
	fs.IntVar(&cfg.TokenBurst, "token-burst", getEnvInt("SCHEDULER_TOKEN_BURST", cfg.TokenBurst), "Token rate bucket capacity")
	fs.DurationVar(&cfg.MaxPacingDelay, "max-pacing-delay", getEnvDuration("SCHEDULER_MAX_PACING_DELAY", cfg.MaxPacingDelay), "Maximum time to pause a request for rate-limit pacing before rejecting with 429 (0 = wait up to max-queue-wait)")
	fs.BoolVar(&cfg.AutoLearnLimits, "auto-learn-limits", getEnvBool("SCHEDULER_AUTO_LEARN_LIMITS", cfg.AutoLearnLimits), "Learn RPM/TPM limits from provider rate-limit headers when -rpm/-tpm are not set")
	fs.IntVar(&cfg.MaxRetries, "max-retries", getEnvInt("SCHEDULER_MAX_RETRIES", cfg.MaxRetries), "Maximum coordinated retries on transient errors")
	fs.DurationVar(&cfg.InitialBackoff, "initial-backoff", getEnvDuration("SCHEDULER_INITIAL_BACKOFF", cfg.InitialBackoff), "Initial backoff duration for retries")
	fs.DurationVar(&cfg.MaxBackoff, "max-backoff", getEnvDuration("SCHEDULER_MAX_BACKOFF", cfg.MaxBackoff), "Maximum backoff duration for retries")
	fs.IntVar(&cfg.WeightProduction, "weight-production", getEnvInt("SCHEDULER_WEIGHT_PROD", cfg.WeightProduction), "Relative scheduling weight for production traffic")
	fs.IntVar(&cfg.WeightInteractive, "weight-interactive", getEnvInt("SCHEDULER_WEIGHT_INTER", cfg.WeightInteractive), "Relative scheduling weight for interactive traffic")
	fs.IntVar(&cfg.WeightBackground, "weight-background", getEnvInt("SCHEDULER_WEIGHT_BG", cfg.WeightBackground), "Relative scheduling weight for background traffic")
	fs.IntVar(&cfg.WeightBatch, "weight-batch", getEnvInt("SCHEDULER_WEIGHT_BATCH", cfg.WeightBatch), "Relative scheduling weight for batch traffic")

	if err := fs.Parse(args); err != nil {
		return cfg, err
	}

	if err := cfg.Validate(); err != nil {
		return cfg, err
	}

	return cfg, nil
}

func getEnv(key, defVal string) string {
	if val, ok := os.LookupEnv(key); ok && val != "" {
		return val
	}
	return defVal
}

func getEnvInt(key string, defVal int) int {
	if val, ok := os.LookupEnv(key); ok && val != "" {
		if i, err := strconv.Atoi(val); err == nil {
			return i
		}
	}
	return defVal
}

func getEnvFloat(key string, defVal float64) float64 {
	if val, ok := os.LookupEnv(key); ok && val != "" {
		if f, err := strconv.ParseFloat(val, 64); err == nil {
			return f
		}
	}
	return defVal
}

func getEnvBool(key string, defVal bool) bool {
	if val, ok := os.LookupEnv(key); ok && val != "" {
		if b, err := strconv.ParseBool(val); err == nil {
			return b
		}
	}
	return defVal
}

func getEnvDuration(key string, defVal time.Duration) time.Duration {
	if val, ok := os.LookupEnv(key); ok && val != "" {
		if d, err := time.ParseDuration(val); err == nil {
			return d
		}
	}
	return defVal
}
