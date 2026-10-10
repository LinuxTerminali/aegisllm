package config

import (
	"flag"
	"os"
	"strconv"
	"time"
)

// ServerConfig stores the complete configuration for the scheduler service.
type ServerConfig struct {
	ListenAddr       string
	UpstreamURL      string
	UpstreamAPIKey   string
	MaxConcurrency   int
	MaxQueueSize     int
	MaxQueueWait     time.Duration
	RunMockUpstream  bool
	MockAddr         string
	MockLatency      time.Duration
	SchedulingPolicy string
}

// DefaultConfig returns safe default settings.
func DefaultConfig() ServerConfig {
	return ServerConfig{
		ListenAddr:       ":8080",
		UpstreamURL:      "http://127.0.0.1:8081",
		UpstreamAPIKey:   "",
		MaxConcurrency:   5,
		MaxQueueSize:     100,
		MaxQueueWait:     30 * time.Second,
		RunMockUpstream:  false,
		MockAddr:         ":8081",
		MockLatency:      30 * time.Millisecond,
		SchedulingPolicy: "priority",
	}
}

// ParseFlags parses command-line flags and environment variables into ServerConfig.
func ParseFlags(args []string) (ServerConfig, error) {
	cfg := DefaultConfig()

	fs := flag.NewFlagSet("model-scheduler", flag.ContinueOnError)

	fs.StringVar(&cfg.ListenAddr, "listen-addr", getEnv("SCHEDULER_LISTEN_ADDR", cfg.ListenAddr), "Address to listen on for proxy traffic")
	fs.StringVar(&cfg.UpstreamURL, "upstream-url", getEnv("SCHEDULER_UPSTREAM_URL", cfg.UpstreamURL), "Upstream LLM provider base URL")
	fs.StringVar(&cfg.UpstreamAPIKey, "upstream-api-key", getEnv("SCHEDULER_UPSTREAM_API_KEY", cfg.UpstreamAPIKey), "Optional API key for upstream provider")
	fs.IntVar(&cfg.MaxConcurrency, "max-concurrency", getEnvInt("SCHEDULER_MAX_CONCURRENCY", cfg.MaxConcurrency), "Maximum concurrent in-flight requests to upstream")
	fs.IntVar(&cfg.MaxQueueSize, "max-queue-size", getEnvInt("SCHEDULER_MAX_QUEUE_SIZE", cfg.MaxQueueSize), "Maximum queued requests before rejection")
	fs.DurationVar(&cfg.MaxQueueWait, "max-queue-wait", getEnvDuration("SCHEDULER_MAX_QUEUE_WAIT", cfg.MaxQueueWait), "Maximum time a request may wait in queue before timing out")
	fs.BoolVar(&cfg.RunMockUpstream, "run-mock", getEnvBool("SCHEDULER_RUN_MOCK", cfg.RunMockUpstream), "Start embedded mock upstream provider")
	fs.StringVar(&cfg.MockAddr, "mock-addr", getEnv("SCHEDULER_MOCK_ADDR", cfg.MockAddr), "Address for embedded mock upstream server")
	fs.DurationVar(&cfg.MockLatency, "mock-latency", getEnvDuration("SCHEDULER_MOCK_LATENCY", cfg.MockLatency), "Simulated inference latency for mock provider")
	fs.StringVar(&cfg.SchedulingPolicy, "scheduling-policy", getEnv("SCHEDULER_SCHEDULING_POLICY", cfg.SchedulingPolicy), "Queue scheduling policy: 'priority' or 'fifo'")

	if err := fs.Parse(args); err != nil {
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
