package server

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/LinuxTerminali/aegisllm/internal/capacity"
	"github.com/LinuxTerminali/aegisllm/internal/config"
	"github.com/LinuxTerminali/aegisllm/internal/estimation"
	"github.com/LinuxTerminali/aegisllm/internal/metrics"
	"github.com/LinuxTerminali/aegisllm/internal/model"
	"github.com/LinuxTerminali/aegisllm/internal/queue"
	"github.com/LinuxTerminali/aegisllm/internal/retry"
	"github.com/LinuxTerminali/aegisllm/internal/scheduler"
	"github.com/LinuxTerminali/aegisllm/internal/upstream"
)

// Server encapsulates the HTTP proxy server.
type Server struct {
	httpServer *http.Server
	scheduler  *scheduler.Scheduler
	mu         sync.RWMutex
	listener   net.Listener
	metrics    *metrics.Collector
}

// NewServer initializes the Server with configured scheduler, upstream client, metrics, and routing.
func NewServer(cfg config.ServerConfig, client *upstream.Client, s *scheduler.Scheduler) (*Server, error) {
	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("invalid server configuration: %w", err)
	}

	if s == nil {
		tracker := capacity.NewDualTokenBucketTracker(capacity.Config{
			RequestsPerMinute: cfg.RequestsPerMinute,
			RequestBurst:      cfg.RequestBurst,
			TokensPerMinute:   cfg.TokensPerMinute,
			TokenBurst:        cfg.TokenBurst,
			MaxPacingDelay:    cfg.MaxPacingDelay,
		})
		est := estimation.NewHeuristicEstimator(estimation.DefaultConfig())

		var weights map[int]int
		if cfg.WeightProduction > 0 || cfg.WeightInteractive > 0 || cfg.WeightBackground > 0 || cfg.WeightBatch > 0 {
			weights = map[int]int{
				model.PriorityProduction:  cfg.WeightProduction,
				model.PriorityInteractive: cfg.WeightInteractive,
				model.PriorityBackground:  cfg.WeightBackground,
				model.PriorityBatch:       cfg.WeightBatch,
			}
		}

		s = scheduler.NewSchedulerWithDependencies(scheduler.Config{
			MaxConcurrency:  cfg.MaxConcurrency,
			MaxQueueSize:    cfg.MaxQueueSize,
			MaxQueueWait:    cfg.MaxQueueWait,
			Policy:          queue.SchedulingPolicy(cfg.SchedulingPolicy),
			PriorityWeights: weights,
		}, tracker, est)
	}

	if client == nil {
		var err error
		client, err = upstream.NewClient(upstream.ClientConfig{
			BaseURL:          cfg.UpstreamURL,
			APIKey:           cfg.UpstreamAPIKey,
			ProviderType:     cfg.ProviderType,
			AuthHeaderName:   cfg.AuthHeaderName,
			AuthHeaderPrefix: cfg.AuthHeaderPrefix,
			DefaultModel:     cfg.DefaultModel,
			Timeout:          cfg.UpstreamTimeout,
		})
		if err != nil {
			return nil, fmt.Errorf("failed to initialize upstream client: %w", err)
		}
	}

	rp := retry.Policy{
		MaxRetries:     cfg.MaxRetries,
		InitialBackoff: cfg.InitialBackoff,
		MaxBackoff:     cfg.MaxBackoff,
		BackoffFactor:  2.0,
		JitterFraction: 0.25,
	}

	m := metrics.NewCollector()
	handlers := NewHandlers(s, client, rp, m)

	mux := http.NewServeMux()
	mux.HandleFunc("/v1/chat/completions", handlers.ChatCompletions)
	mux.HandleFunc("/healthz", handlers.Healthz)
	mux.HandleFunc("/status", handlers.Status)
	mux.HandleFunc("/metrics", handlers.Metrics)

	httpServer := &http.Server{
		Addr:              cfg.ListenAddr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       30 * time.Second,
		IdleTimeout:       120 * time.Second,
	}

	return &Server{
		httpServer: httpServer,
		scheduler:  s,
		metrics:    m,
	}, nil
}

// Listen binds the network listener synchronously if not already bound.
func (s *Server) Listen() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.listener != nil {
		return nil
	}
	ln, err := net.Listen("tcp", s.httpServer.Addr)
	if err != nil {
		return fmt.Errorf("failed to bind address %s: %w", s.httpServer.Addr, err)
	}
	s.listener = ln
	return nil
}

// Start begins serving requests on the listener.
func (s *Server) Start() error {
	if err := s.Listen(); err != nil {
		return err
	}

	s.mu.RLock()
	ln := s.listener
	s.mu.RUnlock()

	log.Printf("[SERVER] Listening on http://%s", ln.Addr().String())

	if err := s.httpServer.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return fmt.Errorf("server error: %w", err)
	}
	return nil
}

// Addr returns the bound address of the server (useful when port is 0 in tests).
func (s *Server) Addr() string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.listener != nil {
		return s.listener.Addr().String()
	}
	return s.httpServer.Addr
}

// Shutdown gracefully stops the server, waiting for active requests to finish.
func (s *Server) Shutdown(ctx context.Context) error {
	log.Printf("[SERVER] Initiating graceful shutdown...")
	return s.httpServer.Shutdown(ctx)
}

// Scheduler returns the underlying scheduler instance.
func (s *Server) Scheduler() *scheduler.Scheduler {
	return s.scheduler
}

// Metrics returns the metrics collector.
func (s *Server) Metrics() *metrics.Collector {
	return s.metrics
}

// Handler returns the underlying http.Handler.
func (s *Server) Handler() http.Handler {
	return s.httpServer.Handler
}
