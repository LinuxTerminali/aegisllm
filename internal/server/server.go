package server

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"time"

	"github.com/model-scheduler/model-scheduler/internal/config"
	"github.com/model-scheduler/model-scheduler/internal/scheduler"
	"github.com/model-scheduler/model-scheduler/internal/upstream"
)

// Server encapsulates the HTTP proxy server.
type Server struct {
	httpServer *http.Server
	scheduler  *scheduler.Scheduler
	listener   net.Listener
}

// NewServer initializes the Server with configured scheduler, upstream client, and routing.
func NewServer(cfg config.ServerConfig, client *upstream.Client, s *scheduler.Scheduler) (*Server, error) {
	if s == nil {
		s = scheduler.NewScheduler(scheduler.Config{
			MaxConcurrency: cfg.MaxConcurrency,
			MaxQueueSize:   cfg.MaxQueueSize,
			MaxQueueWait:   cfg.MaxQueueWait,
		})
	}

	if client == nil {
		var err error
		client, err = upstream.NewClient(upstream.ClientConfig{
			BaseURL: cfg.UpstreamURL,
			APIKey:  cfg.UpstreamAPIKey,
			Timeout: 60 * time.Second,
		})
		if err != nil {
			return nil, fmt.Errorf("failed to initialize upstream client: %w", err)
		}
	}

	handlers := NewHandlers(s, client)

	mux := http.NewServeMux()
	mux.HandleFunc("/v1/chat/completions", handlers.ChatCompletions)
	mux.HandleFunc("/healthz", handlers.Healthz)
	mux.HandleFunc("/status", handlers.Status)

	httpServer := &http.Server{
		Addr:              cfg.ListenAddr,
		Handler:           mux,
		ReadHeaderTimeout: 10 * time.Second,
		IdleTimeout:       120 * time.Second,
	}

	return &Server{
		httpServer: httpServer,
		scheduler:  s,
	}, nil
}

// Start begins serving requests on the configured listener address.
func (s *Server) Start() error {
	ln, err := net.Listen("tcp", s.httpServer.Addr)
	if err != nil {
		return fmt.Errorf("failed to bind address %s: %w", s.httpServer.Addr, err)
	}
	s.listener = ln
	log.Printf("[SERVER] Listening on http://%s", ln.Addr().String())

	if err := s.httpServer.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return fmt.Errorf("server error: %w", err)
	}
	return nil
}

// Addr returns the bound address of the server (useful when port is 0 in tests).
func (s *Server) Addr() string {
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
