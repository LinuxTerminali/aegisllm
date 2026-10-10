package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/model-scheduler/model-scheduler/internal/config"
	"github.com/model-scheduler/model-scheduler/internal/server"
	"github.com/model-scheduler/model-scheduler/internal/upstream"
)

func main() {
	cfg, err := config.ParseFlags(os.Args[1:])
	if err != nil {
		fmt.Fprintf(os.Stderr, "Configuration error: %v\n", err)
		os.Exit(1)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// If requested, start embedded mock upstream provider
	if cfg.RunMockUpstream {
		mock := upstream.NewMockUpstream(upstream.MockConfig{
			BaseLatency: cfg.MockLatency,
		})
		mockSrv := &http.Server{
			Addr:              cfg.MockAddr,
			Handler:           mock,
			ReadHeaderTimeout: 5 * time.Second,
		}

		go func() {
			log.Printf("[MOCK] Starting embedded mock upstream on http://%s (latency: %v)", cfg.MockAddr, cfg.MockLatency)
			if err := mockSrv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
				log.Fatalf("[MOCK] Embedded mock server error: %v", err)
			}
		}()

		defer func() {
			shutdownCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			_ = mockSrv.Shutdown(shutdownCtx)
		}()
	}

	// Initialize proxy server
	srv, err := server.NewServer(cfg, nil, nil)
	if err != nil {
		log.Fatalf("[MAIN] Failed to create proxy server: %v", err)
	}

	// Start server in background
	serverErrCh := make(chan error, 1)
	go func() {
		serverErrCh <- srv.Start()
	}()

	log.Printf("[MAIN] LLM Workload Scheduler running. Proxy: %s -> Upstream: %s (Concurrency: %d, QueueSize: %d)",
		srv.Addr(), cfg.UpstreamURL, cfg.MaxConcurrency, cfg.MaxQueueSize)

	// Wait for OS termination signal or startup error
	select {
	case <-ctx.Done():
		log.Printf("[MAIN] Received shutdown signal.")
	case err := <-serverErrCh:
		if err != nil {
			log.Fatalf("[MAIN] Server failure: %v", err)
		}
	}

	// Graceful shutdown
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := srv.Shutdown(shutdownCtx); err != nil {
		log.Printf("[MAIN] Graceful shutdown error: %v", err)
	} else {
		log.Printf("[MAIN] Server stopped gracefully.")
	}
}
