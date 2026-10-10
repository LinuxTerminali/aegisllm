package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/model-scheduler/model-scheduler/internal/upstream"
)

func main() {
	listenAddr := flag.String("listen-addr", ":8081", "Address for mock upstream server")
	latency := flag.Duration("latency", 50*time.Millisecond, "Simulated inference latency")
	maxInFlight := flag.Int("max-in-flight", 0, "Max concurrent requests before returning 429 (0 = unlimited)")
	flag.Parse()

	mock := upstream.NewMockUpstream(upstream.MockConfig{
		BaseLatency:        *latency,
		MaxAllowedInFlight: *maxInFlight,
	})

	srv := &http.Server{
		Addr:              *listenAddr,
		Handler:           mock,
		ReadHeaderTimeout: 5 * time.Second,
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	go func() {
		log.Printf("[MOCK] Mock LLM Upstream listening on http://%s (latency: %v, max-in-flight: %d)", *listenAddr, *latency, *maxInFlight)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			fmt.Fprintf(os.Stderr, "Mock server error: %v\n", err)
			os.Exit(1)
		}
	}()

	<-ctx.Done()
	log.Printf("[MOCK] Shutting down...")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	_ = srv.Shutdown(shutdownCtx)
}
