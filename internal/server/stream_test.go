package server

import (
	"bufio"
	"bytes"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/LinuxTerminali/aegisllm/internal/config"
	"github.com/LinuxTerminali/aegisllm/internal/upstream"
)

func newStreamingProxy(t *testing.T, upstreamURL, providerType string) (*Server, *httptest.Server) {
	t.Helper()
	client, err := upstream.NewClient(upstream.ClientConfig{BaseURL: upstreamURL, ProviderType: providerType, APIKey: "k"})
	if err != nil {
		t.Fatal(err)
	}
	cfg := config.ServerConfig{
		ListenAddr:      "127.0.0.1:0",
		UpstreamURL:     upstreamURL,
		ProviderType:    providerType,
		MaxConcurrency:  4,
		MaxQueueSize:    10,
		MaxQueueWait:    5 * time.Second,
		TokensPerMinute: 60000,
		TokenBurst:      6000,
		MaxRetries:      2,
		InitialBackoff:  10 * time.Millisecond,
		MaxBackoff:      50 * time.Millisecond,
	}
	srv, err := NewServer(cfg, client, nil)
	if err != nil {
		t.Fatal(err)
	}
	ts := httptest.NewServer(srv.Handler())
	t.Cleanup(ts.Close)
	return srv, ts
}

// Events must reach the client as they are produced, not after the whole stream ends.
func TestServer_StreamingPassThroughIsIncremental(t *testing.T) {
	var calls int32
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&calls, 1)
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		f := w.(http.Flusher)
		for i := 0; i < 3; i++ {
			fmt.Fprintf(w, "data: {\"id\":\"c\",\"choices\":[{\"delta\":{\"content\":\"tok%d\"}}]}\n\n", i)
			f.Flush()
			time.Sleep(150 * time.Millisecond)
		}
		fmt.Fprint(w, "data: {\"id\":\"c\",\"choices\":[],\"usage\":{\"prompt_tokens\":10,\"completion_tokens\":3,\"total_tokens\":13}}\n\n")
		fmt.Fprint(w, "data: [DONE]\n\n")
		f.Flush()
	}))
	defer up.Close()

	srv, ts := newStreamingProxy(t, up.URL, "openai")

	body := []byte(`{"model":"gpt-4o","stream":true,"messages":[{"role":"user","content":"hi"}]}`)
	start := time.Now()
	resp, err := http.Post(ts.URL+"/v1/chat/completions", "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status %d", resp.StatusCode)
	}
	if ct := resp.Header.Get("Content-Type"); !strings.Contains(ct, "text/event-stream") {
		t.Fatalf("content-type not passed through: %q", ct)
	}

	sc := bufio.NewScanner(resp.Body)
	var firstAt, lastAt time.Duration
	n := 0
	for sc.Scan() {
		if strings.HasPrefix(sc.Text(), "data:") {
			n++
			if n == 1 {
				firstAt = time.Since(start)
			}
			lastAt = time.Since(start)
		}
	}
	if n != 5 {
		t.Fatalf("expected 5 data lines, got %d", n)
	}
	// The first event arrives ~0ms; the last ~450ms later. If we buffered, both would be ~450ms.
	if firstAt > 100*time.Millisecond || lastAt-firstAt < 300*time.Millisecond {
		t.Fatalf("stream was buffered: first=%v last=%v", firstAt, lastAt)
	}
	if atomic.LoadInt32(&calls) != 1 {
		t.Fatalf("upstream called %d times", calls)
	}
	if got := srv.Scheduler().Stats().Capacity.TotalTokensReconciled; got != 13 {
		t.Fatalf("expected 13 tokens reconciled from stream usage, got %d", got)
	}
}

// A 429 before any bytes are streamed is retried; a stream that starts is never retried.
func TestServer_StreamingRetriesOnlyBeforeFirstByte(t *testing.T) {
	var calls int32
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := atomic.AddInt32(&calls, 1)
		if n == 1 {
			w.Header().Set("Retry-After", "0")
			w.WriteHeader(http.StatusTooManyRequests)
			_, _ = w.Write([]byte(`{"error":{"message":"busy","type":"rate_limit_error"}}`))
			return
		}
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		f := w.(http.Flusher)
		fmt.Fprint(w, "event: message_start\ndata: {\"type\":\"message_start\",\"message\":{\"usage\":{\"input_tokens\":100,\"output_tokens\":1}}}\n\n")
		f.Flush()
		fmt.Fprint(w, "event: message_delta\ndata: {\"type\":\"message_delta\",\"usage\":{\"output_tokens\":20}}\n\n")
		f.Flush()
		// Die mid-stream: the proxy must relay what it has and NOT re-issue the request.
		if hj, ok := w.(http.Hijacker); ok {
			conn, _, _ := hj.Hijack()
			conn.Close()
		}
	}))
	defer up.Close()

	srv, ts := newStreamingProxy(t, up.URL, "anthropic")

	body := []byte(`{"model":"claude-sonnet-4-5","max_tokens":64,"stream":true,"messages":[{"role":"user","content":"hi"}]}`)
	resp, err := http.Post(ts.URL+"/v1/messages", "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status %d", resp.StatusCode)
	}
	sc := bufio.NewScanner(resp.Body)
	events := 0
	for sc.Scan() {
		if strings.HasPrefix(sc.Text(), "data:") {
			events++
		}
	}
	if events != 2 {
		t.Fatalf("expected the 2 relayed events, got %d", events)
	}
	if c := atomic.LoadInt32(&calls); c != 2 {
		t.Fatalf("expected exactly 2 upstream calls (one 429, one stream), got %d", c)
	}
	if got := srv.Scheduler().Stats().Capacity.TotalTokensReconciled; got != 120 {
		t.Fatalf("expected 120 tokens reconciled from Anthropic stream, got %d", got)
	}
}
