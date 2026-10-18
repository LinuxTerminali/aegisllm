package server

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/LinuxTerminali/aegisllm/internal/config"
	"github.com/LinuxTerminali/aegisllm/internal/upstream"
)

// Two clients reusing the same X-Request-ID must both be served, and cancelling one must
// not affect the other. Before, the client ID was the queue key.
func TestServer_DuplicateClientRequestIDs(t *testing.T) {
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(150 * time.Millisecond)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"choices":[],"usage":{"total_tokens":3}}`))
	}))
	defer up.Close()
	client, _ := upstream.NewClient(upstream.ClientConfig{BaseURL: up.URL})
	srv, err := NewServer(config.ServerConfig{
		ListenAddr: "127.0.0.1:0", UpstreamURL: up.URL,
		MaxConcurrency: 1, MaxQueueSize: 10, MaxQueueWait: 5 * time.Second,
	}, client, nil)
	if err != nil {
		t.Fatal(err)
	}
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()

	body := []byte(`{"model":"m","messages":[{"role":"user","content":"hi"}]}`)
	var wg sync.WaitGroup
	codes := make([]int, 4)
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			ctx := context.Background()
			if i == 1 { // the second one gives up while queued
				var cancel context.CancelFunc
				ctx, cancel = context.WithTimeout(context.Background(), 50*time.Millisecond)
				defer cancel()
			}
			req, _ := http.NewRequestWithContext(ctx, http.MethodPost, ts.URL+"/v1/chat/completions", bytes.NewReader(body))
			req.Header.Set("Content-Type", "application/json")
			req.Header.Set("X-Request-ID", "same-id") // all four share it
			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				codes[i] = -1
				return
			}
			defer resp.Body.Close()
			codes[i] = resp.StatusCode
			if resp.Header.Get("X-Request-ID") != "same-id" {
				t.Errorf("client id not echoed")
			}
		}(i)
		time.Sleep(10 * time.Millisecond)
	}
	wg.Wait()
	for i, c := range codes {
		if i == 1 {
			continue
		}
		if c != http.StatusOK {
			t.Fatalf("request %d: expected 200, got %d (codes %v)", i, c, codes)
		}
	}
}

// A stream longer than the upstream timeout must not be cut off: the timeout bounds
// time-to-first-byte only.
func TestServer_LongStreamOutlivesUpstreamTimeout(t *testing.T) {
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		f := w.(http.Flusher)
		for i := 0; i < 6; i++ {
			fmt.Fprintf(w, "data: {\"i\":%d}\n\n", i)
			f.Flush()
			time.Sleep(300 * time.Millisecond)
		}
		fmt.Fprint(w, "data: [DONE]\n\n")
		f.Flush()
	}))
	defer up.Close()
	client, _ := upstream.NewClient(upstream.ClientConfig{BaseURL: up.URL, Timeout: 1 * time.Second})
	srv, _ := NewServer(config.ServerConfig{
		ListenAddr: "127.0.0.1:0", UpstreamURL: up.URL, UpstreamTimeout: 1 * time.Second,
		MaxConcurrency: 2, MaxQueueSize: 2, MaxQueueWait: 2 * time.Second,
	}, client, nil)
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()

	resp, err := http.Post(ts.URL+"/v1/chat/completions", "application/json", strings.NewReader(`{"model":"m","stream":true,"messages":[]}`))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	n := 0
	sc := bufio.NewScanner(resp.Body)
	for sc.Scan() {
		if strings.HasPrefix(sc.Text(), "data:") {
			n++
		}
	}
	if n != 7 {
		t.Fatalf("stream was cut: got %d of 7 events (1.8s stream, 1s upstream timeout)", n)
	}
}

// The proxy's own overload responses tell clients when to come back.
func TestServer_SchedulerErrorsCarryRetryAfter(t *testing.T) {
	block := make(chan struct{})
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		<-block
		_, _ = w.Write([]byte(`{"choices":[]}`))
	}))
	defer up.Close()
	defer close(block)
	client, _ := upstream.NewClient(upstream.ClientConfig{BaseURL: up.URL})
	srv, _ := NewServer(config.ServerConfig{
		ListenAddr: "127.0.0.1:0", UpstreamURL: up.URL,
		MaxConcurrency: 1, MaxQueueSize: 1, MaxQueueWait: 10 * time.Second,
	}, client, nil)
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()

	body := `{"model":"m","messages":[]}`
	go http.Post(ts.URL+"/v1/chat/completions", "application/json", strings.NewReader(body)) // takes the slot
	time.Sleep(50 * time.Millisecond)
	go http.Post(ts.URL+"/v1/chat/completions", "application/json", strings.NewReader(body)) // fills the queue
	time.Sleep(50 * time.Millisecond)
	resp, err := http.Post(ts.URL+"/v1/chat/completions", "application/json", strings.NewReader(body)) // rejected
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("expected 503 queue full, got %d", resp.StatusCode)
	}
	if resp.Header.Get("Retry-After") == "" {
		t.Fatalf("503 must carry Retry-After")
	}
}
