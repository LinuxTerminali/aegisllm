package server

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/LinuxTerminali/aegisllm/internal/config"
	"github.com/LinuxTerminali/aegisllm/internal/upstream"
)

func TestServer_AllV1PathsAreScheduled(t *testing.T) {
	var seen []string
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		seen = append(seen, r.URL.Path)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"input_tokens":12}`))
	}))
	defer up.Close()

	client, _ := upstream.NewClient(upstream.ClientConfig{BaseURL: up.URL, ProviderType: "anthropic", APIKey: "k"})
	srv, err := NewServer(config.ServerConfig{
		ListenAddr: "127.0.0.1:0", UpstreamURL: up.URL, ProviderType: "anthropic",
		MaxConcurrency: 2, MaxQueueSize: 4, MaxQueueWait: time.Second,
	}, client, nil)
	if err != nil {
		t.Fatal(err)
	}
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()

	for _, p := range []string{"/v1/messages/count_tokens", "/v1/embeddings", "/v1/responses"} {
		resp, err := http.Post(ts.URL+p, "application/json", bytes.NewReader([]byte(`{"model":"m"}`)))
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("%s: expected 200 via scheduler, got %d", p, resp.StatusCode)
		}
	}
	if len(seen) != 3 || seen[0] != "/v1/messages/count_tokens" {
		t.Fatalf("upstream paths: %v", seen)
	}
	if srv.Scheduler().Stats().TotalAdmitted != 3 {
		t.Fatalf("expected 3 admitted through scheduler, got %d", srv.Scheduler().Stats().TotalAdmitted)
	}
}
