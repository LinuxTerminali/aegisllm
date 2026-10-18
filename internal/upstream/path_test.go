package upstream

import (
	"context"
	"net/http"
	"net/url"
	"testing"
)

func TestJoinUpstreamPath(t *testing.T) {
	cases := []struct{ base, in, want string }{
		{"/v1", "/v1/chat/completions", "/v1/chat/completions"},
		{"", "/v1/chat/completions", "/v1/chat/completions"},
		{"/", "/v1/messages", "/v1/messages"},
		{"/openai/deployments/gpt4o/chat/completions", "/v1/chat/completions", "/openai/deployments/gpt4o/chat/completions"},
		{"/v1/", "/v1/messages", "/v1/messages"},
		{"/v1beta/openai", "/v1/chat/completions", "/v1beta/openai/chat/completions"},
		{"/openai/v1", "/v1/chat/completions", "/openai/v1/chat/completions"},
		{"/api/v1", "/v1/embeddings", "/api/v1/embeddings"},
		{"", "/v1/messages/count_tokens", "/v1/messages/count_tokens"},
		{"/v1", "/v1/messages/count_tokens", "/v1/messages/count_tokens"},
	}
	for _, c := range cases {
		if got := JoinUpstreamPath(c.base, c.in); got != c.want {
			t.Errorf("base %q + %q: want %q got %q", c.base, c.in, c.want, got)
		}
	}
}

func TestPrepareRequest_PreservesQueryAndStripsAcceptEncoding(t *testing.T) {
	p := NewAzureOpenAIProvider("key", "")
	base, _ := url.Parse("https://res.openai.azure.com/openai/deployments/gpt4o/chat/completions?api-version=2024-10-21")
	in := http.Header{}
	in.Set("Accept-Encoding", "gzip, br")
	in.Set("X-Run-ID", "r1")
	req, err := p.PrepareRequest(context.Background(), base, "/v1/chat/completions?foo=bar", in, []byte(`{}`))
	if err != nil {
		t.Fatal(err)
	}
	if req.URL.Path != "/openai/deployments/gpt4o/chat/completions" {
		t.Fatalf("azure path mangled: %s", req.URL.Path)
	}
	q := req.URL.Query()
	if q.Get("api-version") != "2024-10-21" || q.Get("foo") != "bar" {
		t.Fatalf("query not preserved/merged: %s", req.URL.RawQuery)
	}
	if req.Header.Get("Accept-Encoding") != "" {
		t.Fatalf("Accept-Encoding must not be forwarded")
	}
	if req.Header.Get("X-Run-ID") != "" {
		t.Fatalf("internal header leaked")
	}
	if req.Header.Get("api-key") != "key" {
		t.Fatalf("azure api-key not injected")
	}

	o := NewOpenAIProvider("sk-test", "")
	base2, _ := url.Parse("https://api.openai.com/v1")
	req2, _ := o.PrepareRequest(context.Background(), base2, "/v1/chat/completions", http.Header{}, []byte(`{}`))
	if req2.URL.String() != "https://api.openai.com/v1/chat/completions" {
		t.Fatalf("openai path doubled: %s", req2.URL.String())
	}
}

func TestPrepareRequest_ConfiguredKeyOverridesClientKey(t *testing.T) {
	a := NewAnthropicProvider("real-key", "")
	base, _ := url.Parse("https://api.anthropic.com")
	in := http.Header{}
	in.Set("x-api-key", "placeholder")
	in.Set("Authorization", "Bearer placeholder")
	req, err := a.PrepareRequest(context.Background(), base, "/v1/messages", in, []byte(`{}`))
	if err != nil {
		t.Fatal(err)
	}
	if req.Header.Get("x-api-key") != "real-key" {
		t.Fatalf("configured key must win, got %q", req.Header.Get("x-api-key"))
	}
	if req.Header.Get("Authorization") != "" {
		t.Fatalf("client Authorization must be dropped when proxy holds the key")
	}

	// No configured key: caller's credential passes through.
	g := NewGenericProvider("", "", "", "")
	req2, _ := g.PrepareRequest(context.Background(), base, "/v1/chat/completions", in, []byte(`{}`))
	if req2.Header.Get("Authorization") != "Bearer placeholder" {
		t.Fatalf("pass-through credential lost")
	}
}
