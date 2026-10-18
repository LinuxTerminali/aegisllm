package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"math"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/LinuxTerminali/aegisllm/internal/config"
	"github.com/LinuxTerminali/aegisllm/internal/model"
	"github.com/LinuxTerminali/aegisllm/internal/queue"
	"github.com/LinuxTerminali/aegisllm/internal/server"
	"github.com/LinuxTerminali/aegisllm/internal/upstream"
)

// readErrorBody returns up to 300 bytes of a response body for diagnosis.
func readErrorBody(resp *http.Response) string {
	b, _ := io.ReadAll(io.LimitReader(resp.Body, 300))
	return strings.TrimSpace(string(b))
}

// realTarget, when set, makes every mode call a real provider instead of the embedded mock.
type realTarget struct {
	URL          string
	APIKey       string
	ProviderType string
	Model        string
}

var real *realTarget

var verbose bool

// endpointPath is the request path for the provider shape in use.
func (t *realTarget) endpointPath() string {
	if t.ProviderType == "anthropic" {
		return "/v1/messages"
	}
	return "/v1/chat/completions"
}

// requestBody builds the smallest valid request for the provider shape in use.
func (t *realTarget) requestBody(content string) string {
	return fmt.Sprintf(`{"model":%q,"max_tokens":5,"messages":[{"role":"user","content":%q}]}`, t.Model, content)
}

// WorkloadRequest defines a deterministic request in the test workload.
type WorkloadRequest struct {
	Index         int           `json:"index"`
	RequestID     string        `json:"request_id"`
	RunID         string        `json:"run_id"`
	WorkloadClass string        `json:"workload_class"`
	Priority      int           `json:"priority"`
	LaunchDelay   time.Duration `json:"launch_delay"`   // Offset from benchmark start
	Duration      time.Duration `json:"duration"`       // Simulated upstream processing time
	ClientTimeout time.Duration `json:"client_timeout"` // Client deadline
	Tokens        int           `json:"tokens"`         // Estimated/actual token consumption
}

// RequestResult records the outcome of a single request.
type RequestResult struct {
	Index          int
	RunID          string
	WorkloadClass  string
	Priority       int
	Success        bool
	StatusCode     int
	ErrorCode      string
	Retries        int
	QueueWaitMs    float64
	DurationMs     float64
	IsTimeout      bool
	IsCanceled     bool
	ErrorBody      string  // first bytes of a non-200 body, for diagnosis
	StartOffsetMs  float64 // when the client sent it, relative to the mode's start
	IsRateLimited  bool    // Upstream 429
	IsScheduler429 bool    // Scheduler 429 capacity_exceeded
	IsQueueFull    bool    // Scheduler 503 queue_full
	IsUpstream503  bool    // Upstream 503
}

// RunStats aggregates results for a single run ID.
type RunStats struct {
	TotalRequests int     `json:"total_requests"`
	Completed     int     `json:"completed"`
	Failed        int     `json:"failed"`
	SuccessRate   float64 `json:"success_rate_pct"`
	ServiceShare  float64 `json:"service_share_pct"`
	P50LatencyMs  float64 `json:"p50_latency_ms"`
	P95LatencyMs  float64 `json:"p95_latency_ms"`
}

// ClassStats aggregates results for a specific workload class.
type ClassStats struct {
	TotalRequests int     `json:"total_requests"`
	Completed     int     `json:"completed"`
	Failed        int     `json:"failed"`
	SuccessRate   float64 `json:"success_rate_pct"`
	P50LatencyMs  float64 `json:"p50_latency_ms"`
	P95LatencyMs  float64 `json:"p95_latency_ms"`
	P50WaitMs     float64 `json:"p50_wait_ms"`
	P95WaitMs     float64 `json:"p95_wait_ms"`
}

// BenchmarkReport summarizes the results of a benchmark mode.
type BenchmarkReport struct {
	Mode                string                `json:"mode"`
	TotalSubmitted      int                   `json:"total_submitted"`
	Successful          int                   `json:"successful"`
	SuccessRatePct      float64               `json:"success_rate_pct"`
	HTTP429Upstream     int                   `json:"http_429_upstream"`
	HTTP429Scheduler    int                   `json:"http_429_scheduler"`
	HTTP503QueueFull    int                   `json:"http_503_queue_full"`
	HTTP503Upstream     int                   `json:"http_503_upstream"`
	HTTP504Timeout      int                   `json:"http_504_timeout"`
	CanceledCount       int                   `json:"canceled_count"`
	OtherFailures       int                   `json:"other_failures"`
	StatusCounts        map[int]int           `json:"status_counts"`          // every non-200 status seen
	SampleError         string                `json:"sample_error,omitempty"` // first error body seen
	TotalRetries        int                   `json:"total_retries"`
	ThroughputRPS       float64               `json:"throughput_rps"`
	WallClockDurationMs float64               `json:"wall_clock_duration_ms"`
	P50LatencyMs        float64               `json:"p50_latency_ms"`
	P95LatencyMs        float64               `json:"p95_latency_ms"`
	P50WaitMs           float64               `json:"p50_wait_ms"`
	P95WaitMs           float64               `json:"p95_wait_ms"`
	JainsFairnessIndex  float64               `json:"jains_fairness_index"`
	Classes             map[string]ClassStats `json:"classes"`
	Runs                map[string]RunStats   `json:"runs"`
}

func main() {
	scenario := flag.String("scenario", "mixed-priorities", "Benchmark scenario: 'mixed-priorities', 'competing-runs', 'concurrency', 'rpm', 'tpm', 'cancellation', 'token-mix'")
	requestsCount := flag.Int("requests", 100, "Total requests in workload")
	seed := flag.Int64("seed", 42, "Random seed for reproducible workload")
	upstreamConc := flag.Int("upstream-concurrency", 4, "Upstream allowed concurrent in-flight limit")
	burstCount := flag.Int("burst", 40, "Number of initial burst requests")
	rateLimitRPS := flag.Float64("rate-limit-rps", 0, "Optional upstream token bucket rate limit RPS (0 = disabled)")
	rateLimitBurst := flag.Int("rate-limit-burst", 0, "Optional upstream token bucket burst capacity")
	rateLimitTPS := flag.Float64("rate-limit-tps", 0, "Optional upstream token bucket rate limit TPS (0 = disabled)")
	rateLimitTokenBurst := flag.Int("rate-limit-token-burst", 0, "Optional upstream token bucket token burst capacity")
	jsonOut := flag.String("json-out", "", "Path to write JSON benchmark report")
	clientTimeout := flag.Duration("client-timeout", 0, "Override per-request client deadline for every run (0 = scenario default)")
	autoLearn := flag.Bool("auto-learn", true, "Scheduler modes learn RPM/TPM from upstream headers instead of being configured with the limit")
	realURL := flag.String("upstream-url", "", "Run against a real provider at this base URL instead of the embedded mock (e.g. https://api.openai.com, https://api.anthropic.com, https://generativelanguage.googleapis.com/v1beta/openai)")
	realKey := flag.String("upstream-api-key", os.Getenv("BENCH_API_KEY"), "API key for the real provider (or BENCH_API_KEY)")
	realProvider := flag.String("provider-type", "openai", "Provider adapter for the real upstream: openai, anthropic, generic")
	realModel := flag.String("model", "", "Model to request from the real provider (e.g. gpt-4o-mini, claude-3-5-haiku-latest, gemini-2.5-flash)")
	verboseFlag := flag.Bool("verbose", false, "Keep the proxy's per-request logs and print a per-request timeline for every mode")
	modesFlag := flag.String("modes", "A,B,C,D", "Which modes to run, e.g. A,C. Skipped modes show as zeros in the table")
	cooldown := flag.Duration("cooldown", -1, "Pause between modes so a real provider's window refills (default 65s with -upstream-url, 0 for the mock)")
	flag.Parse()
	verbose = *verboseFlag

	if *cooldown < 0 {
		*cooldown = 0
		if *realURL != "" {
			*cooldown = 65 * time.Second
		}
	}
	wantMode := map[string]bool{}
	for _, m := range strings.Split(strings.ToUpper(*modesFlag), ",") {
		wantMode[strings.TrimSpace(m)] = true
	}

	if *realURL != "" {
		if *realModel == "" {
			fmt.Fprintln(os.Stderr, "-model is required with -upstream-url")
			os.Exit(2)
		}
		real = &realTarget{URL: *realURL, APIKey: *realKey, ProviderType: strings.ToLower(*realProvider), Model: *realModel}
		if *clientTimeout == 0 {
			*clientTimeout = 60 * time.Second
		}
		if *rateLimitRPS == 0 && !*autoLearn {
			fmt.Fprintln(os.Stderr, "with -auto-learn=false against a real provider, pass -rate-limit-rps (and -rate-limit-tps) so the scheduler knows the limit")
			os.Exit(2)
		}
		fmt.Fprintf(os.Stderr, "real provider: %s (%s, model %s). Four modes x %d requests = %d provider calls; keep -requests small on free tiers.\n",
			*realURL, real.ProviderType, real.Model, *requestsCount, 4**requestsCount)
	}
	if !verbose {
		log.SetOutput(io.Discard) // the proxy logs every request; keep benchmark output readable
	}

	// Apply scenario defaults if not explicitly overridden
	switch *scenario {
	case "rpm":
		if *rateLimitRPS == 0 {
			*rateLimitRPS = 15
			*rateLimitBurst = 5
		}
		// Agents wait for an answer; they do not give up in 600ms. Without this the workload is
		// infeasible for any scheduler (100 requests at 15 RPS need ~7s) and every mode scores ~30%.
		if *clientTimeout == 0 {
			*clientTimeout = 20 * time.Second
		}
	case "tpm":
		if *rateLimitTPS == 0 {
			*rateLimitTPS = 400
			*rateLimitTokenBurst = 200
		}
		if *clientTimeout == 0 {
			*clientTimeout = 20 * time.Second
		}
	}

	fmt.Printf("aegis benchmark: scenario=%s requests=%d seed=%d concurrency=%d burst=%d rpm=%.0f tpm=%.0f\n\n",
		*scenario, *requestsCount, *seed, *upstreamConc, *burstCount, *rateLimitRPS*60, *rateLimitTPS*60)

	// Generate identical workload
	workload := generateWorkload(*scenario, *requestsCount, *seed, *burstCount)
	if *clientTimeout > 0 {
		for i := range workload {
			workload[i].ClientTimeout = *clientTimeout
		}
	}
	schedKnowsLimits := !*autoLearn

	// Mode A: Direct requests with independent retries and jitter
	type modeDef struct {
		key, title, label string
		run               func() BenchmarkReport
	}
	modes := []modeDef{
		{"A", "mode A: direct calls with retries and jitter", "A. Direct + Retries & Jitter", func() BenchmarkReport {
			return runModeA(workload, *upstreamConc, *rateLimitRPS, *rateLimitBurst, *rateLimitTPS, *rateLimitTokenBurst)
		}},
		{"B", "mode B: FIFO scheduler", "B. Bounded FIFO Scheduler", func() BenchmarkReport {
			return runModeScheduler(workload, *upstreamConc, *rateLimitRPS, *rateLimitBurst, *rateLimitTPS, *rateLimitTokenBurst, queue.PolicyFIFO, "B. Bounded FIFO Scheduler", schedKnowsLimits)
		}},
		{"C", "mode C: priority scheduler", "C. Priority Scheduler", func() BenchmarkReport {
			return runModeScheduler(workload, *upstreamConc, *rateLimitRPS, *rateLimitBurst, *rateLimitTPS, *rateLimitTokenBurst, queue.PolicyPriority, "C. Priority Scheduler", schedKnowsLimits)
		}},
		{"D", "mode D: fair scheduler (per-run WDRR)", "D. Fair Scheduler (WDRR)", func() BenchmarkReport {
			return runModeScheduler(workload, *upstreamConc, *rateLimitRPS, *rateLimitBurst, *rateLimitTPS, *rateLimitTokenBurst, queue.PolicyFair, "D. Fair Scheduler (WDRR)", schedKnowsLimits)
		}},
	}

	var reports []BenchmarkReport
	ran := 0
	for _, m := range modes {
		if !wantMode[m.key] {
			reports = append(reports, BenchmarkReport{Mode: m.label + " (skipped)", Classes: map[string]ClassStats{}, Runs: map[string]RunStats{}, StatusCounts: map[int]int{}})
			continue
		}
		if ran > 0 && *cooldown > 0 {
			// Every mode must start from a full provider window, or it inherits the
			// previous mode's exhaustion and the comparison is meaningless.
			fmt.Printf("cooling down %s so the provider's window refills\n", cooldown.Round(time.Second))
			time.Sleep(*cooldown)
		}
		fmt.Printf("%s\n", m.title)
		reports = append(reports, m.run())
		ran++
	}

	printComparisonTable(reports)
	printFailureBreakdown(reports)

	if *jsonOut != "" {
		data, err := json.MarshalIndent(reports, "", "  ")
		if err == nil {
			_ = os.WriteFile(*jsonOut, data, 0644)
			fmt.Printf("Wrote full JSON report to %s\n", *jsonOut)
		}
	}
}

// generateWorkload creates a reproducible slice of requests for the requested scenario.
func generateWorkload(scenario string, total int, seed int64, burstCount int) []WorkloadRequest {
	rng := rand.New(rand.NewSource(seed))

	type runConfig struct {
		runID         string
		class         string
		priority      int
		weight        int
		clientTimeout time.Duration
	}

	var runConfigs []runConfig

	switch scenario {
	case "competing-runs":
		// Several runs share production class, but run-heavy submits 60% of requests
		runConfigs = []runConfig{
			{"run-prod-heavy", "production", model.PriorityProduction, 60, 800 * time.Millisecond},
			{"run-prod-light-1", "production", model.PriorityProduction, 20, 800 * time.Millisecond},
			{"run-prod-light-2", "production", model.PriorityProduction, 20, 800 * time.Millisecond},
		}
	case "cancellation":
		// Aggressive client timeouts to induce queue cancellations
		runConfigs = []runConfig{
			{"run-prod-checkout", "production", model.PriorityProduction, 35, 150 * time.Millisecond},
			{"run-interactive-qa", "interactive", model.PriorityInteractive, 35, 200 * time.Millisecond},
			{"run-batch-eval", "batch", model.PriorityBatch, 30, 350 * time.Millisecond},
		}
	default:
		// Standard mixed workload across priorities
		runConfigs = []runConfig{
			{"run-prod-checkout", "production", model.PriorityProduction, 25, 600 * time.Millisecond},
			{"run-prod-assistant", "production", model.PriorityProduction, 20, 600 * time.Millisecond},
			{"run-interactive-qa", "interactive", model.PriorityInteractive, 25, 700 * time.Millisecond},
			{"run-bg-indexer", "background", model.PriorityBackground, 15, 1500 * time.Millisecond},
			{"run-batch-eval", "batch", model.PriorityBatch, 15, 2500 * time.Millisecond},
		}
	}

	workload := make([]WorkloadRequest, total)

	for i := 0; i < total; i++ {
		r := rng.Intn(100)
		cum := 0
		var chosen runConfig
		for _, rc := range runConfigs {
			cum += rc.weight
			if r < cum {
				chosen = rc
				break
			}
		}

		var duration time.Duration
		var tokenCount int
		if scenario == "token-mix" {
			if rng.Float64() < 0.70 {
				duration = time.Duration(15+rng.Intn(11)) * time.Millisecond
				tokenCount = 25
			} else {
				duration = time.Duration(120+rng.Intn(50)) * time.Millisecond
				tokenCount = 350
			}
		} else {
			if rng.Float64() < 0.60 {
				duration = time.Duration(20+rng.Intn(11)) * time.Millisecond
				tokenCount = 30
			} else {
				duration = time.Duration(80+rng.Intn(41)) * time.Millisecond
				tokenCount = 120
			}
		}

		var launchDelay time.Duration
		if i < burstCount {
			launchDelay = time.Duration(rng.Intn(50)) * time.Millisecond
		} else {
			launchDelay = time.Duration(50+rng.Intn(750)) * time.Millisecond
		}

		workload[i] = WorkloadRequest{
			Index:         i,
			RequestID:     fmt.Sprintf("req-%03d", i),
			RunID:         chosen.runID,
			WorkloadClass: chosen.class,
			Priority:      chosen.priority,
			LaunchDelay:   launchDelay,
			Duration:      duration,
			ClientTimeout: chosen.clientTimeout,
			Tokens:        tokenCount,
		}
	}

	return workload
}

// runModeA tests direct upstream calls with bounded exponential backoff and jitter.
func runModeA(workload []WorkloadRequest, upstreamConcurrency int, rateLimitRPS float64, rateLimitBurst int, rateLimitTPS float64, rateLimitTokenBurst int) BenchmarkReport {
	mock := upstream.NewMockUpstream(upstream.MockConfig{
		MaxAllowedInFlight:  upstreamConcurrency,
		RateLimitRPS:        rateLimitRPS,
		RateLimitBurst:      rateLimitBurst,
		RateLimitTPS:        rateLimitTPS,
		RateLimitTokenBurst: rateLimitTokenBurst,
	})
	server := httptest.NewServer(mock)
	defer server.Close()

	var direct *upstream.Client
	if real != nil {
		var err error
		direct, err = upstream.NewClient(upstream.ClientConfig{BaseURL: real.URL, APIKey: real.APIKey, ProviderType: real.ProviderType, Timeout: 60 * time.Second})
		if err != nil {
			panic(err)
		}
	}

	results := make([]RequestResult, len(workload))
	var wg sync.WaitGroup

	startTime := time.Now()

	for i, req := range workload {
		wg.Add(1)
		go func(idx int, r WorkloadRequest) {
			defer wg.Done()

			if r.LaunchDelay > 0 {
				time.Sleep(r.LaunchDelay)
			}

			reqStart := time.Now()
			ctx, cancel := context.WithTimeout(context.Background(), r.ClientTimeout)
			defer cancel()

			maxRetries := 3
			baseBackoff := 25 * time.Millisecond
			maxBackoff := 250 * time.Millisecond
			rng := rand.New(rand.NewSource(int64(idx * 7919)))

			retries := 0
			var lastStatusCode int
			var lastErrBody string
			var lastErr error
			var lastRetryAfter time.Duration
			success := false

			for attempt := 0; attempt <= maxRetries; attempt++ {
				if ctx.Err() != nil {
					lastErr = ctx.Err()
					break
				}

				if attempt > 0 {
					retries++
					var sleepDur time.Duration
					if lastRetryAfter > 0 {
						sleepDur = lastRetryAfter
						lastRetryAfter = 0
					} else {
						factor := math.Pow(2, float64(attempt-1))
						curBackoff := time.Duration(float64(baseBackoff) * factor)
						if curBackoff > maxBackoff {
							curBackoff = maxBackoff
						}
						jitter := (rng.Float64()*0.5 - 0.25) * float64(curBackoff)
						sleepDur = curBackoff + time.Duration(jitter)
					}

					select {
					case <-time.After(sleepDur):
					case <-ctx.Done():
						lastErr = ctx.Err()
						break
					}
					if ctx.Err() != nil {
						lastErr = ctx.Err()
						break
					}
				}

				var resp *http.Response
				var err error
				if real != nil {
					resp, err = direct.Forward(ctx, real.endpointPath(), http.Header{}, []byte(real.requestBody(r.RequestID)))
				} else {
					body := fmt.Sprintf(`{"model":"gpt-4o","messages":[{"role":"user","content":"%s"}]}`, r.RequestID)
					var httpReq *http.Request
					httpReq, err = http.NewRequestWithContext(ctx, http.MethodPost, server.URL+"/v1/chat/completions", bytes.NewBufferString(body))
					if err != nil {
						lastErr = err
						break
					}
					httpReq.Header.Set("Content-Type", "application/json")
					httpReq.Header.Set("X-Mock-Delay", r.Duration.String())
					resp, err = http.DefaultClient.Do(httpReq)
				}
				if err != nil {
					lastErr = err
					break
				}

				lastStatusCode = resp.StatusCode
				if resp.StatusCode != http.StatusOK {
					lastErrBody = readErrorBody(resp)
				}
				_, _ = io.Copy(io.Discard, resp.Body)
				resp.Body.Close()

				if resp.StatusCode == http.StatusOK {
					success = true
					break
				} else if resp.StatusCode == http.StatusTooManyRequests {
					if ra, ok := upstream.ParseRetryAfter(resp.Header.Get("Retry-After")); ok && ra > 0 {
						lastRetryAfter = ra
					}
					continue
				} else {
					break
				}
			}

			dur := time.Since(reqStart)
			isCanceled := lastErr == context.Canceled || (lastErr == context.DeadlineExceeded)
			if lastErr != nil && lastErrBody == "" {
				lastErrBody = lastErr.Error()
			}

			results[idx] = RequestResult{
				Index:         idx,
				RunID:         r.RunID,
				WorkloadClass: r.WorkloadClass,
				Priority:      r.Priority,
				Success:       success,
				StatusCode:    lastStatusCode,
				StartOffsetMs: reqStart.Sub(startTime).Seconds() * 1000.0,
				Retries:       retries,
				QueueWaitMs:   0,
				DurationMs:    dur.Seconds() * 1000.0,
				IsTimeout:     lastErr == context.DeadlineExceeded,
				IsCanceled:    isCanceled,
				IsRateLimited: lastStatusCode == http.StatusTooManyRequests,
				IsUpstream503: lastStatusCode == http.StatusServiceUnavailable,
				ErrorBody:     lastErrBody,
			}
		}(i, req)
	}

	wg.Wait()
	wallClock := time.Since(startTime)

	return analyzeResults("A. Direct + Retries & Jitter", results, wallClock)
}

// runModeScheduler runs workload against the scheduler with specified policy.
func runModeScheduler(workload []WorkloadRequest, upstreamConcurrency int, rateLimitRPS float64, rateLimitBurst int, rateLimitTPS float64, rateLimitTokenBurst int, policy queue.SchedulingPolicy, modeName string, knowsLimits bool) BenchmarkReport {
	mock := upstream.NewMockUpstream(upstream.MockConfig{
		MaxAllowedInFlight:  upstreamConcurrency,
		RateLimitRPS:        rateLimitRPS,
		RateLimitBurst:      rateLimitBurst,
		RateLimitTPS:        rateLimitTPS,
		RateLimitTokenBurst: rateLimitTokenBurst,
	})
	mockServer := httptest.NewServer(mock)
	defer mockServer.Close()

	maxClientTimeout := 2 * time.Second
	for _, r := range workload {
		if r.ClientTimeout > maxClientTimeout {
			maxClientTimeout = r.ClientTimeout
		}
	}

	upstreamURL := mockServer.URL
	providerType := ""
	apiKey := ""
	if real != nil {
		upstreamURL, providerType, apiKey = real.URL, real.ProviderType, real.APIKey
	}

	cfg := config.ServerConfig{
		ListenAddr:        "127.0.0.1:0",
		UpstreamURL:       upstreamURL,
		ProviderType:      providerType,
		UpstreamAPIKey:    apiKey,
		UpstreamTimeout:   60 * time.Second,
		MaxConcurrency:    upstreamConcurrency,
		MaxQueueSize:      len(workload),
		MaxQueueWait:      maxClientTimeout,
		SchedulingPolicy:  string(policy),
		AutoLearnLimits:   !knowsLimits,
		MaxPacingDelay:    0, // wait for quota up to the queue deadline
		MaxRetries:        3,
		InitialBackoff:    25 * time.Millisecond,
		MaxBackoff:        250 * time.Millisecond,
		WeightProduction:  8,
		WeightInteractive: 4,
		WeightBackground:  2,
		WeightBatch:       1,
	}
	if knowsLimits {
		cfg.RequestsPerMinute = rateLimitRPS * 60
		cfg.RequestBurst = rateLimitBurst
		cfg.TokensPerMinute = rateLimitTPS * 60
		cfg.TokenBurst = rateLimitTokenBurst
	}

	client, err := upstream.NewClient(upstream.ClientConfig{BaseURL: upstreamURL, ProviderType: providerType, APIKey: apiKey, Timeout: 60 * time.Second})
	if err != nil {
		panic(err)
	}

	srv, err := server.NewServer(cfg, client, nil)
	if err != nil {
		panic(err)
	}

	proxyServer := httptest.NewServer(srv.Handler())
	defer proxyServer.Close()

	results := make([]RequestResult, len(workload))
	var wg sync.WaitGroup

	startTime := time.Now()

	for i, req := range workload {
		wg.Add(1)
		go func(idx int, r WorkloadRequest) {
			defer wg.Done()

			if r.LaunchDelay > 0 {
				time.Sleep(r.LaunchDelay)
			}

			reqStart := time.Now()
			ctx, cancel := context.WithTimeout(context.Background(), r.ClientTimeout)
			defer cancel()

			body := fmt.Sprintf(`{"model":"gpt-4o","messages":[{"role":"user","content":"%s"}]}`, r.RequestID)
			path := "/v1/chat/completions"
			if real != nil {
				body, path = real.requestBody(r.RequestID), real.endpointPath()
			}
			httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, proxyServer.URL+path, bytes.NewBufferString(body))
			if err != nil {
				return
			}
			httpReq.Header.Set("Content-Type", "application/json")
			httpReq.Header.Set("X-Run-ID", r.RunID)
			httpReq.Header.Set("X-Workload-Class", r.WorkloadClass)
			httpReq.Header.Set("X-Priority", strconv.Itoa(r.Priority))
			if real == nil {
				httpReq.Header.Set("X-Mock-Delay", r.Duration.String())
			}

			resp, err := http.DefaultClient.Do(httpReq)
			dur := time.Since(reqStart)

			if err != nil {
				isTimeout := ctx.Err() == context.DeadlineExceeded
				results[idx] = RequestResult{
					Index:         idx,
					RunID:         r.RunID,
					WorkloadClass: r.WorkloadClass,
					Priority:      r.Priority,
					Success:       false,
					StatusCode:    0,
					DurationMs:    dur.Seconds() * 1000.0,
					IsTimeout:     isTimeout,
					IsCanceled:    true,
				}
				return
			}
			defer resp.Body.Close()
			var errBody string
			if resp.StatusCode != http.StatusOK {
				errBody = readErrorBody(resp)
			}
			_, _ = io.Copy(io.Discard, resp.Body)

			var waitMs float64
			if waitHeader := resp.Header.Get("X-Scheduler-Wait-Ms"); waitHeader != "" {
				if parsed, err := strconv.ParseFloat(waitHeader, 64); err == nil {
					waitMs = parsed
				}
			}

			var retries int
			if retriesHeader := resp.Header.Get("X-Scheduler-Retries"); retriesHeader != "" {
				if parsed, err := strconv.Atoi(retriesHeader); err == nil {
					retries = parsed
				}
			}

			schedErr := resp.Header.Get("X-Scheduler-Error")

			success := (resp.StatusCode == http.StatusOK)
			results[idx] = RequestResult{
				Index:          idx,
				RunID:          r.RunID,
				WorkloadClass:  r.WorkloadClass,
				Priority:       r.Priority,
				Success:        success,
				StatusCode:     resp.StatusCode,
				StartOffsetMs:  reqStart.Sub(startTime).Seconds() * 1000.0,
				Retries:        retries,
				QueueWaitMs:    waitMs,
				DurationMs:     dur.Seconds() * 1000.0,
				IsRateLimited:  resp.StatusCode == http.StatusTooManyRequests && schedErr == "",
				IsScheduler429: resp.StatusCode == http.StatusTooManyRequests && schedErr == "capacity_exceeded",
				IsQueueFull:    resp.StatusCode == http.StatusServiceUnavailable && schedErr == "queue_full",
				IsUpstream503:  resp.StatusCode == http.StatusServiceUnavailable && schedErr == "",
				IsTimeout:      resp.StatusCode == http.StatusGatewayTimeout || schedErr == "queue_timeout",
				ErrorBody:      errBody,
			}
		}(i, req)
	}

	wg.Wait()
	wallClock := time.Since(startTime)

	return analyzeResults(modeName, results, wallClock)
}

// printTimeline shows every request in completion order: when it was sent, how long it
// waited in the scheduler, when it finished, with what status, and how many retries.
func printTimeline(mode string, results []RequestResult) {
	sorted := make([]RequestResult, len(results))
	copy(sorted, results)
	sort.Slice(sorted, func(i, j int) bool {
		return sorted[i].StartOffsetMs+sorted[i].DurationMs < sorted[j].StartOffsetMs+sorted[j].DurationMs
	})
	fmt.Printf("\n%s timeline (sent -> done, ms from mode start)\n", mode)
	fmt.Printf("%-4s %-12s %-20s %7s %8s %8s %6s %7s %s\n", "#", "class", "run", "sent", "wait", "done", "status", "retries", "note")
	for _, r := range sorted {
		note := ""
		if !r.Success && r.ErrorBody != "" {
			note = r.ErrorBody
			if len(note) > 60 {
				note = note[:60] + "..."
			}
		}
		fmt.Printf("%-4d %-12s %-20s %7.0f %8.0f %8.0f %6d %7d %s\n",
			r.Index, r.WorkloadClass, r.RunID, r.StartOffsetMs, r.QueueWaitMs, r.StartOffsetMs+r.DurationMs, r.StatusCode, r.Retries, note)
	}
}

func analyzeResults(mode string, results []RequestResult, wallClock time.Duration) BenchmarkReport {
	if verbose {
		printTimeline(mode, results)
	}
	report := BenchmarkReport{
		Mode:                mode,
		TotalSubmitted:      len(results),
		WallClockDurationMs: wallClock.Seconds() * 1000.0,
		Classes:             make(map[string]ClassStats),
		Runs:                make(map[string]RunStats),
		StatusCounts:        make(map[int]int),
	}

	var allLatencies []float64
	var allWaitTimes []float64
	classLatencies := make(map[string][]float64)
	classWaitTimes := make(map[string][]float64)
	classCounts := make(map[string]int)
	classSuccesses := make(map[string]int)
	runCounts := make(map[string]int)
	runSuccesses := make(map[string]int)
	runLatencies := make(map[string][]float64)

	for _, r := range results {
		report.TotalRetries += r.Retries
		classCounts[r.WorkloadClass]++
		runCounts[r.RunID]++

		if r.Success {
			report.Successful++
			classSuccesses[r.WorkloadClass]++
			runSuccesses[r.RunID]++
			allLatencies = append(allLatencies, r.DurationMs)
			allWaitTimes = append(allWaitTimes, r.QueueWaitMs)
			classLatencies[r.WorkloadClass] = append(classLatencies[r.WorkloadClass], r.DurationMs)
			classWaitTimes[r.WorkloadClass] = append(classWaitTimes[r.WorkloadClass], r.QueueWaitMs)
			runLatencies[r.RunID] = append(runLatencies[r.RunID], r.DurationMs)
		} else {
			report.StatusCounts[r.StatusCode]++
			if report.SampleError == "" && r.ErrorBody != "" {
				report.SampleError = fmt.Sprintf("HTTP %d: %s", r.StatusCode, r.ErrorBody)
			}
			if r.IsRateLimited {
				report.HTTP429Upstream++
			} else if r.IsScheduler429 {
				report.HTTP429Scheduler++
			} else if r.IsQueueFull {
				report.HTTP503QueueFull++
			} else if r.IsUpstream503 {
				report.HTTP503Upstream++
			} else if r.IsTimeout {
				report.HTTP504Timeout++
			} else if r.IsCanceled {
				report.CanceledCount++
			} else {
				report.OtherFailures++
			}
		}
	}

	if report.TotalSubmitted > 0 {
		report.SuccessRatePct = (float64(report.Successful) / float64(report.TotalSubmitted)) * 100.0
	}
	if wallClock.Seconds() > 0 {
		report.ThroughputRPS = float64(report.Successful) / wallClock.Seconds()
	}

	report.P50LatencyMs = percentile(allLatencies, 50)
	report.P95LatencyMs = percentile(allLatencies, 95)
	report.P50WaitMs = percentile(allWaitTimes, 50)
	report.P95WaitMs = percentile(allWaitTimes, 95)

	// Jain's Fairness Index across all active runs:
	// J = (sum(x_i))^2 / (n * sum(x_i^2))
	var sumCompletions float64
	var sumSquares float64
	nRuns := 0
	for _, c := range runCounts {
		if c > 0 {
			nRuns++
		}
	}
	for _, succ := range runSuccesses {
		s := float64(succ)
		sumCompletions += s
		sumSquares += s * s
	}
	if nRuns > 0 && sumSquares > 0 {
		report.JainsFairnessIndex = (sumCompletions * sumCompletions) / (float64(nRuns) * sumSquares)
	}

	// Class breakdown
	for cls, count := range classCounts {
		succ := classSuccesses[cls]
		rate := 0.0
		if count > 0 {
			rate = (float64(succ) / float64(count)) * 100.0
		}
		lats := classLatencies[cls]
		waits := classWaitTimes[cls]
		report.Classes[cls] = ClassStats{
			TotalRequests: count,
			Completed:     succ,
			Failed:        count - succ,
			SuccessRate:   rate,
			P50LatencyMs:  percentile(lats, 50),
			P95LatencyMs:  percentile(lats, 95),
			P50WaitMs:     percentile(waits, 50),
			P95WaitMs:     percentile(waits, 95),
		}
	}

	// Run breakdown
	for rID, count := range runCounts {
		succ := runSuccesses[rID]
		rate := 0.0
		share := 0.0
		if count > 0 {
			rate = (float64(succ) / float64(count)) * 100.0
		}
		if report.Successful > 0 {
			share = (float64(succ) / float64(report.Successful)) * 100.0
		}
		report.Runs[rID] = RunStats{
			TotalRequests: count,
			Completed:     succ,
			Failed:        count - succ,
			SuccessRate:   rate,
			ServiceShare:  share,
			P50LatencyMs:  percentile(runLatencies[rID], 50),
			P95LatencyMs:  percentile(runLatencies[rID], 95),
		}
	}

	return report
}

func percentile(values []float64, pct float64) float64 {
	if len(values) == 0 {
		return 0.0
	}
	sorted := make([]float64, len(values))
	copy(sorted, values)
	sort.Float64s(sorted)

	index := int(math.Ceil(pct/100.0*float64(len(sorted)))) - 1
	if index < 0 {
		index = 0
	}
	if index >= len(sorted) {
		index = len(sorted) - 1
	}
	return sorted[index]
}

// printFailureBreakdown never lets a failure disappear into a zero: every non-200 status
// (0 = transport error) per mode, with one example body.
func printFailureBreakdown(reports []BenchmarkReport) {
	for _, rep := range reports {
		if len(rep.StatusCounts) == 0 {
			continue
		}
		var parts []string
		for code, n := range rep.StatusCounts {
			parts = append(parts, fmt.Sprintf("%d x%d", code, n))
		}
		sort.Strings(parts)
		fmt.Printf("\n%s: non-200 statuses: %s\n", rep.Mode, strings.Join(parts, ", "))
		if rep.SampleError != "" {
			fmt.Printf("  example: %s\n", rep.SampleError)
		}
	}
}

func printComparisonTable(reports []BenchmarkReport) {
	fmt.Printf("results\n\n")

	fmt.Printf("%-32s | %-12s | %-12s | %-12s | %-12s\n", "Metric", "Mode A (Dir)", "Mode B (FIFO)", "Mode C (Prio)", "Mode D (Fair)")
	fmt.Println(strings.Repeat("-", 95))

	fmt.Printf("%-32s | %-12d | %-12d | %-12d | %-12d\n", "Submitted Requests",
		reports[0].TotalSubmitted, reports[1].TotalSubmitted, reports[2].TotalSubmitted, reports[3].TotalSubmitted)
	fmt.Printf("%-32s | %-12d | %-12d | %-12d | %-12d\n", "Successful Completed",
		reports[0].Successful, reports[1].Successful, reports[2].Successful, reports[3].Successful)
	fmt.Printf("%-32s | %-11.1f%% | %-11.1f%% | %-11.1f%% | %-11.1f%%\n", "Overall Success Rate",
		reports[0].SuccessRatePct, reports[1].SuccessRatePct, reports[2].SuccessRatePct, reports[3].SuccessRatePct)
	fmt.Printf("%-32s | %-12d | %-12d | %-12d | %-12d\n", "Upstream 429",
		reports[0].HTTP429Upstream, reports[1].HTTP429Upstream, reports[2].HTTP429Upstream, reports[3].HTTP429Upstream)
	fmt.Printf("%-32s | %-12d | %-12d | %-12d | %-12d\n", "Scheduler 429 (Capacity)",
		reports[0].HTTP429Scheduler, reports[1].HTTP429Scheduler, reports[2].HTTP429Scheduler, reports[3].HTTP429Scheduler)
	fmt.Printf("%-32s | %-12d | %-12d | %-12d | %-12d\n", "Scheduler 503 (Queue Full)",
		reports[0].HTTP503QueueFull, reports[1].HTTP503QueueFull, reports[2].HTTP503QueueFull, reports[3].HTTP503QueueFull)
	fmt.Printf("%-32s | %-12d | %-12d | %-12d | %-12d\n", "Queue Timeout (504)",
		reports[0].HTTP504Timeout, reports[1].HTTP504Timeout, reports[2].HTTP504Timeout, reports[3].HTTP504Timeout)
	fmt.Printf("%-32s | %-12d | %-12d | %-12d | %-12d\n", "Client Canceled / Deadline",
		reports[0].CanceledCount, reports[1].CanceledCount, reports[2].CanceledCount, reports[3].CanceledCount)
	fmt.Printf("%-32s | %-12d | %-12d | %-12d | %-12d\n", "Other Failures (unclassified)",
		reports[0].OtherFailures, reports[1].OtherFailures, reports[2].OtherFailures, reports[3].OtherFailures)
	fmt.Printf("%-32s | %-12d | %-12d | %-12d | %-12d\n", "Retries Issued",
		reports[0].TotalRetries, reports[1].TotalRetries, reports[2].TotalRetries, reports[3].TotalRetries)
	fmt.Printf("%-32s | %-11.1f  | %-11.1f  | %-11.1f  | %-11.1f\n", "Throughput (req/s)",
		reports[0].ThroughputRPS, reports[1].ThroughputRPS, reports[2].ThroughputRPS, reports[3].ThroughputRPS)
	fmt.Printf("%-32s | %-11.1f  | %-11.1f  | %-11.1f  | %-11.1f\n", "Total Wall Clock (ms)",
		reports[0].WallClockDurationMs, reports[1].WallClockDurationMs, reports[2].WallClockDurationMs, reports[3].WallClockDurationMs)

	fmt.Println(strings.Repeat("-", 95))
	fmt.Printf("%-32s | %-11.1f  | %-11.1f  | %-11.1f  | %-11.1f\n", "Latency p50 (ms)",
		reports[0].P50LatencyMs, reports[1].P50LatencyMs, reports[2].P50LatencyMs, reports[3].P50LatencyMs)
	fmt.Printf("%-32s | %-11.1f  | %-11.1f  | %-11.1f  | %-11.1f\n", "Latency p95 (ms)",
		reports[0].P95LatencyMs, reports[1].P95LatencyMs, reports[2].P95LatencyMs, reports[3].P95LatencyMs)
	fmt.Printf("%-32s | %-11.1f  | %-11.1f  | %-11.1f  | %-11.1f\n", "Queue Wait p50 (ms)",
		reports[0].P50WaitMs, reports[1].P50WaitMs, reports[2].P50WaitMs, reports[3].P50WaitMs)
	fmt.Printf("%-32s | %-11.1f  | %-11.1f  | %-11.1f  | %-11.1f\n", "Queue Wait p95 (ms)",
		reports[0].P95WaitMs, reports[1].P95WaitMs, reports[2].P95WaitMs, reports[3].P95WaitMs)
	fmt.Printf("%-32s | %-11.3f  | %-11.3f  | %-11.3f  | %-11.3f\n", "Jain's Fairness Index",
		reports[0].JainsFairnessIndex, reports[1].JainsFairnessIndex, reports[2].JainsFairnessIndex, reports[3].JainsFairnessIndex)

	// Workload Class Breakdown
	fmt.Println("\n" + strings.Repeat("=", 95))
	fmt.Println("BREAKDOWN BY WORKLOAD CLASS")
	fmt.Println(strings.Repeat("=", 95))
	classes := []string{"production", "interactive", "background", "batch"}
	for _, cls := range classes {
		if reports[0].Classes[cls].TotalRequests == 0 && reports[3].Classes[cls].TotalRequests == 0 {
			continue
		}
		fmt.Printf("\n[%s]\n", strings.ToUpper(cls))
		fmt.Printf("  Mode A (Direct): %2d/%2d succ (%5.1f%%) | p50: %6.1fms | p95: %6.1fms\n",
			reports[0].Classes[cls].Completed, reports[0].Classes[cls].TotalRequests, reports[0].Classes[cls].SuccessRate,
			reports[0].Classes[cls].P50LatencyMs, reports[0].Classes[cls].P95LatencyMs)
		fmt.Printf("  Mode B (FIFO):   %2d/%2d succ (%5.1f%%) | p50: %6.1fms | p95: %6.1fms (p50 wait: %5.1fms)\n",
			reports[1].Classes[cls].Completed, reports[1].Classes[cls].TotalRequests, reports[1].Classes[cls].SuccessRate,
			reports[1].Classes[cls].P50LatencyMs, reports[1].Classes[cls].P95LatencyMs, reports[1].Classes[cls].P50WaitMs)
		fmt.Printf("  Mode C (Prio):   %2d/%2d succ (%5.1f%%) | p50: %6.1fms | p95: %6.1fms (p50 wait: %5.1fms)\n",
			reports[2].Classes[cls].Completed, reports[2].Classes[cls].TotalRequests, reports[2].Classes[cls].SuccessRate,
			reports[2].Classes[cls].P50LatencyMs, reports[2].Classes[cls].P95LatencyMs, reports[2].Classes[cls].P50WaitMs)
		fmt.Printf("  Mode D (Fair):   %2d/%2d succ (%5.1f%%) | p50: %6.1fms | p95: %6.1fms (p50 wait: %5.1fms)\n",
			reports[3].Classes[cls].Completed, reports[3].Classes[cls].TotalRequests, reports[3].Classes[cls].SuccessRate,
			reports[3].Classes[cls].P50LatencyMs, reports[3].Classes[cls].P95LatencyMs, reports[3].Classes[cls].P50WaitMs)
	}

	// Per-Run Breakdown
	fmt.Println("\n" + strings.Repeat("=", 95))
	fmt.Println("PER-RUN SERVICE SHARE (Completed / Total -> % Share of Finished Work)")
	fmt.Println(strings.Repeat("=", 95))
	var allRuns []string
	for r := range reports[3].Runs {
		allRuns = append(allRuns, r)
	}
	sort.Strings(allRuns)
	for _, r := range allRuns {
		fmt.Printf("%-20s:\n", r)
		fmt.Printf("  Mode A: %2d/%2d (%5.1f%%) -> %5.1f%% share | p50: %5.1fms\n",
			reports[0].Runs[r].Completed, reports[0].Runs[r].TotalRequests, reports[0].Runs[r].SuccessRate, reports[0].Runs[r].ServiceShare, reports[0].Runs[r].P50LatencyMs)
		fmt.Printf("  Mode B: %2d/%2d (%5.1f%%) -> %5.1f%% share | p50: %5.1fms\n",
			reports[1].Runs[r].Completed, reports[1].Runs[r].TotalRequests, reports[1].Runs[r].SuccessRate, reports[1].Runs[r].ServiceShare, reports[1].Runs[r].P50LatencyMs)
		fmt.Printf("  Mode C: %2d/%2d (%5.1f%%) -> %5.1f%% share | p50: %5.1fms\n",
			reports[2].Runs[r].Completed, reports[2].Runs[r].TotalRequests, reports[2].Runs[r].SuccessRate, reports[2].Runs[r].ServiceShare, reports[2].Runs[r].P50LatencyMs)
		fmt.Printf("  Mode D: %2d/%2d (%5.1f%%) -> %5.1f%% share | p50: %5.1fms\n",
			reports[3].Runs[r].Completed, reports[3].Runs[r].TotalRequests, reports[3].Runs[r].SuccessRate, reports[3].Runs[r].ServiceShare, reports[3].Runs[r].P50LatencyMs)
	}
}
