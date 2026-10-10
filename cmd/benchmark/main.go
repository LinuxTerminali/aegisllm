package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
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

	"github.com/model-scheduler/model-scheduler/internal/config"
	"github.com/model-scheduler/model-scheduler/internal/model"
	"github.com/model-scheduler/model-scheduler/internal/queue"
	"github.com/model-scheduler/model-scheduler/internal/scheduler"
	"github.com/model-scheduler/model-scheduler/internal/server"
	"github.com/model-scheduler/model-scheduler/internal/upstream"
)

// WorkloadRequest defines a deterministic request in the test workload.
type WorkloadRequest struct {
	Index         int           `json:"index"`
	RequestID     string        `json:"request_id"`
	RunID         string        `json:"run_id"`
	WorkloadClass string        `json:"workload_class"`
	Priority      int           `json:"priority"`
	LaunchDelay   time.Duration `json:"launch_delay"`   // Offset from benchmark start
	Duration      time.Duration `json:"duration"`       // Simulated upstream processing time
	ClientTimeout time.Duration `json:"client_timeout"`  // Client deadline
}

// RequestResult records the outcome of a single request.
type RequestResult struct {
	Index         int
	RunID         string
	WorkloadClass string
	Priority      int
	Success       bool
	StatusCode    int
	ErrorCode     string
	Retries       int
	QueueWaitMs   float64
	DurationMs    float64
	IsTimeout     bool
	IsCanceled    bool
	IsRateLimited bool
	IsQueueFull   bool
}

// RunStats aggregates results for a single run ID.
type RunStats struct {
	TotalRequests int     `json:"total_requests"`
	Completed     int     `json:"completed"`
	Failed        int     `json:"failed"`
	SuccessRate   float64 `json:"success_rate_pct"`
	ServiceShare  float64 `json:"service_share_pct"`
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
	Mode               string                `json:"mode"`
	TotalSubmitted     int                   `json:"total_submitted"`
	Successful         int                   `json:"successful"`
	SuccessRatePct     float64               `json:"success_rate_pct"`
	HTTP429Count       int                   `json:"http_429_count"`
	HTTP503Count       int                   `json:"http_503_count"`
	HTTP504Count       int                   `json:"http_504_count"`
	CanceledCount      int                   `json:"canceled_count"`
	OtherFailures      int                   `json:"other_failures"`
	TotalRetries       int                   `json:"total_retries"`
	ThroughputRPS      float64               `json:"throughput_rps"`
	WallClockDurationMs float64              `json:"wall_clock_duration_ms"`
	P50LatencyMs       float64               `json:"p50_latency_ms"`
	P95LatencyMs       float64               `json:"p95_latency_ms"`
	P50WaitMs          float64               `json:"p50_wait_ms"`
	P95WaitMs          float64               `json:"p95_wait_ms"`
	Classes            map[string]ClassStats `json:"classes"`
	Runs               map[string]RunStats   `json:"runs"`
}

func main() {
	requestsCount := flag.Int("requests", 100, "Total requests in workload")
	seed := flag.Int64("seed", 42, "Random seed for reproducible workload")
	upstreamConc := flag.Int("upstream-concurrency", 4, "Upstream allowed concurrent in-flight limit")
	burstCount := flag.Int("burst", 40, "Number of initial burst requests")
	rateLimitRPS := flag.Float64("rate-limit-rps", 0, "Optional upstream token bucket rate limit RPS (0 = disabled)")
	rateLimitBurst := flag.Int("rate-limit-burst", 0, "Optional upstream token bucket burst capacity")
	jsonOut := flag.String("json-out", "", "Path to write JSON benchmark report")
	flag.Parse()

	fmt.Printf("================================================================================\n")
	fmt.Printf("LLM WORKLOAD SCHEDULER EXPERIMENTAL VALIDATION BENCHMARK\n")
	fmt.Printf("Workload: %d requests, Seed: %d, Upstream Concurrency: %d, Burst Size: %d, RateLimitRPS: %.1f\n",
		*requestsCount, *seed, *upstreamConc, *burstCount, *rateLimitRPS)
	fmt.Printf("================================================================================\n\n")

	// 1. Generate identical workload
	workload := generateWorkload(*requestsCount, *seed, *burstCount)

	// 2. Run Mode A: Direct requests with independent retries and jitter
	fmt.Printf(">>> Running Mode A: Direct Upstream Calls (with Retries & Jitter)...\n")
	reportA := runModeA(workload, *upstreamConc, *rateLimitRPS, *rateLimitBurst)

	// 3. Run Mode B: Scheduler with FIFO Ordering
	fmt.Printf(">>> Running Mode B: Bounded FIFO Scheduler...\n")
	reportB := runModeScheduler(workload, *upstreamConc, *rateLimitRPS, *rateLimitBurst, queue.PolicyFIFO, "B. Bounded FIFO Scheduler")

	// 4. Run Mode C: Scheduler with Priority Ordering
	fmt.Printf(">>> Running Mode C: Priority-Aware Scheduler...\n")
	reportC := runModeScheduler(workload, *upstreamConc, *rateLimitRPS, *rateLimitBurst, queue.PolicyPriority, "C. Priority-Aware Scheduler")

	// 5. Print Comparison Summary
	reports := []BenchmarkReport{reportA, reportB, reportC}
	printComparisonTable(reports)

	if *jsonOut != "" {
		data, err := json.MarshalIndent(reports, "", "  ")
		if err == nil {
			_ = os.WriteFile(*jsonOut, data, 0644)
			fmt.Printf("Wrote full JSON report to %s\n", *jsonOut)
		}
	}
}

// generateWorkload creates a reproducible slice of requests.
func generateWorkload(total int, seed int64, burstCount int) []WorkloadRequest {
	rng := rand.New(rand.NewSource(seed))

	runConfigs := []struct {
		runID         string
		class         string
		priority      int
		weight        int
		clientTimeout time.Duration
	}{
		{"run-prod-checkout", "production", model.PriorityProduction, 25, 600 * time.Millisecond},
		{"run-prod-assistant", "production", model.PriorityProduction, 20, 600 * time.Millisecond},
		{"run-interactive-qa", "interactive", model.PriorityInteractive, 25, 700 * time.Millisecond},
		{"run-bg-indexer", "background", model.PriorityBackground, 15, 1500 * time.Millisecond},
		{"run-batch-eval", "batch", model.PriorityBatch, 15, 2500 * time.Millisecond},
	}

	workload := make([]WorkloadRequest, total)

	for i := 0; i < total; i++ {
		// Pick run config by weight
		r := rng.Intn(100)
		cum := 0
		var chosen struct {
			runID         string
			class         string
			priority      int
			weight        int
			clientTimeout time.Duration
		}
		for _, rc := range runConfigs {
			cum += rc.weight
			if r < cum {
				chosen = rc
				break
			}
		}

		// Duration: 60% short (20-30ms), 40% long (80-120ms)
		var duration time.Duration
		if rng.Float64() < 0.60 {
			duration = time.Duration(20+rng.Intn(11)) * time.Millisecond
		} else {
			duration = time.Duration(80+rng.Intn(41)) * time.Millisecond
		}

		// Launch delay: First burstCount items launch within 0-50ms
		var launchDelay time.Duration
		if i < burstCount {
			launchDelay = time.Duration(rng.Intn(50)) * time.Millisecond
		} else {
			// Remaining items spaced out across 50ms to 800ms
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
		}
	}

	return workload
}

// runModeA tests direct upstream calls with bounded exponential backoff and jitter.
func runModeA(workload []WorkloadRequest, upstreamConcurrency int, rateLimitRPS float64, rateLimitBurst int) BenchmarkReport {
	// Create mock upstream with concurrency and rate limits
	mock := upstream.NewMockUpstream(upstream.MockConfig{
		MaxAllowedInFlight: upstreamConcurrency,
		RateLimitRPS:       rateLimitRPS,
		RateLimitBurst:     rateLimitBurst,
	})
	server := httptest.NewServer(mock)
	defer server.Close()

	results := make([]RequestResult, len(workload))
	var wg sync.WaitGroup

	startTime := time.Now()

	for i, req := range workload {
		wg.Add(1)
		go func(idx int, r WorkloadRequest) {
			defer wg.Done()

			// Wait for scheduled launch time
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
			var lastErr error
			success := false

			for attempt := 0; attempt <= maxRetries; attempt++ {
				if ctx.Err() != nil {
					lastErr = ctx.Err()
					break
				}

				if attempt > 0 {
					retries++
					// Bounded exponential backoff + jitter (+-25%)
					factor := math.Pow(2, float64(attempt-1))
					curBackoff := time.Duration(float64(baseBackoff) * factor)
					if curBackoff > maxBackoff {
						curBackoff = maxBackoff
					}
					jitter := (rng.Float64()*0.5 - 0.25) * float64(curBackoff)
					sleepDur := curBackoff + time.Duration(jitter)

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

				body := fmt.Sprintf(`{"model":"gpt-4o","messages":[{"role":"user","content":"%s"}]}`, r.RequestID)
				httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, server.URL+"/v1/chat/completions", bytes.NewBufferString(body))
				if err != nil {
					lastErr = err
					break
				}
				httpReq.Header.Set("Content-Type", "application/json")
				httpReq.Header.Set("X-Mock-Delay", r.Duration.String())

				resp, err := http.DefaultClient.Do(httpReq)
				if err != nil {
					lastErr = err
					break
				}

				lastStatusCode = resp.StatusCode
				_, _ = io.Copy(io.Discard, resp.Body)
				resp.Body.Close()

				if resp.StatusCode == http.StatusOK {
					success = true
					break
				} else if resp.StatusCode == http.StatusTooManyRequests {
					// 429: Retryable
					continue
				} else {
					// Other status: Permanent
					break
				}
			}

			dur := time.Since(reqStart)
			isCanceled := lastErr == context.Canceled || (lastErr == context.DeadlineExceeded)

			results[idx] = RequestResult{
				Index:         idx,
				RunID:         r.RunID,
				WorkloadClass: r.WorkloadClass,
				Priority:      r.Priority,
				Success:       success,
				StatusCode:    lastStatusCode,
				Retries:       retries,
				QueueWaitMs:   0, // Direct calls do not have a proxy queue
				DurationMs:    dur.Seconds() * 1000.0,
				IsTimeout:     lastErr == context.DeadlineExceeded,
				IsCanceled:    isCanceled,
				IsRateLimited: lastStatusCode == http.StatusTooManyRequests,
			}
		}(i, req)
	}

	wg.Wait()
	wallClock := time.Since(startTime)

	return analyzeResults("A. Direct + Retries & Jitter", results, wallClock)
}

// runModeScheduler runs workload against the scheduler with specified policy (FIFO or Priority).
func runModeScheduler(workload []WorkloadRequest, upstreamConcurrency int, rateLimitRPS float64, rateLimitBurst int, policy queue.SchedulingPolicy, modeName string) BenchmarkReport {
	// Create mock upstream (allows same concurrency limit)
	mock := upstream.NewMockUpstream(upstream.MockConfig{
		MaxAllowedInFlight: upstreamConcurrency,
		RateLimitRPS:       rateLimitRPS,
		RateLimitBurst:     rateLimitBurst,
	})
	mockServer := httptest.NewServer(mock)
	defer mockServer.Close()

	// Create Scheduler and Server
	cfg := config.ServerConfig{
		UpstreamURL:      mockServer.URL,
		MaxConcurrency:   upstreamConcurrency,
		MaxQueueSize:     50, // Bounded queue
		MaxQueueWait:     2 * time.Second,
		SchedulingPolicy: string(policy),
	}

	client, err := upstream.NewClient(upstream.ClientConfig{BaseURL: mockServer.URL})
	if err != nil {
		panic(err)
	}

	sched := scheduler.NewScheduler(scheduler.Config{
		MaxConcurrency: cfg.MaxConcurrency,
		MaxQueueSize:   cfg.MaxQueueSize,
		MaxQueueWait:   cfg.MaxQueueWait,
		Policy:         policy,
	})

	srv, err := server.NewServer(cfg, client, sched)
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
			httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, proxyServer.URL+"/v1/chat/completions", bytes.NewBufferString(body))
			if err != nil {
				return
			}
			httpReq.Header.Set("Content-Type", "application/json")
			httpReq.Header.Set("X-Run-ID", r.RunID)
			httpReq.Header.Set("X-Workload-Class", r.WorkloadClass)
			httpReq.Header.Set("X-Priority", strconv.Itoa(r.Priority))
			httpReq.Header.Set("X-Mock-Delay", r.Duration.String())

			resp, err := http.DefaultClient.Do(httpReq)
			dur := time.Since(reqStart)

			if err != nil {
				// Client context canceled / timed out before response
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
			_, _ = io.Copy(io.Discard, resp.Body)

			var waitMs float64
			if waitHeader := resp.Header.Get("X-Scheduler-Wait-Ms"); waitHeader != "" {
				if parsed, err := strconv.ParseFloat(waitHeader, 64); err == nil {
					waitMs = parsed
				}
			}

			success := (resp.StatusCode == http.StatusOK)
			results[idx] = RequestResult{
				Index:         idx,
				RunID:         r.RunID,
				WorkloadClass: r.WorkloadClass,
				Priority:      r.Priority,
				Success:       success,
				StatusCode:    resp.StatusCode,
				QueueWaitMs:   waitMs,
				DurationMs:    dur.Seconds() * 1000.0,
				IsRateLimited: resp.StatusCode == http.StatusTooManyRequests,
				IsQueueFull:   resp.StatusCode == http.StatusServiceUnavailable,
				IsTimeout:     resp.StatusCode == http.StatusGatewayTimeout,
			}
		}(i, req)
	}

	wg.Wait()
	wallClock := time.Since(startTime)

	return analyzeResults(modeName, results, wallClock)
}

func analyzeResults(mode string, results []RequestResult, wallClock time.Duration) BenchmarkReport {
	report := BenchmarkReport{
		Mode:                mode,
		TotalSubmitted:      len(results),
		WallClockDurationMs: wallClock.Seconds() * 1000.0,
		Classes:             make(map[string]ClassStats),
		Runs:                make(map[string]RunStats),
	}

	var allLatencies []float64
	var allWaitTimes []float64
	classLatencies := make(map[string][]float64)
	classWaitTimes := make(map[string][]float64)
	classCounts := make(map[string]int)
	classSuccesses := make(map[string]int)
	runCounts := make(map[string]int)
	runSuccesses := make(map[string]int)

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
		} else {
			if r.IsRateLimited {
				report.HTTP429Count++
			} else if r.IsQueueFull {
				report.HTTP503Count++
			} else if r.IsTimeout {
				report.HTTP504Count++
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

func printComparisonTable(reports []BenchmarkReport) {
	fmt.Printf("\n================================================================================\n")
	fmt.Printf("BENCHMARK RESULTS SUMMARY\n")
	fmt.Printf("================================================================================\n\n")

	fmt.Printf("%-32s | %-12s | %-12s | %-12s\n", "Metric", "Mode A (Direct)", "Mode B (FIFO)", "Mode C (Priority)")
	fmt.Println(strings.Repeat("-", 75))

	fmt.Printf("%-32s | %-12d | %-12d | %-12d\n", "Submitted Requests",
		reports[0].TotalSubmitted, reports[1].TotalSubmitted, reports[2].TotalSubmitted)
	fmt.Printf("%-32s | %-12d | %-12d | %-12d\n", "Successful Completed",
		reports[0].Successful, reports[1].Successful, reports[2].Successful)
	fmt.Printf("%-32s | %-11.1f%% | %-11.1f%% | %-11.1f%%\n", "Overall Success Rate",
		reports[0].SuccessRatePct, reports[1].SuccessRatePct, reports[2].SuccessRatePct)
	fmt.Printf("%-32s | %-12d | %-12d | %-12d\n", "HTTP 429 Responses",
		reports[0].HTTP429Count, reports[1].HTTP429Count, reports[2].HTTP429Count)
	fmt.Printf("%-32s | %-12d | %-12d | %-12d\n", "HTTP 503 (Queue Full)",
		reports[0].HTTP503Count, reports[1].HTTP503Count, reports[2].HTTP503Count)
	fmt.Printf("%-32s | %-12d | %-12d | %-12d\n", "Client Canceled / Deadline",
		reports[0].CanceledCount, reports[1].CanceledCount, reports[2].CanceledCount)
	fmt.Printf("%-32s | %-12d | %-12d | %-12d\n", "Client Retries Issued",
		reports[0].TotalRetries, reports[1].TotalRetries, reports[2].TotalRetries)
	fmt.Printf("%-32s | %-11.1f  | %-11.1f  | %-11.1f\n", "Throughput (req/s)",
		reports[0].ThroughputRPS, reports[1].ThroughputRPS, reports[2].ThroughputRPS)
	fmt.Printf("%-32s | %-11.1f  | %-11.1f  | %-11.1f\n", "Total Wall Clock (ms)",
		reports[0].WallClockDurationMs, reports[1].WallClockDurationMs, reports[2].WallClockDurationMs)

	fmt.Println(strings.Repeat("-", 75))
	fmt.Printf("%-32s | %-11.1f  | %-11.1f  | %-11.1f\n", "Latency p50 (ms)",
		reports[0].P50LatencyMs, reports[1].P50LatencyMs, reports[2].P50LatencyMs)
	fmt.Printf("%-32s | %-11.1f  | %-11.1f  | %-11.1f\n", "Latency p95 (ms)",
		reports[0].P95LatencyMs, reports[1].P95LatencyMs, reports[2].P95LatencyMs)
	fmt.Printf("%-32s | %-11.1f  | %-11.1f  | %-11.1f\n", "Queue Wait p50 (ms)",
		reports[0].P50WaitMs, reports[1].P50WaitMs, reports[2].P50WaitMs)
	fmt.Printf("%-32s | %-11.1f  | %-11.1f  | %-11.1f\n", "Queue Wait p95 (ms)",
		reports[0].P95WaitMs, reports[1].P95WaitMs, reports[2].P95WaitMs)

	// Workload Class Breakdown
	fmt.Println("\n" + strings.Repeat("=", 75))
	fmt.Println("BREAKDOWN BY WORKLOAD CLASS")
	fmt.Println(strings.Repeat("=", 75))
	classes := []string{"production", "interactive", "background", "batch"}
	for _, cls := range classes {
		fmt.Printf("\n[%s]\n", strings.ToUpper(cls))
		fmt.Printf("  Mode A (Direct):   %2d/%2d succ (%5.1f%%) | p50: %6.1fms | p95: %6.1fms\n",
			reports[0].Classes[cls].Completed, reports[0].Classes[cls].TotalRequests, reports[0].Classes[cls].SuccessRate,
			reports[0].Classes[cls].P50LatencyMs, reports[0].Classes[cls].P95LatencyMs)
		fmt.Printf("  Mode B (FIFO):     %2d/%2d succ (%5.1f%%) | p50: %6.1fms | p95: %6.1fms (p50 wait: %5.1fms)\n",
			reports[1].Classes[cls].Completed, reports[1].Classes[cls].TotalRequests, reports[1].Classes[cls].SuccessRate,
			reports[1].Classes[cls].P50LatencyMs, reports[1].Classes[cls].P95LatencyMs, reports[1].Classes[cls].P50WaitMs)
		fmt.Printf("  Mode C (Priority): %2d/%2d succ (%5.1f%%) | p50: %6.1fms | p95: %6.1fms (p50 wait: %5.1fms)\n",
			reports[2].Classes[cls].Completed, reports[2].Classes[cls].TotalRequests, reports[2].Classes[cls].SuccessRate,
			reports[2].Classes[cls].P50LatencyMs, reports[2].Classes[cls].P95LatencyMs, reports[2].Classes[cls].P50WaitMs)
	}

	// Per-Run Breakdown
	fmt.Println("\n" + strings.Repeat("=", 75))
	fmt.Println("PER-RUN SERVICE SHARE (Completed / Total -> % Share of Finished Work)")
	fmt.Println(strings.Repeat("=", 75))
	runs := []string{"run-prod-checkout", "run-prod-assistant", "run-interactive-qa", "run-bg-indexer", "run-batch-eval"}
	for _, r := range runs {
		fmt.Printf("%-20s:\n", r)
		fmt.Printf("  Mode A: %2d/%2d (%5.1f%% succ) -> %5.1f%% share\n",
			reports[0].Runs[r].Completed, reports[0].Runs[r].TotalRequests, reports[0].Runs[r].SuccessRate, reports[0].Runs[r].ServiceShare)
		fmt.Printf("  Mode B: %2d/%2d (%5.1f%% succ) -> %5.1f%% share\n",
			reports[1].Runs[r].Completed, reports[1].Runs[r].TotalRequests, reports[1].Runs[r].SuccessRate, reports[1].Runs[r].ServiceShare)
		fmt.Printf("  Mode C: %2d/%2d (%5.1f%% succ) -> %5.1f%% share\n",
			reports[2].Runs[r].Completed, reports[2].Runs[r].TotalRequests, reports[2].Runs[r].SuccessRate, reports[2].Runs[r].ServiceShare)
	}
	fmt.Printf("\n================================================================================\n")
}
