package metrics

import (
	"fmt"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/LinuxTerminali/aegisllm/internal/model"
)

// ClassName converts a numeric priority to standard class name for bounded metric labels.
func ClassName(priority int) string {
	switch priority {
	case model.PriorityProduction:
		return "production"
	case model.PriorityInteractive:
		return "interactive"
	case model.PriorityBackground:
		return "background"
	case model.PriorityBatch:
		return "batch"
	default:
		return "other"
	}
}

// PriorityLatencyStats tracks duration totals and counts for a priority class.
type PriorityLatencyStats struct {
	Count       int64   `json:"count"`
	TotalWaitMs int64   `json:"total_wait_ms"`
	AvgWaitMs   float64 `json:"avg_wait_ms"`
	MaxWaitMs   int64   `json:"max_wait_ms"`
}

// Collector records bounded operational metrics.
type Collector struct {
	mu sync.RWMutex

	// Request counters
	requestsAccepted  atomic.Int64
	requestsCompleted atomic.Int64
	requestsFailed    atomic.Int64
	requestsCanceled  atomic.Int64
	requestsRejected  atomic.Int64

	// Rejections by reason (bounded)
	rejectionsQueueFull atomic.Int64
	rejectionsCapacity  atomic.Int64
	rejectionsTimeout   atomic.Int64

	// Retries
	retriesTotal atomic.Int64

	// Upstream status codes (bounded)
	status2xx atomic.Int64
	status4xx atomic.Int64
	status5xx atomic.Int64
	status429 atomic.Int64
	status502 atomic.Int64
	status503 atomic.Int64
	status504 atomic.Int64

	// Latency tracking
	totalRequestDurationMs  atomic.Int64
	totalUpstreamDurationMs atomic.Int64
	completedRequestsCount  atomic.Int64

	// Per-class queue wait tracking
	classWaitStats map[string]*PriorityLatencyStats
}

// NewCollector creates an initialized metrics collector.
func NewCollector() *Collector {
	c := &Collector{
		classWaitStats: make(map[string]*PriorityLatencyStats),
	}
	for _, class := range []string{"production", "interactive", "background", "batch", "other"} {
		c.classWaitStats[class] = &PriorityLatencyStats{}
	}
	return c
}

// RecordAccepted increments admitted requests.
func (c *Collector) RecordAccepted(priority int, waitDur time.Duration) {
	c.requestsAccepted.Add(1)

	class := ClassName(priority)
	waitMs := waitDur.Milliseconds()

	c.mu.Lock()
	stats, ok := c.classWaitStats[class]
	if !ok {
		stats = &PriorityLatencyStats{}
		c.classWaitStats[class] = stats
	}
	stats.Count++
	stats.TotalWaitMs += waitMs
	if waitMs > stats.MaxWaitMs {
		stats.MaxWaitMs = waitMs
	}
	if stats.Count > 0 {
		stats.AvgWaitMs = float64(stats.TotalWaitMs) / float64(stats.Count)
	}
	c.mu.Unlock()
}

// RecordCompleted records successful request completion and execution durations.
func (c *Collector) RecordCompleted(totalDur, upstreamDur time.Duration, statusCode int) {
	c.requestsCompleted.Add(1)
	c.totalRequestDurationMs.Add(totalDur.Milliseconds())
	c.totalUpstreamDurationMs.Add(upstreamDur.Milliseconds())
	c.completedRequestsCount.Add(1)

	c.RecordUpstreamStatus(statusCode)
}

// RecordFailed records request failure.
func (c *Collector) RecordFailed(statusCode int) {
	c.requestsFailed.Add(1)
	c.RecordUpstreamStatus(statusCode)
}

// RecordCanceled records client context cancellation.
func (c *Collector) RecordCanceled() {
	c.requestsCanceled.Add(1)
}

// RecordRejection records admission control rejection by reason.
func (c *Collector) RecordRejection(reason string) {
	c.requestsRejected.Add(1)
	switch reason {
	case "queue_full":
		c.rejectionsQueueFull.Add(1)
	case "capacity_exceeded":
		c.rejectionsCapacity.Add(1)
	case "queue_timeout":
		c.rejectionsTimeout.Add(1)
	}
}

// RecordRetry increments retries counter.
func (c *Collector) RecordRetry() {
	c.retriesTotal.Add(1)
}

// RecordUpstreamStatus records upstream HTTP status code counters.
func (c *Collector) RecordUpstreamStatus(code int) {
	if code >= 200 && code < 300 {
		c.status2xx.Add(1)
	} else if code >= 400 && code < 500 {
		c.status4xx.Add(1)
	} else if code >= 500 {
		c.status5xx.Add(1)
	}

	switch code {
	case http.StatusTooManyRequests:
		c.status429.Add(1)
	case http.StatusBadGateway:
		c.status502.Add(1)
	case http.StatusServiceUnavailable:
		c.status503.Add(1)
	case http.StatusGatewayTimeout:
		c.status504.Add(1)
	}
}

// StatsSnapshot captures all collected metrics for JSON export.
type StatsSnapshot struct {
	RequestsAccepted    int64                           `json:"requests_accepted"`
	RequestsCompleted   int64                           `json:"requests_completed"`
	RequestsFailed      int64                           `json:"requests_failed"`
	RequestsCanceled    int64                           `json:"requests_canceled"`
	RequestsRejected    int64                           `json:"requests_rejected"`
	RejectionsByReason  map[string]int64                `json:"rejections_by_reason"`
	RetriesTotal        int64                           `json:"retries_total"`
	UpstreamStatusClass map[string]int64                `json:"upstream_status_class"`
	UpstreamSpecific4xx map[string]int64                `json:"upstream_specific_status"`
	AvgRequestMs        float64                         `json:"avg_request_duration_ms"`
	AvgUpstreamMs       float64                         `json:"avg_upstream_duration_ms"`
	QueueWaitByClass    map[string]PriorityLatencyStats `json:"queue_wait_by_class"`
}

// Snapshot returns a copy of current operational metrics.
func (c *Collector) Snapshot() StatsSnapshot {
	c.mu.RLock()
	defer c.mu.RUnlock()

	waitByClass := make(map[string]PriorityLatencyStats, len(c.classWaitStats))
	for k, v := range c.classWaitStats {
		waitByClass[k] = *v
	}

	var avgReq, avgUpstream float64
	compCount := c.completedRequestsCount.Load()
	if compCount > 0 {
		avgReq = float64(c.totalRequestDurationMs.Load()) / float64(compCount)
		avgUpstream = float64(c.totalUpstreamDurationMs.Load()) / float64(compCount)
	}

	return StatsSnapshot{
		RequestsAccepted:  c.requestsAccepted.Load(),
		RequestsCompleted: c.requestsCompleted.Load(),
		RequestsFailed:    c.requestsFailed.Load(),
		RequestsCanceled:  c.requestsCanceled.Load(),
		RequestsRejected:  c.requestsRejected.Load(),
		RejectionsByReason: map[string]int64{
			"queue_full":        c.rejectionsQueueFull.Load(),
			"capacity_exceeded": c.rejectionsCapacity.Load(),
			"queue_timeout":     c.rejectionsTimeout.Load(),
		},
		RetriesTotal: c.retriesTotal.Load(),
		UpstreamStatusClass: map[string]int64{
			"2xx": c.status2xx.Load(),
			"4xx": c.status4xx.Load(),
			"5xx": c.status5xx.Load(),
		},
		UpstreamSpecific4xx: map[string]int64{
			"429": c.status429.Load(),
			"502": c.status502.Load(),
			"503": c.status503.Load(),
			"504": c.status504.Load(),
		},
		AvgRequestMs:     avgReq,
		AvgUpstreamMs:    avgUpstream,
		QueueWaitByClass: waitByClass,
	}
}

// FormatPrometheus generates Prometheus text exposition format metrics.
// Cardinality is strictly bounded: only fixed priority classes, status classes, and reasons are used.
func (c *Collector) FormatPrometheus(activeConcurrency, maxConcurrency, queuedRequests, maxQueue int,
	availReq, maxReq, availTok, maxTok float64, totalReservations, totalPaced, totalTokensReserved, totalTokensReconciled int64) string {
	var sb strings.Builder

	// Scheduler requests
	sb.WriteString("# HELP scheduler_requests_total Total requests processed by status.\n")
	sb.WriteString("# TYPE scheduler_requests_total counter\n")
	fmt.Fprintf(&sb, "scheduler_requests_total{status=\"accepted\"} %d\n", c.requestsAccepted.Load())
	fmt.Fprintf(&sb, "scheduler_requests_total{status=\"completed\"} %d\n", c.requestsCompleted.Load())
	fmt.Fprintf(&sb, "scheduler_requests_total{status=\"failed\"} %d\n", c.requestsFailed.Load())
	fmt.Fprintf(&sb, "scheduler_requests_total{status=\"canceled\"} %d\n", c.requestsCanceled.Load())
	fmt.Fprintf(&sb, "scheduler_requests_total{status=\"rejected\"} %d\n", c.requestsRejected.Load())

	// Rejections by reason
	sb.WriteString("# HELP scheduler_rejections_total Total scheduler admission rejections by reason.\n")
	sb.WriteString("# TYPE scheduler_rejections_total counter\n")
	fmt.Fprintf(&sb, "scheduler_rejections_total{reason=\"queue_full\"} %d\n", c.rejectionsQueueFull.Load())
	fmt.Fprintf(&sb, "scheduler_rejections_total{reason=\"capacity_exceeded\"} %d\n", c.rejectionsCapacity.Load())
	fmt.Fprintf(&sb, "scheduler_rejections_total{reason=\"queue_timeout\"} %d\n", c.rejectionsTimeout.Load())

	// Retries
	sb.WriteString("# HELP scheduler_retries_total Total coordinated retry attempts to upstream.\n")
	sb.WriteString("# TYPE scheduler_retries_total counter\n")
	fmt.Fprintf(&sb, "scheduler_retries_total %d\n", c.retriesTotal.Load())

	// Upstream status class
	sb.WriteString("# HELP upstream_responses_total Total responses from upstream provider by status class.\n")
	sb.WriteString("# TYPE upstream_responses_total counter\n")
	fmt.Fprintf(&sb, "upstream_responses_total{status_class=\"2xx\"} %d\n", c.status2xx.Load())
	fmt.Fprintf(&sb, "upstream_responses_total{status_class=\"4xx\"} %d\n", c.status4xx.Load())
	fmt.Fprintf(&sb, "upstream_responses_total{status_class=\"5xx\"} %d\n", c.status5xx.Load())
	fmt.Fprintf(&sb, "upstream_responses_total{code=\"429\"} %d\n", c.status429.Load())
	fmt.Fprintf(&sb, "upstream_responses_total{code=\"502\"} %d\n", c.status502.Load())
	fmt.Fprintf(&sb, "upstream_responses_total{code=\"503\"} %d\n", c.status503.Load())
	fmt.Fprintf(&sb, "upstream_responses_total{code=\"504\"} %d\n", c.status504.Load())

	// Concurrency & Queues
	sb.WriteString("# HELP scheduler_active_concurrency Current in-flight requests executing upstream.\n")
	sb.WriteString("# TYPE scheduler_active_concurrency gauge\n")
	fmt.Fprintf(&sb, "scheduler_active_concurrency %d\n", activeConcurrency)
	sb.WriteString("# HELP scheduler_max_concurrency Configured maximum concurrent requests.\n")
	sb.WriteString("# TYPE scheduler_max_concurrency gauge\n")
	fmt.Fprintf(&sb, "scheduler_max_concurrency %d\n", maxConcurrency)

	sb.WriteString("# HELP scheduler_queued_requests Current requests waiting in admission queue.\n")
	sb.WriteString("# TYPE scheduler_queued_requests gauge\n")
	fmt.Fprintf(&sb, "scheduler_queued_requests %d\n", queuedRequests)
	sb.WriteString("# HELP scheduler_max_queue_size Configured maximum admission queue capacity.\n")
	sb.WriteString("# TYPE scheduler_max_queue_size gauge\n")
	fmt.Fprintf(&sb, "scheduler_max_queue_size %d\n", maxQueue)

	// Capacity tracking
	sb.WriteString("# HELP scheduler_capacity_requests_available Available request quota in local token bucket.\n")
	sb.WriteString("# TYPE scheduler_capacity_requests_available gauge\n")
	fmt.Fprintf(&sb, "scheduler_capacity_requests_available %.2f\n", availReq)
	sb.WriteString("# HELP scheduler_capacity_tokens_available Available token quota in local token bucket.\n")
	sb.WriteString("# TYPE scheduler_capacity_tokens_available gauge\n")
	fmt.Fprintf(&sb, "scheduler_capacity_tokens_available %.2f\n", availTok)

	sb.WriteString("# HELP scheduler_capacity_reservations_total Total capacity reservations initiated.\n")
	sb.WriteString("# TYPE scheduler_capacity_reservations_total counter\n")
	fmt.Fprintf(&sb, "scheduler_capacity_reservations_total %d\n", totalReservations)
	sb.WriteString("# HELP scheduler_capacity_paced_requests_total Total requests paused for rate limit pacing.\n")
	sb.WriteString("# TYPE scheduler_capacity_paced_requests_total counter\n")
	fmt.Fprintf(&sb, "scheduler_capacity_paced_requests_total %d\n", totalPaced)
	sb.WriteString("# HELP scheduler_capacity_tokens_reserved_total Cumulative tokens reserved.\n")
	sb.WriteString("# TYPE scheduler_capacity_tokens_reserved_total counter\n")
	fmt.Fprintf(&sb, "scheduler_capacity_tokens_reserved_total %d\n", totalTokensReserved)
	sb.WriteString("# HELP scheduler_capacity_tokens_reconciled_total Cumulative tokens reconciled from upstream.\n")
	sb.WriteString("# TYPE scheduler_capacity_tokens_reconciled_total counter\n")
	fmt.Fprintf(&sb, "scheduler_capacity_tokens_reconciled_total %d\n", totalTokensReconciled)

	// Queue wait by priority class
	c.mu.RLock()
	sb.WriteString("# HELP scheduler_queue_wait_ms_total Cumulative queue wait time in milliseconds by priority class.\n")
	sb.WriteString("# TYPE scheduler_queue_wait_ms_total counter\n")
	for class, st := range c.classWaitStats {
		fmt.Fprintf(&sb, "scheduler_queue_wait_ms_total{priority=\"%s\"} %d\n", class, st.TotalWaitMs)
	}
	sb.WriteString("# HELP scheduler_queue_wait_count_total Requests waited in queue by priority class.\n")
	sb.WriteString("# TYPE scheduler_queue_wait_count_total counter\n")
	for class, st := range c.classWaitStats {
		fmt.Fprintf(&sb, "scheduler_queue_wait_count_total{priority=\"%s\"} %d\n", class, st.Count)
	}
	c.mu.RUnlock()

	return sb.String()
}
