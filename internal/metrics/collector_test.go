package metrics

import (
	"strings"
	"testing"
	"time"

	"github.com/LinuxTerminali/aegisllm/internal/model"
)

func TestCollector_RecordAndSnapshot(t *testing.T) {
	c := NewCollector()

	c.RecordAccepted(model.PriorityProduction, 20*time.Millisecond)
	c.RecordAccepted(model.PriorityProduction, 40*time.Millisecond)
	c.RecordAccepted(model.PriorityBatch, 100*time.Millisecond)

	c.RecordCompleted(150*time.Millisecond, 100*time.Millisecond, 200)
	c.RecordFailed(500)
	c.RecordCanceled()
	c.RecordRejection("queue_full")
	c.RecordRejection("capacity_exceeded")
	c.RecordRetry()

	snap := c.Snapshot()
	if snap.RequestsAccepted != 3 {
		t.Errorf("expected 3 accepted, got %d", snap.RequestsAccepted)
	}
	if snap.RequestsCompleted != 1 {
		t.Errorf("expected 1 completed, got %d", snap.RequestsCompleted)
	}
	if snap.RequestsFailed != 1 {
		t.Errorf("expected 1 failed, got %d", snap.RequestsFailed)
	}
	if snap.RequestsCanceled != 1 {
		t.Errorf("expected 1 canceled, got %d", snap.RequestsCanceled)
	}
	if snap.RequestsRejected != 2 {
		t.Errorf("expected 2 rejected, got %d", snap.RequestsRejected)
	}
	if snap.RetriesTotal != 1 {
		t.Errorf("expected 1 retry, got %d", snap.RetriesTotal)
	}

	prodStats := snap.QueueWaitByClass["production"]
	if prodStats.Count != 2 {
		t.Errorf("expected 2 production requests, got %d", prodStats.Count)
	}
	if prodStats.MaxWaitMs != 40 {
		t.Errorf("expected max wait 40ms, got %d", prodStats.MaxWaitMs)
	}
	if prodStats.AvgWaitMs != 30.0 {
		t.Errorf("expected avg wait 30ms, got %f", prodStats.AvgWaitMs)
	}
}

func TestCollector_FormatPrometheus(t *testing.T) {
	c := NewCollector()
	c.RecordAccepted(model.PriorityProduction, 10*time.Millisecond)
	c.RecordCompleted(100*time.Millisecond, 90*time.Millisecond, 200)
	c.RecordUpstreamStatus(429)

	prom := c.FormatPrometheus(1, 5, 2, 100, 50.0, 60.0, 1000.0, 2000.0, 10, 2, 500, 480)

	requiredSnippets := []string{
		"scheduler_requests_total{status=\"accepted\"} 1",
		"scheduler_requests_total{status=\"completed\"} 1",
		"upstream_responses_total{code=\"429\"} 1",
		"scheduler_active_concurrency 1",
		"scheduler_max_concurrency 5",
		"scheduler_queued_requests 2",
		"scheduler_max_queue_size 100",
		"scheduler_capacity_requests_available 50.00",
		"scheduler_capacity_tokens_available 1000.00",
		"scheduler_queue_wait_ms_total{priority=\"production\"} 10",
	}

	for _, snippet := range requiredSnippets {
		if !strings.Contains(prom, snippet) {
			t.Errorf("expected Prometheus output to contain %q, but got:\n%s", snippet, prom)
		}
	}

	// Verify no unbounded labels
	if strings.Contains(prom, "run_id") || strings.Contains(prom, "prompt") || strings.Contains(prom, "api_key") {
		t.Errorf("Prometheus output must not contain unbounded labels!")
	}
}
