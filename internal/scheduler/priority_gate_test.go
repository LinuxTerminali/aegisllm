package scheduler

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/LinuxTerminali/aegisllm/internal/capacity"
	"github.com/LinuxTerminali/aegisllm/internal/model"
	"github.com/LinuxTerminali/aegisllm/internal/queue"
)

// A production request arriving in the middle of a batch burst must be admitted next,
// not after every batch request that was already waiting for quota. This is the
// behaviour the live Gemini run showed to be broken: slots were plentiful, so all
// requests lined up at the quota gate in arrival order and priority never applied.
func TestPriorityAppliesAtQuotaGate(t *testing.T) {
	tracker := capacity.NewDualTokenBucketTracker(capacity.Config{
		RequestsPerMinute: 300, // one request every 200ms
		RequestBurst:      1,
	})
	s := NewSchedulerWithDependencies(Config{
		MaxConcurrency: 32, // deliberately more than the workload: quota is the only bottleneck
		MaxQueueSize:   100,
		MaxQueueWait:   10 * time.Second,
		Policy:         queue.PolicyPriority,
	}, tracker, nil)
	defer s.Stop()

	var mu sync.Mutex
	var order []string
	admit := func(id string, priority int) {
		lease, err := s.Acquire(context.Background(), model.RequestMetadata{RequestID: id, RunID: id, Priority: priority}, []byte(`{"messages":[{"role":"user","content":"x"}]}`))
		if err != nil {
			t.Errorf("%s: %v", id, err)
			return
		}
		mu.Lock()
		order = append(order, id)
		mu.Unlock()
		lease.Reconcile(1) // a real call consumed the request; don't refund it
		lease.Release()
	}

	var wg sync.WaitGroup
	for i := 0; i < 6; i++ {
		wg.Add(1)
		id := "batch-" + string(rune('1'+i))
		go func() { defer wg.Done(); admit(id, 10) }()
		time.Sleep(5 * time.Millisecond) // deterministic arrival order
	}
	time.Sleep(100 * time.Millisecond) // burst is queued; first batch item is being paced
	wg.Add(1)
	go func() { defer wg.Done(); admit("prod-1", 100) }()
	wg.Wait()

	mu.Lock()
	defer mu.Unlock()
	if len(order) != 7 {
		t.Fatalf("expected 7 admissions, got %v", order)
	}
	// batch-1 is admitted immediately (burst 1). batch-2 may already be at the head being
	// paced when prod-1 arrives. prod-1 must then be no later than third.
	pos := -1
	for i, id := range order {
		if id == "prod-1" {
			pos = i
		}
	}
	if pos < 0 || pos > 2 {
		t.Fatalf("production request should be admitted 2nd or 3rd, got position %d in %v", pos+1, order)
	}
}
