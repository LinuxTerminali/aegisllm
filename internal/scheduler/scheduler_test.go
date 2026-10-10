package scheduler

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/model-scheduler/model-scheduler/internal/model"
)

func TestScheduler_ConcurrencyLimit(t *testing.T) {
	maxConcurrency := 3
	sched := NewScheduler(Config{
		MaxConcurrency: maxConcurrency,
		MaxQueueSize:   20,
		MaxQueueWait:   5 * time.Second,
	})

	var activeCount atomic.Int32
	var peakActive atomic.Int32
	var wg sync.WaitGroup

	totalRequests := 15
	for i := 0; i < totalRequests; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			meta := model.RequestMetadata{
				RequestID: fmt.Sprintf("req-%d", id),
				Priority:  50,
			}
			lease, err := sched.Acquire(context.Background(), meta)
			if err != nil {
				t.Errorf("failed to acquire lease: %v", err)
				return
			}
			defer lease.Release()

			cur := activeCount.Add(1)
			defer activeCount.Add(-1)

			for {
				peak := peakActive.Load()
				if cur <= peak || peakActive.CompareAndSwap(peak, cur) {
					break
				}
			}

			// Simulate work
			time.Sleep(20 * time.Millisecond)
		}(i)
	}

	wg.Wait()

	if peakActive.Load() > int32(maxConcurrency) {
		t.Fatalf("peak active concurrency %d exceeded limit %d", peakActive.Load(), maxConcurrency)
	}

	stats := sched.Stats()
	if stats.ActiveRequests != 0 {
		t.Fatalf("expected 0 active requests at end, got %d", stats.ActiveRequests)
	}
	if stats.QueuedRequests != 0 {
		t.Fatalf("expected 0 queued requests at end, got %d", stats.QueuedRequests)
	}
	if stats.TotalCompleted != int64(totalRequests) {
		t.Fatalf("expected %d total completed, got %d", totalRequests, stats.TotalCompleted)
	}
}

func TestScheduler_PriorityOrder(t *testing.T) {
	// 1 concurrency slot, hold it so remaining requests queue up
	sched := NewScheduler(Config{
		MaxConcurrency: 1,
		MaxQueueSize:   10,
		MaxQueueWait:   5 * time.Second,
	})

	// Hold the slot
	blockerLease, err := sched.Acquire(context.Background(), model.RequestMetadata{RequestID: "blocker", Priority: 50})
	if err != nil {
		t.Fatalf("failed to acquire blocker: %v", err)
	}

	var executionOrder []string
	var orderMu sync.Mutex
	var wg sync.WaitGroup

	reqs := []struct {
		id       string
		priority int
	}{
		{"batch-1", model.PriorityBatch},             // 25
		{"interactive-1", model.PriorityInteractive}, // 75
		{"production-1", model.PriorityProduction},   // 100
		{"background-1", model.PriorityBackground},   // 50
	}

	for _, req := range reqs {
		wg.Add(1)
		go func(r struct {
			id       string
			priority int
		}) {
			defer wg.Done()
			lease, err := sched.Acquire(context.Background(), model.RequestMetadata{
				RequestID: r.id,
				Priority:  r.priority,
			})
			if err != nil {
				t.Errorf("failed to acquire %s: %v", r.id, err)
				return
			}
			defer lease.Release()

			orderMu.Lock()
			executionOrder = append(executionOrder, r.id)
			orderMu.Unlock()
		}(req)
		// Small sleep to ensure determinism in arrival order
		time.Sleep(10 * time.Millisecond)
	}

	// Release blocker so queued requests start executing by priority
	blockerLease.Release()
	wg.Wait()

	expected := []string{"production-1", "interactive-1", "background-1", "batch-1"}
	if len(executionOrder) != len(expected) {
		t.Fatalf("expected %d executions, got %d", len(expected), len(executionOrder))
	}
	for i, exp := range expected {
		if executionOrder[i] != exp {
			t.Errorf("step %d: expected %s, got %s", i, exp, executionOrder[i])
		}
	}
}

func TestScheduler_FIFOOrder(t *testing.T) {
	sched := NewScheduler(Config{
		MaxConcurrency: 1,
		MaxQueueSize:   10,
		MaxQueueWait:   5 * time.Second,
		Policy:         "fifo",
	})

	blockerLease, err := sched.Acquire(context.Background(), model.RequestMetadata{RequestID: "blocker", Priority: 50})
	if err != nil {
		t.Fatalf("failed to acquire blocker: %v", err)
	}

	var executionOrder []string
	var orderMu sync.Mutex
	var wg sync.WaitGroup

	reqs := []struct {
		id       string
		priority int
	}{
		{"batch-1", model.PriorityBatch},             // 25, arrives first
		{"production-1", model.PriorityProduction},   // 100, arrives second
		{"interactive-1", model.PriorityInteractive}, // 75, arrives third
	}

	for _, req := range reqs {
		wg.Add(1)
		go func(r struct {
			id       string
			priority int
		}) {
			defer wg.Done()
			lease, err := sched.Acquire(context.Background(), model.RequestMetadata{
				RequestID: r.id,
				Priority:  r.priority,
			})
			if err != nil {
				t.Errorf("failed to acquire %s: %v", r.id, err)
				return
			}
			defer lease.Release()

			orderMu.Lock()
			executionOrder = append(executionOrder, r.id)
			orderMu.Unlock()
		}(req)
		time.Sleep(10 * time.Millisecond)
	}

	blockerLease.Release()
	wg.Wait()

	expected := []string{"batch-1", "production-1", "interactive-1"}
	if len(executionOrder) != len(expected) {
		t.Fatalf("expected %d executions, got %d", len(expected), len(executionOrder))
	}
	for i, exp := range expected {
		if executionOrder[i] != exp {
			t.Errorf("step %d: expected %s, got %s", i, exp, executionOrder[i])
		}
	}
}

func TestScheduler_QueueCapacityRejection(t *testing.T) {
	sched := NewScheduler(Config{
		MaxConcurrency: 1,
		MaxQueueSize:   2,
		MaxQueueWait:   5 * time.Second,
	})

	// Hold the 1 concurrency slot
	blocker, err := sched.Acquire(context.Background(), model.RequestMetadata{RequestID: "b", Priority: 50})
	if err != nil {
		t.Fatalf("failed to acquire blocker: %v", err)
	}
	defer blocker.Release()

	// Fill queue with 2 requests
	var wg sync.WaitGroup
	for i := 1; i <= 2; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			lease, err := sched.Acquire(context.Background(), model.RequestMetadata{
				RequestID: fmt.Sprintf("queued-%d", id),
				Priority:  50,
			})
			if err != nil {
				return
			}
			defer lease.Release()
		}(i)
	}

	// Wait for the 2 requests to be enqueued
	time.Sleep(50 * time.Millisecond)
	if sched.Stats().QueuedRequests != 2 {
		t.Fatalf("expected 2 queued requests, got %d", sched.Stats().QueuedRequests)
	}

	// 3rd queued request should be rejected immediately with ErrQueueFull
	_, err = sched.Acquire(context.Background(), model.RequestMetadata{RequestID: "overflow", Priority: 100})
	if err != ErrQueueFull {
		t.Fatalf("expected ErrQueueFull, got %v", err)
	}

	if sched.Stats().TotalRejected != 1 {
		t.Fatalf("expected 1 rejected request, got %d", sched.Stats().TotalRejected)
	}

	blocker.Release()
	wg.Wait()
}

func TestScheduler_CancellationWhileQueued(t *testing.T) {
	sched := NewScheduler(Config{
		MaxConcurrency: 1,
		MaxQueueSize:   5,
		MaxQueueWait:   5 * time.Second,
	})

	// Hold concurrency slot
	blocker, _ := sched.Acquire(context.Background(), model.RequestMetadata{RequestID: "b", Priority: 50})
	defer blocker.Release()

	ctx, cancel := context.WithCancel(context.Background())

	errCh := make(chan error, 1)
	go func() {
		_, err := sched.Acquire(ctx, model.RequestMetadata{RequestID: "canceled-req", Priority: 50})
		errCh <- err
	}()

	time.Sleep(30 * time.Millisecond)
	if sched.Stats().QueuedRequests != 1 {
		t.Fatalf("expected 1 queued request, got %d", sched.Stats().QueuedRequests)
	}

	// Cancel context
	cancel()

	err := <-errCh
	if err != context.Canceled {
		t.Fatalf("expected context.Canceled, got %v", err)
	}

	if sched.Stats().QueuedRequests != 0 {
		t.Fatalf("expected queue to be empty after cancellation, got %d", sched.Stats().QueuedRequests)
	}
	if sched.Stats().TotalCanceled != 1 {
		t.Fatalf("expected 1 total canceled, got %d", sched.Stats().TotalCanceled)
	}
}

func TestScheduler_WaitTimeout(t *testing.T) {
	sched := NewScheduler(Config{
		MaxConcurrency: 1,
		MaxQueueSize:   5,
		MaxQueueWait:   50 * time.Millisecond,
	})

	// Hold concurrency slot
	blocker, _ := sched.Acquire(context.Background(), model.RequestMetadata{RequestID: "b", Priority: 50})
	defer blocker.Release()

	_, err := sched.Acquire(context.Background(), model.RequestMetadata{RequestID: "timeout-req", Priority: 50})
	if err != ErrWaitTimeout {
		t.Fatalf("expected ErrWaitTimeout, got %v", err)
	}

	if sched.Stats().TotalTimedOut != 1 {
		t.Fatalf("expected 1 total timed out, got %d", sched.Stats().TotalTimedOut)
	}
}
