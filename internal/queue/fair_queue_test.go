package queue

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/LinuxTerminali/aegisllm/internal/model"
)

func TestFairQueue_TwoRunsInSameClassInterleave(t *testing.T) {
	// 2 runs in same priority class (Production)
	fq := NewFairQueue(20, nil)

	// Run A submits 3 requests, Run B submits 3 requests
	for i := 1; i <= 3; i++ {
		_ = fq.Enqueue(NewItem(fmt.Sprintf("A-%d", i), model.RequestMetadata{RunID: "run-A", Priority: model.PriorityProduction}, context.Background()))
		_ = fq.Enqueue(NewItem(fmt.Sprintf("B-%d", i), model.RequestMetadata{RunID: "run-B", Priority: model.PriorityProduction}, context.Background()))
	}

	if fq.Len() != 6 {
		t.Fatalf("expected 6 queued items, got %d", fq.Len())
	}

	// Should dequeue alternating between Run A and Run B, in FIFO order within each run
	expected := []string{"A-1", "B-1", "A-2", "B-2", "A-3", "B-3"}
	for i, exp := range expected {
		item := fq.Dequeue()
		if item == nil {
			t.Fatalf("step %d: expected item %s, got nil", i, exp)
		}
		if item.ID != exp {
			t.Fatalf("step %d: expected %s, got %s", i, exp, item.ID)
		}
	}

	if fq.Len() != 0 {
		t.Fatalf("expected empty queue, got %d", fq.Len())
	}
}

func TestFairQueue_HighVolumeVsLowVolumeRun(t *testing.T) {
	// Run-High submits 10 requests, Run-Low submits 2 requests
	fq := NewFairQueue(30, nil)

	for i := 1; i <= 10; i++ {
		_ = fq.Enqueue(NewItem(fmt.Sprintf("H-%d", i), model.RequestMetadata{RunID: "run-high", Priority: model.PriorityProduction}, context.Background()))
	}
	for i := 1; i <= 2; i++ {
		_ = fq.Enqueue(NewItem(fmt.Sprintf("L-%d", i), model.RequestMetadata{RunID: "run-low", Priority: model.PriorityProduction}, context.Background()))
	}

	// Dequeue first 4 items:
	// Turn 1: H-1
	// Turn 2: L-1
	// Turn 3: H-2
	// Turn 4: L-2 (Run-Low is now completely finished!)
	expectedFirst4 := []string{"H-1", "L-1", "H-2", "L-2"}
	for i, exp := range expectedFirst4 {
		item := fq.Dequeue()
		if item == nil || item.ID != exp {
			t.Fatalf("step %d: expected %s, got %v", i, exp, item)
		}
	}

	// Remaining items must be all Run-High items (H-3 through H-10) in FIFO order
	for i := 3; i <= 10; i++ {
		item := fq.Dequeue()
		exp := fmt.Sprintf("H-%d", i)
		if item == nil || item.ID != exp {
			t.Fatalf("expected %s, got %v", exp, item)
		}
	}

	if fq.Dequeue() != nil {
		t.Fatalf("expected queue to be completely empty")
	}
}

func TestFairQueue_FIFOOrderWithinRun(t *testing.T) {
	fq := NewFairQueue(10, nil)

	for i := 1; i <= 5; i++ {
		_ = fq.Enqueue(NewItem(fmt.Sprintf("req-%d", i), model.RequestMetadata{RunID: "single-run", Priority: model.PriorityProduction}, context.Background()))
	}

	for i := 1; i <= 5; i++ {
		item := fq.Dequeue()
		exp := fmt.Sprintf("req-%d", i)
		if item == nil || item.ID != exp {
			t.Fatalf("step %d: expected %s, got %v", i, exp, item)
		}
	}
}

func TestFairQueue_PriorityWeights(t *testing.T) {
	// Configure weights: Production weight 2, Batch weight 1
	weights := map[int]int{
		model.PriorityProduction: 2,
		model.PriorityBatch:      1,
	}
	fq := NewFairQueue(20, weights)

	// Enqueue 4 Production items and 4 Batch items
	for i := 1; i <= 4; i++ {
		_ = fq.Enqueue(NewItem(fmt.Sprintf("P-%d", i), model.RequestMetadata{RunID: "run-p", Priority: model.PriorityProduction}, context.Background()))
		_ = fq.Enqueue(NewItem(fmt.Sprintf("B-%d", i), model.RequestMetadata{RunID: "run-b", Priority: model.PriorityBatch}, context.Background()))
	}

	// DRR with weights 2:1 should yield roughly: P, P, B, P, P, B...
	expected := []string{"P-1", "P-2", "B-1", "P-3", "P-4", "B-2"}
	for i, exp := range expected {
		item := fq.Dequeue()
		if item == nil || item.ID != exp {
			t.Fatalf("step %d: expected %s, got %v", i, exp, item)
		}
	}
}

func TestFairQueue_MissingRunID(t *testing.T) {
	fq := NewFairQueue(20, nil)

	// Run A has 2 requests
	_ = fq.Enqueue(NewItem("A-1", model.RequestMetadata{RunID: "run-A", Priority: model.PriorityProduction}, context.Background()))
	_ = fq.Enqueue(NewItem("A-2", model.RequestMetadata{RunID: "run-A", Priority: model.PriorityProduction}, context.Background()))

	// 2 untagged requests (missing Run ID)
	_ = fq.Enqueue(NewItem("anon-1", model.RequestMetadata{RunID: "", Priority: model.PriorityProduction}, context.Background()))
	_ = fq.Enqueue(NewItem("anon-2", model.RequestMetadata{RunID: "", Priority: model.PriorityProduction}, context.Background()))

	// Untagged requests participate as an isolated fair run and alternate with Run A
	expected := []string{"A-1", "anon-1", "A-2", "anon-2"}
	for i, exp := range expected {
		item := fq.Dequeue()
		if item == nil || item.ID != exp {
			t.Fatalf("step %d: expected %s, got %v", i, exp, item)
		}
	}
}

func TestFairQueue_CancellationAndCleanup(t *testing.T) {
	fq := NewFairQueue(10, nil)

	_ = fq.Enqueue(NewItem("A-1", model.RequestMetadata{RunID: "run-A", Priority: model.PriorityProduction}, context.Background()))
	_ = fq.Enqueue(NewItem("A-2", model.RequestMetadata{RunID: "run-A", Priority: model.PriorityProduction}, context.Background()))
	_ = fq.Enqueue(NewItem("B-1", model.RequestMetadata{RunID: "run-B", Priority: model.PriorityProduction}, context.Background()))

	if fq.Len() != 3 {
		t.Fatalf("expected 3 items, got %d", fq.Len())
	}

	// Cancel A-2
	removed, err := fq.Remove("A-2")
	if err != nil || removed.ID != "A-2" {
		t.Fatalf("failed to remove A-2: %v", err)
	}

	if fq.Len() != 2 {
		t.Fatalf("expected 2 items after cancellation, got %d", fq.Len())
	}

	// Cancel B-1 (Run B is now empty, should prune completely)
	removedB, err := fq.Remove("B-1")
	if err != nil || removedB.ID != "B-1" {
		t.Fatalf("failed to remove B-1: %v", err)
	}

	stats := fq.Stats()
	if stats.ActiveRunsCount != 1 {
		t.Fatalf("expected exactly 1 active run after B-1 cleanup, got %d", stats.ActiveRunsCount)
	}

	// Only A-1 remains
	item := fq.Dequeue()
	if item == nil || item.ID != "A-1" {
		t.Fatalf("expected A-1, got %v", item)
	}

	if fq.Len() != 0 {
		t.Fatalf("expected empty queue, got %d", fq.Len())
	}
	if fq.Stats().ActiveRunsCount != 0 {
		t.Fatalf("expected 0 active runs when empty, got %d", fq.Stats().ActiveRunsCount)
	}
}

func TestFairQueue_ConcurrentAccess(t *testing.T) {
	fq := NewFairQueue(500, nil)
	var wg sync.WaitGroup

	numProducers := 10
	itemsPerProducer := 20

	// Concurrent enqueues from multiple runs
	for p := 0; p < numProducers; p++ {
		wg.Add(1)
		go func(runIdx int) {
			defer wg.Done()
			runID := fmt.Sprintf("run-%d", runIdx)
			for i := 0; i < itemsPerProducer; i++ {
				_ = fq.Enqueue(NewItem(fmt.Sprintf("%s-item-%d", runID, i), model.RequestMetadata{RunID: runID, Priority: 50}, context.Background()))
			}
		}(p)
	}

	wg.Wait()

	expectedTotal := numProducers * itemsPerProducer
	if fq.Len() != expectedTotal {
		t.Fatalf("expected %d items, got %d", expectedTotal, fq.Len())
	}

	// Concurrent dequeues
	dequeuedCount := 0
	var countMu sync.Mutex
	for c := 0; c < 5; c++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				item := fq.Dequeue()
				if item == nil {
					return
				}
				countMu.Lock()
				dequeuedCount++
				countMu.Unlock()
			}
		}()
	}

	wg.Wait()

	if dequeuedCount != expectedTotal {
		t.Fatalf("expected %d dequeued items, got %d", expectedTotal, dequeuedCount)
	}
}
