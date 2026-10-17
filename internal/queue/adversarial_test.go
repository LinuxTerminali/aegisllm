package queue

import (
	"context"
	"fmt"
	"runtime"
	"testing"

	"github.com/LinuxTerminali/aegisllm/internal/model"
)

// TestFairQueue_BoundedMemoryUnderManyRunIDs ensures that short-lived run IDs
// are pruned when empty, preventing unbounded memory growth.
func TestFairQueue_BoundedMemoryUnderManyRunIDs(t *testing.T) {
	const numRuns = 5000
	fq := NewFairQueue(100, nil)

	var m1, m2 runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&m1)

	// Sequentially enqueue and dequeue across 5000 distinct run IDs
	for i := 0; i < numRuns; i++ {
		runID := fmt.Sprintf("ephemeral-run-%d", i)
		meta := model.RequestMetadata{
			RequestID:     fmt.Sprintf("req-%d", i),
			RunID:         runID,
			WorkloadClass: "interactive",
			Priority:      model.PriorityInteractive,
		}
		item := NewItem(meta.RequestID, meta, context.Background())

		if err := fq.Enqueue(item); err != nil {
			t.Fatalf("failed to enqueue: %v", err)
		}

		dequeued := fq.Dequeue()
		if dequeued == nil || dequeued.ID != item.ID {
			t.Fatalf("expected item %s, got %v", item.ID, dequeued)
		}
	}

	runtime.GC()
	runtime.ReadMemStats(&m2)

	// Verify all internal state is pruned
	stats := fq.Stats()
	if stats.TotalQueued != 0 {
		t.Errorf("expected 0 queued items, got %d", stats.TotalQueued)
	}
	if stats.ActiveRunsCount != 0 {
		t.Errorf("expected 0 active runs, got %d", stats.ActiveRunsCount)
	}
	if len(fq.bands) != 0 {
		t.Errorf("expected 0 active bands, got %d", len(fq.bands))
	}
	if len(fq.itemsByID) != 0 {
		t.Errorf("expected 0 items in itemsByID, got %d", len(fq.itemsByID))
	}
}
