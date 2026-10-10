package queue

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/model-scheduler/model-scheduler/internal/model"
)

func TestPriorityQueue_OrderAndTieBreaker(t *testing.T) {
	pq := NewPriorityQueue(10)

	// Create items with varying priorities and same priorities
	items := []*Item{
		NewItem("batch-1", model.RequestMetadata{RunID: "run-1", Priority: model.PriorityBatch}, context.Background()),
		NewItem("interactive-1", model.RequestMetadata{RunID: "run-2", Priority: model.PriorityInteractive}, context.Background()),
		NewItem("production-1", model.RequestMetadata{RunID: "run-3", Priority: model.PriorityProduction}, context.Background()),
		NewItem("interactive-2", model.RequestMetadata{RunID: "run-4", Priority: model.PriorityInteractive}, context.Background()),
		NewItem("background-1", model.RequestMetadata{RunID: "run-5", Priority: model.PriorityBackground}, context.Background()),
		NewItem("production-2", model.RequestMetadata{RunID: "run-6", Priority: model.PriorityProduction}, context.Background()),
	}

	for _, it := range items {
		if err := pq.Enqueue(it); err != nil {
			t.Fatalf("failed to enqueue item %s: %v", it.ID, err)
		}
	}

	expectedOrder := []string{
		"production-1",  // Priority 100, arrived first
		"production-2",  // Priority 100, arrived second
		"interactive-1", // Priority 75, arrived first among 75s
		"interactive-2", // Priority 75, arrived second among 75s
		"background-1",  // Priority 50
		"batch-1",       // Priority 25
	}

	for i, expectedID := range expectedOrder {
		dequeued := pq.Dequeue()
		if dequeued == nil {
			t.Fatalf("step %d: expected item %s, got nil", i, expectedID)
		}
		if dequeued.ID != expectedID {
			t.Fatalf("step %d: expected item %s, got %s (priority %d)", i, expectedID, dequeued.ID, dequeued.Meta.Priority)
		}
	}

	if extra := pq.Dequeue(); extra != nil {
		t.Fatalf("expected empty queue, got %s", extra.ID)
	}
}

func TestPriorityQueue_FIFOPolicy(t *testing.T) {
	pq := NewPriorityQueueWithPolicy(10, PolicyFIFO)

	items := []*Item{
		NewItem("batch-1", model.RequestMetadata{Priority: model.PriorityBatch}, context.Background()),
		NewItem("production-1", model.RequestMetadata{Priority: model.PriorityProduction}, context.Background()),
		NewItem("interactive-1", model.RequestMetadata{Priority: model.PriorityInteractive}, context.Background()),
	}

	for _, it := range items {
		if err := pq.Enqueue(it); err != nil {
			t.Fatalf("failed to enqueue: %v", err)
		}
	}

	// Must dequeue in exact arrival order (FIFO), ignoring priority
	expected := []string{"batch-1", "production-1", "interactive-1"}
	for _, exp := range expected {
		popped := pq.Dequeue()
		if popped == nil || popped.ID != exp {
			t.Fatalf("expected %s, got %v", exp, popped)
		}
	}
}

func TestPriorityQueue_CapacityLimit(t *testing.T) {
	capacity := 3
	pq := NewPriorityQueue(capacity)

	for i := 1; i <= capacity; i++ {
		item := NewItem(fmt.Sprintf("item-%d", i), model.RequestMetadata{Priority: 50}, context.Background())
		if err := pq.Enqueue(item); err != nil {
			t.Fatalf("unexpected error enqueuing item-%d: %v", i, err)
		}
	}

	if pq.Len() != capacity {
		t.Fatalf("expected queue length %d, got %d", capacity, pq.Len())
	}

	// 4th item must be rejected
	overflowItem := NewItem("item-overflow", model.RequestMetadata{Priority: 100}, context.Background())
	err := pq.Enqueue(overflowItem)
	if err != ErrQueueFull {
		t.Fatalf("expected ErrQueueFull, got %v", err)
	}

	// Dequeue one, then enqueue should succeed
	popped := pq.Dequeue()
	if popped == nil {
		t.Fatal("expected non-nil item from dequeue")
	}

	if err := pq.Enqueue(overflowItem); err != nil {
		t.Fatalf("expected successful enqueue after dequeue, got %v", err)
	}
}

func TestPriorityQueue_Remove(t *testing.T) {
	pq := NewPriorityQueue(10)

	items := []*Item{
		NewItem("p1", model.RequestMetadata{Priority: 100}, context.Background()),
		NewItem("p2", model.RequestMetadata{Priority: 80}, context.Background()),
		NewItem("p3", model.RequestMetadata{Priority: 60}, context.Background()),
		NewItem("p4", model.RequestMetadata{Priority: 40}, context.Background()),
	}

	for _, it := range items {
		_ = pq.Enqueue(it)
	}

	// Remove middle element p2
	removed, err := pq.Remove("p2")
	if err != nil {
		t.Fatalf("failed to remove p2: %v", err)
	}
	if removed.ID != "p2" {
		t.Fatalf("expected removed ID p2, got %s", removed.ID)
	}
	if pq.Len() != 3 {
		t.Fatalf("expected len 3, got %d", pq.Len())
	}

	// Verify remaining order is still valid max-heap
	expected := []string{"p1", "p3", "p4"}
	for _, exp := range expected {
		popped := pq.Dequeue()
		if popped.ID != exp {
			t.Fatalf("expected %s, got %s", exp, popped.ID)
		}
	}

	// Removing non-existent item returns ErrItemNotFound
	_, err = pq.Remove("non-existent")
	if err != ErrItemNotFound {
		t.Fatalf("expected ErrItemNotFound, got %v", err)
	}
}

func TestPriorityQueue_ConcurrentAccess(t *testing.T) {
	pq := NewPriorityQueue(100)
	var wg sync.WaitGroup

	numProducers := 10
	itemsPerProducer := 10

	for p := 0; p < numProducers; p++ {
		wg.Add(1)
		go func(prodID int) {
			defer wg.Done()
			for i := 0; i < itemsPerProducer; i++ {
				id := fmt.Sprintf("prod-%d-item-%d", prodID, i)
				item := NewItem(id, model.RequestMetadata{Priority: (prodID * 10) + i}, context.Background())
				_ = pq.Enqueue(item)
			}
		}(p)
	}

	wg.Wait()

	if pq.Len() != numProducers*itemsPerProducer {
		t.Fatalf("expected %d items, got %d", numProducers*itemsPerProducer, pq.Len())
	}

	// Dequeue all concurrently
	var dequeuedCount sync.WaitGroup
	var countMu sync.Mutex
	totalDequeued := 0

	numConsumers := 5
	for c := 0; c < numConsumers; c++ {
		dequeuedCount.Add(1)
		go func() {
			defer dequeuedCount.Done()
			for {
				item := pq.Dequeue()
				if item == nil {
					break
				}
				countMu.Lock()
				totalDequeued++
				countMu.Unlock()
			}
		}()
	}

	dequeuedCount.Wait()

	if totalDequeued != numProducers*itemsPerProducer {
		t.Fatalf("expected %d total dequeued, got %d", numProducers*itemsPerProducer, totalDequeued)
	}
}
