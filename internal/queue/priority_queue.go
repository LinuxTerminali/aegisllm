package queue

import (
	"container/heap"
	"errors"
	"sync"
	"sync/atomic"
)

var (
	// ErrQueueFull is returned when an enqueue operation exceeds maximum capacity.
	ErrQueueFull = errors.New("queue capacity exceeded")
	// ErrItemNotFound is returned when an item cannot be found for removal.
	ErrItemNotFound = errors.New("queue item not found")
)

// itemHeap implements heap.Interface for Items.
type itemHeap []*Item

func (h itemHeap) Len() int { return len(h) }

func (h itemHeap) Less(i, j int) bool {
	// Max-heap by Priority: higher priority value first
	if h[i].Meta.Priority != h[j].Meta.Priority {
		return h[i].Meta.Priority > h[j].Meta.Priority
	}
	// Strict FIFO tie-breaker on identical priority: lower sequence number first
	return h[i].Seq < h[j].Seq
}

func (h itemHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
	h[i].Index = i
	h[j].Index = j
}

func (h *itemHeap) Push(x any) {
	n := len(*h)
	item := x.(*Item)
	item.Index = n
	*h = append(*h, item)
}

func (h *itemHeap) Pop() any {
	old := *h
	n := len(old)
	item := old[n-1]
	old[n-1] = nil // Avoid memory leak
	item.Index = -1
	*h = old[0 : n-1]
	return item
}

// PriorityQueue provides a concurrency-safe, bounded priority queue.
type PriorityQueue struct {
	mu          sync.Mutex
	h           itemHeap
	itemsByID   map[string]*Item
	maxCapacity int
	seqCounter  atomic.Uint64
}

// NewPriorityQueue creates a PriorityQueue with the given maximum capacity.
func NewPriorityQueue(maxCapacity int) *PriorityQueue {
	if maxCapacity <= 0 {
		maxCapacity = 1
	}
	pq := &PriorityQueue{
		h:           make(itemHeap, 0),
		itemsByID:   make(map[string]*Item),
		maxCapacity: maxCapacity,
	}
	heap.Init(&pq.h)
	return pq
}

// Capacity returns the maximum capacity of the queue.
func (pq *PriorityQueue) Capacity() int {
	return pq.maxCapacity
}

// Len returns the current number of queued items.
func (pq *PriorityQueue) Len() int {
	pq.mu.Lock()
	defer pq.mu.Unlock()
	return len(pq.h)
}

// Enqueue inserts an item into the priority queue.
// Returns ErrQueueFull if the queue has reached maxCapacity.
func (pq *PriorityQueue) Enqueue(item *Item) error {
	pq.mu.Lock()
	defer pq.mu.Unlock()

	if len(pq.h) >= pq.maxCapacity {
		return ErrQueueFull
	}

	item.Seq = pq.seqCounter.Add(1)
	heap.Push(&pq.h, item)
	pq.itemsByID[item.ID] = item
	return nil
}

// Dequeue removes and returns the highest priority item from the queue.
// If the queue is empty, returns nil.
func (pq *PriorityQueue) Dequeue() *Item {
	pq.mu.Lock()
	defer pq.mu.Unlock()

	if len(pq.h) == 0 {
		return nil
	}

	item := heap.Pop(&pq.h).(*Item)
	delete(pq.itemsByID, item.ID)
	return item
}

// Remove removes a specific item by its ID.
// This is used when a request context is canceled or times out while waiting.
func (pq *PriorityQueue) Remove(id string) (*Item, error) {
	pq.mu.Lock()
	defer pq.mu.Unlock()

	item, exists := pq.itemsByID[id]
	if !exists {
		return nil, ErrItemNotFound
	}

	heap.Remove(&pq.h, item.Index)
	delete(pq.itemsByID, id)
	return item, nil
}

// Peek returns the highest priority item without removing it.
// Returns nil if queue is empty.
func (pq *PriorityQueue) Peek() *Item {
	pq.mu.Lock()
	defer pq.mu.Unlock()

	if len(pq.h) == 0 {
		return nil
	}
	return pq.h[0]
}
