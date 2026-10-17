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

// SchedulingPolicy determines queue eviction order.
type SchedulingPolicy string

const (
	PolicyPriority SchedulingPolicy = "priority"
	PolicyFIFO     SchedulingPolicy = "fifo"
	PolicyFair     SchedulingPolicy = "fair"
)

// itemHeap implements heap.Interface for Items.
type itemHeap struct {
	items  []*Item
	policy SchedulingPolicy
}

func (h itemHeap) Len() int { return len(h.items) }

func (h itemHeap) Less(i, j int) bool {
	if h.policy == PolicyFIFO {
		return h.items[i].Seq < h.items[j].Seq
	}
	// Max-heap by Priority: higher priority value first
	if h.items[i].Meta.Priority != h.items[j].Meta.Priority {
		return h.items[i].Meta.Priority > h.items[j].Meta.Priority
	}
	// Strict FIFO tie-breaker on identical priority: lower sequence number first
	return h.items[i].Seq < h.items[j].Seq
}

func (h itemHeap) Swap(i, j int) {
	h.items[i], h.items[j] = h.items[j], h.items[i]
	h.items[i].Index = i
	h.items[j].Index = j
}

func (h *itemHeap) Push(x any) {
	n := len(h.items)
	item := x.(*Item)
	item.Index = n
	h.items = append(h.items, item)
}

func (h *itemHeap) Pop() any {
	old := h.items
	n := len(old)
	item := old[n-1]
	old[n-1] = nil // Avoid memory leak
	item.Index = -1
	h.items = old[0 : n-1]
	return item
}

// PriorityQueue provides a concurrency-safe, bounded priority queue.
type PriorityQueue struct {
	mu          sync.Mutex
	policy      SchedulingPolicy
	h           itemHeap
	itemsByID   map[string]*Item
	maxCapacity int
	seqCounter  atomic.Uint64
	fairQ       *FairQueue
}

// NewPriorityQueue creates a PriorityQueue with default priority ordering.
func NewPriorityQueue(maxCapacity int) *PriorityQueue {
	return NewPriorityQueueWithPolicy(maxCapacity, PolicyPriority)
}

// NewPriorityQueueWithPolicy creates a PriorityQueue with the specified policy.
func NewPriorityQueueWithPolicy(maxCapacity int, policy SchedulingPolicy) *PriorityQueue {
	return NewPriorityQueueWithWeights(maxCapacity, policy, nil)
}

// NewPriorityQueueWithWeights creates a PriorityQueue with policy and optional priority class weights.
func NewPriorityQueueWithWeights(maxCapacity int, policy SchedulingPolicy, weights map[int]int) *PriorityQueue {
	if maxCapacity <= 0 {
		maxCapacity = 1
	}
	if policy == "" {
		policy = PolicyPriority
	}

	pq := &PriorityQueue{
		policy: policy,
		h: itemHeap{
			items:  make([]*Item, 0),
			policy: policy,
		},
		itemsByID:   make(map[string]*Item),
		maxCapacity: maxCapacity,
	}

	if policy == PolicyFair {
		pq.fairQ = NewFairQueue(maxCapacity, weights)
	} else {
		heap.Init(&pq.h)
	}

	return pq
}

// Capacity returns the maximum capacity of the queue.
func (pq *PriorityQueue) Capacity() int {
	if pq.policy == PolicyFair && pq.fairQ != nil {
		return pq.fairQ.Capacity()
	}
	return pq.maxCapacity
}

// Len returns the current number of queued items.
func (pq *PriorityQueue) Len() int {
	if pq.policy == PolicyFair && pq.fairQ != nil {
		return pq.fairQ.Len()
	}
	pq.mu.Lock()
	defer pq.mu.Unlock()
	return len(pq.h.items)
}

// Enqueue inserts an item into the priority queue.
// Returns ErrQueueFull if the queue has reached maxCapacity.
func (pq *PriorityQueue) Enqueue(item *Item) error {
	if pq.policy == PolicyFair && pq.fairQ != nil {
		return pq.fairQ.Enqueue(item)
	}

	pq.mu.Lock()
	defer pq.mu.Unlock()

	if len(pq.h.items) >= pq.maxCapacity {
		return ErrQueueFull
	}

	item.Seq = pq.seqCounter.Add(1)
	heap.Push(&pq.h, item)
	pq.itemsByID[item.ID] = item
	return nil
}

// Dequeue removes and returns the highest priority or fairest item from the queue.
// If the queue is empty, returns nil.
func (pq *PriorityQueue) Dequeue() *Item {
	if pq.policy == PolicyFair && pq.fairQ != nil {
		return pq.fairQ.Dequeue()
	}

	pq.mu.Lock()
	defer pq.mu.Unlock()

	if len(pq.h.items) == 0 {
		return nil
	}

	item := heap.Pop(&pq.h).(*Item)
	delete(pq.itemsByID, item.ID)
	return item
}

// Remove removes a specific item by its ID.
// This is used when a request context is canceled or times out while waiting.
func (pq *PriorityQueue) Remove(id string) (*Item, error) {
	if pq.policy == PolicyFair && pq.fairQ != nil {
		return pq.fairQ.Remove(id)
	}

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
	if pq.policy == PolicyFair && pq.fairQ != nil {
		// FairQueue does not expose Peek because Deficit Round-Robin state advances on Dequeue
		return nil
	}

	pq.mu.Lock()
	defer pq.mu.Unlock()

	if len(pq.h.items) == 0 {
		return nil
	}
	return pq.h.items[0]
}

// Stats returns snapshot queue depth statistics by priority and run.
func (pq *PriorityQueue) Stats() FairQueueStats {
	if pq.policy == PolicyFair && pq.fairQ != nil {
		return pq.fairQ.Stats()
	}

	pq.mu.Lock()
	defer pq.mu.Unlock()

	byPriority := make(map[string]int)
	byRun := make(map[string]int)
	for _, it := range pq.h.items {
		byPriority[priorityToClass(it.Meta.Priority)]++
		runID := it.Meta.RunID
		if runID == "" {
			runID = UnspecifiedRunID
		}
		if len(byRun) < 20 {
			byRun[runID]++
		}
	}

	return FairQueueStats{
		TotalQueued:      len(pq.h.items),
		QueuedByPriority: byPriority,
		QueuedByRun:      byRun,
		ActiveRunsCount:  len(byRun),
	}
}
