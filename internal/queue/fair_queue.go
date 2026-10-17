package queue

import (
	"fmt"
	"sort"
	"sync"

	"github.com/LinuxTerminali/aegisllm/internal/model"
)

const (
	// UnspecifiedRunID is the key assigned to requests that omit X-Run-ID.
	// It participates as an isolated fair run, preventing untagged floods from monopolizing capacity.
	UnspecifiedRunID = "_unspecified_"
)

// PriorityBandConfig allows configuring weights across priority levels.
type PriorityBandConfig struct {
	// PriorityWeights maps priority value (e.g. 100 for production) to a relative weight.
	// If empty, strict priority order is enforced across priority levels.
	PriorityWeights map[int]int
}

// DefaultPriorityWeights provides standard relative service shares across classes.
// Production: 8x, Interactive: 4x, Background: 2x, Batch: 1x.
func DefaultPriorityWeights() map[int]int {
	return map[int]int{
		model.PriorityProduction:  8,
		model.PriorityInteractive: 4,
		model.PriorityBackground:  2,
		model.PriorityBatch:       1,
	}
}

// runQueue maintains queued requests for a single logical run in FIFO order.
type runQueue struct {
	runID   string
	items   []*Item
	deficit int
	quantum int
}

// priorityBand maintains active runs within a specific priority level.
type priorityBand struct {
	priority      int
	weight        int
	deficit       int
	quantum       int
	runs          map[string]*runQueue
	activeRuns    []*runQueue // In-order ring of active runs with queued work
	currentRunIdx int
}

func newPriorityBand(priority int, weight int) *priorityBand {
	if weight <= 0 {
		weight = 1
	}
	return &priorityBand{
		priority:   priority,
		weight:     weight,
		quantum:    weight,
		deficit:    0,
		runs:       make(map[string]*runQueue),
		activeRuns: make([]*runQueue, 0),
	}
}

func (b *priorityBand) len() int {
	count := 0
	for _, rq := range b.activeRuns {
		count += len(rq.items)
	}
	return count
}

// enqueue adds an item to the appropriate run queue within this band.
func (b *priorityBand) enqueue(item *Item) {
	runID := item.Meta.RunID
	if runID == "" {
		runID = UnspecifiedRunID
	}

	rq, exists := b.runs[runID]
	if !exists {
		rq = &runQueue{
			runID:   runID,
			items:   make([]*Item, 0, 4),
			quantum: 1, // 1 request per round-robin deficit quantum
			deficit: 0,
		}
		b.runs[runID] = rq
		b.activeRuns = append(b.activeRuns, rq)
	}

	rq.items = append(rq.items, item)
}

// dequeue selects the next item using Deficit Round-Robin across active runs.
func (b *priorityBand) dequeue() *Item {
	if len(b.activeRuns) == 0 {
		return nil
	}

	// Loop until an item is successfully dispatched
	for len(b.activeRuns) > 0 {
		if b.currentRunIdx >= len(b.activeRuns) {
			b.currentRunIdx = 0
		}

		rq := b.activeRuns[b.currentRunIdx]

		// If run has no items (should be pruned, but check defensively)
		if len(rq.items) == 0 {
			b.removeActiveRun(b.currentRunIdx)
			continue
		}

		// Award quantum if starting a new round
		if rq.deficit <= 0 {
			rq.deficit += rq.quantum
		}

		// Can this run emit a request? Cost = 1 request
		const requestCost = 1
		if rq.deficit >= requestCost {
			rq.deficit -= requestCost
			item := rq.items[0]
			rq.items = rq.items[1:]

			// If run queue is now empty, prune it from active memory
			if len(rq.items) == 0 {
				b.removeActiveRun(b.currentRunIdx)
				delete(b.runs, rq.runID)
			} else {
				// Advance cursor to next run
				b.currentRunIdx = (b.currentRunIdx + 1) % len(b.activeRuns)
			}

			return item
		}

		// Deficit insufficient, move to next run
		b.currentRunIdx = (b.currentRunIdx + 1) % len(b.activeRuns)
	}

	return nil
}

// removeActiveRun removes the run at index from activeRuns.
func (b *priorityBand) removeActiveRun(idx int) {
	if idx < 0 || idx >= len(b.activeRuns) {
		return
	}
	b.activeRuns = append(b.activeRuns[:idx], b.activeRuns[idx+1:]...)
	if b.currentRunIdx >= len(b.activeRuns) {
		b.currentRunIdx = 0
	}
}

// removeItem removes a specific item by ID from this band.
func (b *priorityBand) removeItem(id string) *Item {
	for rIdx, rq := range b.activeRuns {
		for iIdx, it := range rq.items {
			if it.ID == id {
				// Remove item preserving relative FIFO order of remaining items
				rq.items = append(rq.items[:iIdx], rq.items[iIdx+1:]...)

				// If run queue is now empty, prune it completely
				if len(rq.items) == 0 {
					b.removeActiveRun(rIdx)
					delete(b.runs, rq.runID)
				}
				return it
			}
		}
	}
	return nil
}

// FairQueue implements hierarchical Weighted Deficit Round-Robin scheduling.
// Priority levels are scheduled with strict priority or configurable inter-class weights;
// within each priority level, distinct X-Run-ID values receive fair, round-robin service.
type FairQueue struct {
	mu               sync.Mutex
	maxCapacity      int
	totalItems       int
	bands            map[int]*priorityBand
	sortedPriorities []int // Priorities sorted descending
	weights          map[int]int
	strictPriority   bool
	itemsByID        map[string]*Item
	currentBandIdx   int
}

// NewFairQueue creates a new FairQueue with the specified capacity and optional priority weights.
// If weights is nil or empty, strict priority order is enforced across priority levels.
func NewFairQueue(maxCapacity int, weights map[int]int) *FairQueue {
	if maxCapacity <= 0 {
		maxCapacity = 1
	}

	fq := &FairQueue{
		maxCapacity:      maxCapacity,
		bands:            make(map[int]*priorityBand),
		sortedPriorities: make([]int, 0),
		weights:          weights,
		strictPriority:   len(weights) == 0,
		itemsByID:        make(map[string]*Item),
	}

	return fq
}

// Capacity returns the maximum queue capacity.
func (fq *FairQueue) Capacity() int {
	return fq.maxCapacity
}

// Len returns the total number of currently queued items across all bands.
func (fq *FairQueue) Len() int {
	fq.mu.Lock()
	defer fq.mu.Unlock()
	return fq.totalItems
}

// Enqueue inserts a new request item into the appropriate priority band and run queue.
// Returns ErrQueueFull if total capacity is reached.
func (fq *FairQueue) Enqueue(item *Item) error {
	fq.mu.Lock()
	defer fq.mu.Unlock()

	if fq.totalItems >= fq.maxCapacity {
		return ErrQueueFull
	}

	pri := item.Meta.Priority
	band, exists := fq.bands[pri]
	if !exists {
		weight := 1
		if w, ok := fq.weights[pri]; ok && w > 0 {
			weight = w
		}
		band = newPriorityBand(pri, weight)
		fq.bands[pri] = band
		fq.sortedPriorities = append(fq.sortedPriorities, pri)
		sort.Slice(fq.sortedPriorities, func(i, j int) bool {
			return fq.sortedPriorities[i] > fq.sortedPriorities[j] // Descending
		})
	}

	band.enqueue(item)
	fq.itemsByID[item.ID] = item
	fq.totalItems++

	return nil
}

// Dequeue selects the next item to admit using hierarchical fair scheduling.
func (fq *FairQueue) Dequeue() *Item {
	fq.mu.Lock()
	defer fq.mu.Unlock()

	if fq.totalItems == 0 {
		return nil
	}

	var chosenItem *Item

	if fq.strictPriority {
		// Strict priority: scan bands from highest priority to lowest
		for _, pri := range fq.sortedPriorities {
			band := fq.bands[pri]
			if band != nil && len(band.activeRuns) > 0 {
				chosenItem = band.dequeue()
				if chosenItem != nil {
					break
				}
			}
		}
	} else {
		// Weighted Deficit Round-Robin across non-empty priority bands
		for attempts := 0; attempts < len(fq.sortedPriorities)*2; attempts++ {
			if fq.currentBandIdx >= len(fq.sortedPriorities) {
				fq.currentBandIdx = 0
			}

			pri := fq.sortedPriorities[fq.currentBandIdx]
			band := fq.bands[pri]

			if band == nil || len(band.activeRuns) == 0 {
				fq.currentBandIdx = (fq.currentBandIdx + 1) % len(fq.sortedPriorities)
				continue
			}

			if band.deficit <= 0 {
				band.deficit += band.quantum
			}

			const cost = 1
			if band.deficit >= cost {
				chosenItem = band.dequeue()
				if chosenItem != nil {
					band.deficit -= cost
					if len(band.activeRuns) == 0 {
						band.deficit = 0
						fq.currentBandIdx = (fq.currentBandIdx + 1) % len(fq.sortedPriorities)
					} else if band.deficit < cost {
						fq.currentBandIdx = (fq.currentBandIdx + 1) % len(fq.sortedPriorities)
					}
					break
				}
			}

			fq.currentBandIdx = (fq.currentBandIdx + 1) % len(fq.sortedPriorities)
		}

		// Fallback: if DRR skipped all deficits, select first non-empty band
		if chosenItem == nil {
			for _, pri := range fq.sortedPriorities {
				band := fq.bands[pri]
				if band != nil && len(band.activeRuns) > 0 {
					chosenItem = band.dequeue()
					if chosenItem != nil {
						break
					}
				}
			}
		}
	}

	if chosenItem != nil {
		delete(fq.itemsByID, chosenItem.ID)
		fq.totalItems--
		fq.pruneEmptyBands()
	}

	return chosenItem
}

// Remove extracts a specific item by ID (used on context cancellation or queue wait timeout).
func (fq *FairQueue) Remove(id string) (*Item, error) {
	fq.mu.Lock()
	defer fq.mu.Unlock()

	item, exists := fq.itemsByID[id]
	if !exists {
		return nil, ErrItemNotFound
	}

	band, bandExists := fq.bands[item.Meta.Priority]
	if !bandExists {
		return nil, ErrItemNotFound
	}

	removed := band.removeItem(id)
	if removed == nil {
		return nil, ErrItemNotFound
	}

	delete(fq.itemsByID, id)
	fq.totalItems--
	fq.pruneEmptyBands()

	return removed, nil
}

// pruneEmptyBands deletes bands that have no items to bound in-memory state.
func (fq *FairQueue) pruneEmptyBands() {
	newSorted := make([]int, 0, len(fq.sortedPriorities))
	for _, pri := range fq.sortedPriorities {
		band := fq.bands[pri]
		if band != nil && len(band.activeRuns) > 0 {
			newSorted = append(newSorted, pri)
		} else {
			delete(fq.bands, pri)
		}
	}
	fq.sortedPriorities = newSorted
	if fq.currentBandIdx >= len(fq.sortedPriorities) {
		fq.currentBandIdx = 0
	}
}

// Stats returns a snapshot of queue depths by priority and active runs.
type FairQueueStats struct {
	TotalQueued      int            `json:"total_queued"`
	QueuedByPriority map[string]int `json:"queued_by_priority"`
	QueuedByRun      map[string]int `json:"queued_by_run"`
	ActiveRunsCount  int            `json:"active_runs_count"`
}

// Stats extracts safe, bounded queue depth statistics.
func (fq *FairQueue) Stats() FairQueueStats {
	fq.mu.Lock()
	defer fq.mu.Unlock()

	byPriority := make(map[string]int)
	byRun := make(map[string]int)
	totalRuns := 0

	for pri, band := range fq.bands {
		classLabel := priorityToClass(pri)
		byPriority[classLabel] += band.len()

		for runID, rq := range band.runs {
			totalRuns++
			// Bound run depth map to top 20 active runs to avoid metric cardinality explosion
			if len(byRun) < 20 {
				byRun[runID] = len(rq.items)
			}
		}
	}

	return FairQueueStats{
		TotalQueued:      fq.totalItems,
		QueuedByPriority: byPriority,
		QueuedByRun:      byRun,
		ActiveRunsCount:  totalRuns,
	}
}

func priorityToClass(pri int) string {
	switch pri {
	case model.PriorityProduction:
		return "production"
	case model.PriorityInteractive:
		return "interactive"
	case model.PriorityBackground:
		return "background"
	case model.PriorityBatch:
		return "batch"
	default:
		return fmt.Sprintf("p_%d", pri)
	}
}
