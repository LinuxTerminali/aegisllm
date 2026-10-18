package queue

import (
	"context"
	"time"

	"github.com/LinuxTerminali/aegisllm/internal/model"
)

// Item represents a waiting request inside the priority queue.
type Item struct {
	ID         string
	Meta       model.RequestMetadata
	EnqueuedAt time.Time
	Seq        uint64
	Ready      chan struct{}
	Ctx        context.Context
	Index      int // Position in heap, maintained by heap.Interface

	// Set by the scheduler's dispatcher before Ready is closed.
	Tokens  int   // estimated tokens, used to reserve quota for this item
	Payload any   // the capacity reservation on success
	Err     error // why the item could not be admitted
}

// NewItem creates an initialized Item.
func NewItem(id string, meta model.RequestMetadata, ctx context.Context) *Item {
	return &Item{
		ID:         id,
		Meta:       meta,
		EnqueuedAt: time.Now(),
		Ready:      make(chan struct{}),
		Ctx:        ctx,
		Index:      -1,
	}
}
