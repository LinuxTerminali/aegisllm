package queue

import (
	"context"
	"time"

	"github.com/model-scheduler/model-scheduler/internal/model"
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
