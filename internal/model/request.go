package model

import (
	"strconv"
	"strings"
	"time"
)

// WorkloadClass represents the logical class of workload requested by the client.
type WorkloadClass string

const (
	WorkloadClassProduction  WorkloadClass = "production"
	WorkloadClassInteractive WorkloadClass = "interactive"
	WorkloadClassBackground  WorkloadClass = "background"
	WorkloadClassBatch       WorkloadClass = "batch"
)

// Default priorities mapped from WorkloadClass.
// Higher integer value indicates higher scheduling precedence.
const (
	PriorityProduction  = 100
	PriorityInteractive = 75
	PriorityBackground  = 50
	PriorityBatch       = 25
	PriorityDefault     = PriorityInteractive
)

// RequestMetadata holds parsed metadata extracted from client HTTP headers.
type RequestMetadata struct {
	RequestID     string        `json:"request_id"`
	RunID         string        `json:"run_id"`
	WorkloadClass WorkloadClass `json:"workload_class"`
	Priority      int           `json:"priority"`
	ReceivedAt    time.Time     `json:"received_at"`
}

// ParseRequestMetadata extracts and validates scheduling metadata from HTTP headers.
// Callers must supply stable run IDs via X-Run-ID for run-level fairness.
func ParseRequestMetadata(requestID, runID, workloadHeader, priorityHeader string) RequestMetadata {
	meta := RequestMetadata{
		RequestID:  requestID,
		RunID:      strings.TrimSpace(runID),
		ReceivedAt: time.Now(),
	}

	// Canonicalize workload class
	wc := strings.ToLower(strings.TrimSpace(workloadHeader))
	switch WorkloadClass(wc) {
	case WorkloadClassProduction:
		meta.WorkloadClass = WorkloadClassProduction
		meta.Priority = PriorityProduction
	case WorkloadClassInteractive:
		meta.WorkloadClass = WorkloadClassInteractive
		meta.Priority = PriorityInteractive
	case WorkloadClassBackground:
		meta.WorkloadClass = WorkloadClassBackground
		meta.Priority = PriorityBackground
	case WorkloadClassBatch:
		meta.WorkloadClass = WorkloadClassBatch
		meta.Priority = PriorityBatch
	default:
		// Default to interactive if unspecified or unrecognized
		meta.WorkloadClass = WorkloadClassInteractive
		meta.Priority = PriorityDefault
	}

	// Allow X-Priority to explicitly override the class default priority
	if trimmedPri := strings.TrimSpace(priorityHeader); trimmedPri != "" {
		if priVal, err := strconv.Atoi(trimmedPri); err == nil {
			meta.Priority = priVal
		}
	}

	return meta
}
