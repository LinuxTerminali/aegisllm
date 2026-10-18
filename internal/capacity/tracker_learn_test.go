package capacity

import (
	"context"
	"net/http"
	"testing"
	"time"
)

func TestTracker_LearnsLimitsFromHeaders(t *testing.T) {
	tr := NewDualTokenBucketTracker(Config{AutoLearn: true})

	// Nothing configured: unconstrained.
	if st := tr.Stats(); st.RequestsPerMinute != 0 || st.TokensPerMinute != 0 || st.LimitsLearned {
		t.Fatalf("expected unconstrained tracker before headers, got %+v", st)
	}

	h := http.Header{}
	h.Set("x-ratelimit-limit-requests", "600")
	h.Set("x-ratelimit-limit-tokens", "60000")
	h.Set("x-ratelimit-remaining-requests", "599")
	h.Set("x-ratelimit-remaining-tokens", "59000")
	tr.ReconcileHeaders(h)

	st := tr.Stats()
	if !st.LimitsLearned {
		t.Fatalf("expected limits to be learned")
	}
	if st.RequestsPerMinute != 600 || st.TokensPerMinute != 60000 {
		t.Fatalf("expected 600 RPM / 60000 TPM learned, got %+v", st)
	}
	// Burst is learned from the highest remaining value seen (+1 for the request just made).
	if st.MaxRequestCapacity != 600 || st.MaxTokenCapacity != 59000 {
		t.Fatalf("expected burst 600 req / 59000 tokens, got %+v", st)
	}

	// A low remaining later must not shrink the learned burst, only the current level.
	h2 := http.Header{}
	h2.Set("x-ratelimit-limit-requests", "600")
	h2.Set("x-ratelimit-remaining-requests", "3")
	tr.ReconcileHeaders(h2)
	st = tr.Stats()
	if st.MaxRequestCapacity != 600 || st.AvailableRequestTokens > 3 {
		t.Fatalf("expected burst kept at 600 and level synced down to 3, got %+v", st)
	}

	// Learned limits now pace: drain the request bucket and expect a wait.
	for i := 0; i < 3; i++ {
		if _, err := tr.Reserve(context.Background(), "r", 1); err != nil {
			t.Fatalf("reserve %d failed: %v", i, err)
		}
	}
	start := time.Now()
	if _, err := tr.Reserve(context.Background(), "paced", 1); err != nil {
		t.Fatalf("paced reserve failed: %v", err)
	}
	if time.Since(start) < 50*time.Millisecond {
		t.Fatalf("expected pacing after learned burst was consumed")
	}
}

func TestTracker_DoesNotOverrideExplicitLimits(t *testing.T) {
	tr := NewDualTokenBucketTracker(Config{RequestsPerMinute: 120, AutoLearn: true})
	h := http.Header{}
	h.Set("x-ratelimit-limit-requests", "6000")
	tr.ReconcileHeaders(h)
	if st := tr.Stats(); st.RequestsPerMinute != 120 || st.LimitsLearned {
		t.Fatalf("explicit RPM must win over headers, got %+v", st)
	}
}

func TestTracker_PacingWaitsUntilDeadlineWhenNoCap(t *testing.T) {
	// 60 RPM, burst 1, no MaxPacingDelay: second request waits ~1s instead of being rejected.
	tr := NewDualTokenBucketTracker(Config{RequestsPerMinute: 60, RequestBurst: 1})
	if _, err := tr.Reserve(context.Background(), "a", 1); err != nil {
		t.Fatalf("first reserve failed: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	start := time.Now()
	if _, err := tr.Reserve(ctx, "b", 1); err != nil {
		t.Fatalf("second reserve should wait, not fail: %v", err)
	}
	if el := time.Since(start); el < 800*time.Millisecond {
		t.Fatalf("expected ~1s pacing wait, got %v", el)
	}

	// With a deadline shorter than the needed wait, it is rejected up front.
	short, cancel2 := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel2()
	if _, err := tr.Reserve(short, "c", 1); err != ErrInsufficientCapacity {
		t.Fatalf("expected ErrInsufficientCapacity for infeasible deadline, got %v", err)
	}
}

func TestTracker_MarkExhaustedHoldsReservations(t *testing.T) {
	tr := NewDualTokenBucketTracker(Config{RequestsPerMinute: 6000, RequestBurst: 100})
	tr.MarkExhausted(400 * time.Millisecond)

	if st := tr.Stats(); st.AvailableRequestTokens != 0 || st.ExhaustedUntilMs <= 0 {
		t.Fatalf("expected drained bucket and exhausted window, got %+v", st)
	}

	start := time.Now()
	if _, err := tr.Reserve(context.Background(), "after-429", 1); err != nil {
		t.Fatalf("reserve failed: %v", err)
	}
	if el := time.Since(start); el < 350*time.Millisecond {
		t.Fatalf("expected reservation to wait for Retry-After window, waited %v", el)
	}
}
