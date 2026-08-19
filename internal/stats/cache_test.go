package stats_test

import (
	"sync"
	"testing"

	"github.com/convin/webhook-ingest/internal/stats"
)

func TestCacheRecordAccumulates(t *testing.T) {
	c := stats.NewCache()

	c.Record("acc_1", 30)
	c.Record("acc_1", 12)
	c.Record("acc_2", 5)

	got := c.Get("acc_1")
	if got.CallCount != 2 || got.TotalDurationSec != 42 {
		t.Fatalf("acc_1: got %+v, want CallCount=2 TotalDurationSec=42", got)
	}

	other := c.Get("acc_2")
	if other.CallCount != 1 || other.TotalDurationSec != 5 {
		t.Fatalf("acc_2: got %+v, want CallCount=1 TotalDurationSec=5", other)
	}
}

// TestCacheRecordIsConcurrencySafe pins down the bug where Record mutated
// the shared map without holding the lock Get uses. Run with -race and,
// before the fix, this either panics/reports a race or under-counts
// because updates from concurrent goroutines get lost.
func TestCacheRecordIsConcurrencySafe(t *testing.T) {
	c := stats.NewCache()
	const n = 500

	var wg sync.WaitGroup
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func() {
			defer wg.Done()
			c.Record("acc_race", 1)
		}()
	}
	wg.Wait()

	got := c.Get("acc_race")
	if got.CallCount != n {
		t.Fatalf("got CallCount=%d, want %d (updates were lost to the unlocked map write; rerun with -race)", got.CallCount, n)
	}
	if got.TotalDurationSec != n {
		t.Fatalf("got TotalDurationSec=%d, want %d", got.TotalDurationSec, n)
	}
}

func TestCacheGetUnknownAccountIsZero(t *testing.T) {
	c := stats.NewCache()
	if got := c.Get("nobody"); got.CallCount != 0 || got.TotalDurationSec != 0 {
		t.Fatalf("got %+v, want zero value", got)
	}
}
