package ingest_test

import (
	"context"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/convin/webhook-ingest/internal/config"
	"github.com/convin/webhook-ingest/internal/ingest"
	"github.com/convin/webhook-ingest/internal/redisclient"
	"github.com/convin/webhook-ingest/internal/stats"
	"github.com/convin/webhook-ingest/internal/testutil"
)

// TestShutdownWaitsForInFlightRecordingProcessing exercises Service.Shutdown
// directly: Ingest hands recording processing off to a goroutine and
// returns immediately, and Shutdown is what's supposed to block until that
// goroutine actually finishes. This is the piece main.go relies on to avoid
// dropping in-flight work on every deploy.
func TestShutdownWaitsForInFlightRecordingProcessing(t *testing.T) {
	st := testutil.NewStore(t)
	eventID, callID, accountID := testutil.IDs(t, st)

	cfg := config.Load()
	rdb, err := redisclient.New(context.Background(), cfg.RedisAddr)
	if err != nil {
		t.Fatalf("connect redis: %v", err)
	}
	defer func() { _ = rdb.Close() }()

	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	svc := ingest.New(st, stats.NewCache(), rdb, log)

	evt := ingest.Event{
		EventID:      eventID,
		CallID:       callID,
		AccountID:    accountID,
		Status:       "completed",
		DurationSec:  5,
		RecordingURL: "https://recordings.example.com/" + callID + ".wav",
		OccurredAt:   time.Now(),
	}
	if err := svc.Ingest(context.Background(), evt); err != nil {
		t.Fatalf("Ingest: %v", err)
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := svc.Shutdown(shutdownCtx); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}

	var processed bool
	row := st.Pool().QueryRow(context.Background(),
		`SELECT recording_processed FROM calls WHERE call_id = $1`, callID)
	if err := row.Scan(&processed); err != nil {
		t.Fatalf("scan: %v", err)
	}
	if !processed {
		t.Fatal("expected Shutdown to wait for in-flight recording processing to finish")
	}
}
