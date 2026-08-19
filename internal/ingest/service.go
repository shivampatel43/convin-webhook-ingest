// Package ingest accepts call-completion webhooks and processes them.
package ingest

import (
	"context"
	"encoding/json"
	"log/slog"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/convin/webhook-ingest/internal/stats"
	"github.com/convin/webhook-ingest/internal/store"
)

// recordingWork stands in for downloading and transcoding a recording.
const recordingWork = 50 * time.Millisecond

// recordingTimeout bounds how long a single recording-processing task may
// run, so a stuck download can't hang around forever and block shutdown.
const recordingTimeout = 5 * time.Second

// Service ingests webhook deliveries.
type Service struct {
	store *store.Store
	cache *stats.Cache
	rdb   *redis.Client
	log   *slog.Logger

	// bgCtx is the parent context for work that must outlive any single
	// HTTP request, such as recording processing kicked off by Ingest.
	// It is NOT derived from a request's context, which net/http cancels
	// the moment the handler returns.
	bgCtx context.Context

	// wg tracks in-flight background work so Shutdown can wait for it
	// instead of letting the process exit out from under it.
	wg sync.WaitGroup
}

// New builds a Service.
func New(s *store.Store, c *stats.Cache, rdb *redis.Client, log *slog.Logger) *Service {
	return &Service{store: s, cache: c, rdb: rdb, log: log, bgCtx: context.Background()}
}

// Stats returns the cached totals for an account.
func (s *Service) Stats(accountID string) stats.AccountStats {
	return s.cache.Get(accountID)
}

// Ingest stores a delivery and kicks off processing. Processing runs
// asynchronously so the provider gets a fast acknowledgement.
//
// Ingest is safe to call multiple times with the same event_id: the
// provider delivers at least once, and redelivery of an event that has
// already been stored is a no-op (no duplicate rows, no double-counted
// stats).
func (s *Service) Ingest(ctx context.Context, evt Event) error {
	payload, err := json.Marshal(evt)
	if err != nil {
		return err
	}

	rec := store.Event{
		EventID:      evt.EventID,
		CallID:       evt.CallID,
		AccountID:    evt.AccountID,
		Status:       evt.Status,
		DurationSec:  evt.DurationSec,
		RecordingURL: evt.RecordingURL,
		OccurredAt:   evt.OccurredAt,
		Payload:      payload,
	}

	inserted, err := s.store.IngestEvent(ctx, rec)
	if err != nil {
		return err
	}
	if !inserted {
		s.log.Info("duplicate delivery ignored", "event_id", evt.EventID)
		return nil
	}

	s.cache.Record(rec.AccountID, rec.DurationSec)

	// Recordings are slow to fetch, so that part does not block the
	// provider. It runs against s.bgCtx (not r.Context()) precisely
	// because it must still be running after this handler returns, and
	// Shutdown waits for it via s.wg.
	if rec.RecordingURL != "" {
		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			procCtx, cancel := context.WithTimeout(s.bgCtx, recordingTimeout)
			defer cancel()
			if err := s.processRecording(procCtx, rec); err != nil {
				s.log.Error("process recording failed",
					"event_id", rec.EventID, "call_id", rec.CallID, "err", err)
			}
		}()
	}

	return nil
}

// processRecording downloads and transcodes the call recording, then marks
// the call as done.
func (s *Service) processRecording(ctx context.Context, rec store.Event) error {
	select {
	case <-time.After(recordingWork):
	case <-ctx.Done():
		return ctx.Err()
	}
	return s.store.MarkRecordingProcessed(ctx, rec.CallID)
}

// Shutdown waits for in-flight background work (recording processing) to
// finish, up to ctx's deadline. Call it after the HTTP server has stopped
// accepting new requests, so nothing kicked off by a request that already
// got a 200 is abandoned mid-run.
func (s *Service) Shutdown(ctx context.Context) error {
	done := make(chan struct{})
	go func() {
		s.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
