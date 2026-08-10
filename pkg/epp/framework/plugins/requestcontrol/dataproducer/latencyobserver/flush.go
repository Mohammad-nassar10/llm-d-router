/*
Copyright 2026 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package latencyobserver

import (
	"context"
	"sort"
	"time"

	"sigs.k8s.io/controller-runtime/pkg/log"

	logutil "github.com/llm-d/llm-d-router/pkg/common/observability/logging"
	attrlatency "github.com/llm-d/llm-d-router/pkg/epp/framework/plugins/datalayer/attribute/latency"
)

// flushLoop recomputes every endpoint's snapshot until ctx is cancelled; one
// goroutine serves all endpoints.
//
// It runs here rather than in a request hook so no request pays for it, and so
// an endpoint that stops receiving traffic still ages its window out instead of
// freezing on a stale snapshot.
func (p *Observer) flushLoop(ctx context.Context) {
	ticker := time.NewTicker(p.cfg.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case now := <-ticker.C:
			p.flushAll(ctx, now)
		}
	}
}

// flushAll recomputes and publishes every endpoint's snapshot. The endpoint set
// is snapshotted under a read lock and released before any flush runs, so a
// slow recompute never blocks an endpoint appearing or disappearing.
func (p *Observer) flushAll(ctx context.Context, now time.Time) {
	type entry struct {
		id    string
		state *endpointState
	}

	p.mu.RLock()
	entries := make([]entry, 0, len(p.state))
	for id, state := range p.state {
		entries = append(entries, entry{id: id, state: state})
	}
	p.mu.RUnlock()

	// The published anchors are otherwise only visible through StateDumper,
	// which the standalone file-discovery mode does not serve — so this log is
	// the only way to see them in a multi-cluster hub.
	debugLogger := log.FromContext(ctx).V(logutil.DEBUG)

	for _, e := range entries {
		snapshot := e.state.flush(now, p.cfg)
		e.state.published.Store(snapshot)

		if debugLogger.Enabled() {
			debugLogger.Info("ttft-percentiles published",
				"endpoint", e.id,
				"floorSeconds", snapshot.Floor(),
				"p10LowSeconds", snapshot.P10LowTTFT,
				"lowSeconds", snapshot.LowTTFT,
				"highSeconds", snapshot.HighTTFT,
				"inflightAtLow", snapshot.InflightAtLow,
				"inflightAtHigh", snapshot.InflightAtHigh,
				"recentN", snapshot.RecentN,
				"observations", snapshot.Observations,
				"minRequests", snapshot.MinRequests)
		}
	}
}

// flush recomputes one endpoint's snapshot from two windows. The short window
// is capped and age-bounded, yielding operating anchors that track load
// quickly. The floor is slower, and deliberately so: because its history spans
// both busy and quiet buckets, a low percentile of it locks onto the quiet ones
// — the queue-free service time — instead of drifting up with recent load.
func (s *endpointState) flush(now time.Time, cfg resolvedConfig) *attrlatency.TTFTPercentiles {
	s.mu.Lock()
	defer s.mu.Unlock()

	short := s.tracker.window(now, cfg.maxObservationAge, cfg.maxRequests)
	snapshot := &attrlatency.TTFTPercentiles{
		RecentN:      len(short),
		Observations: s.observations,
		MinRequests:  cfg.minRequests,
	}
	if len(short) > 0 {
		snapshot.P10TTFT = percentileOf(short, 0.10)
		snapshot.LowTTFT = percentileOf(short, cfg.lowPercentile)
		snapshot.HighTTFT = percentileOf(short, cfg.highPercentile)
		snapshot.InflightAtLow = bandInflight(short, cfg.lowPercentile)
		snapshot.InflightAtHigh = bandInflight(short, cfg.highPercentile)
	}

	s.rollBucket(now, cfg)
	snapshot.P10LowTTFT = s.p10Low
	return snapshot
}

// rollBucket closes the current floor bucket if it has elapsed, folding its P10
// into the history and recomputing the floor. Caller must hold s.mu.
func (s *endpointState) rollBucket(now time.Time, cfg resolvedConfig) {
	if s.bucketStart.IsZero() {
		s.bucketStart = now
		return
	}
	if now.Sub(s.bucketStart) < cfg.bucketDuration {
		return
	}

	if bucket := s.tracker.window(now, cfg.bucketDuration, 0); len(bucket) > 0 {
		s.bucketP10s = append(s.bucketP10s, percentileOf(bucket, 0.10))
		// Sorted here because the percentile needs it anyway, which also puts
		// the extremes at the ends for dropMinMax.
		sort.Float64s(s.bucketP10s)
		if len(s.bucketP10s) > cfg.bucketHistorySize {
			s.bucketP10s = dropMinMax(s.bucketP10s)
		}
		s.p10Low = percentileFloat64(s.bucketP10s, 0.10)
	}
	s.bucketStart = now
}
