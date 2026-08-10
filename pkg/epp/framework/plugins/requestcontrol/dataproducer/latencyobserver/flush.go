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
			p.flushAll(now)
		}
	}
}

// flushAll recomputes and publishes every endpoint's snapshot. The endpoint set
// is snapshotted under a read lock and released before any flush runs, so a
// slow recompute never blocks an endpoint appearing or disappearing.
func (p *Observer) flushAll(now time.Time) {
	p.mu.RLock()
	states := make([]*endpointState, 0, len(p.state))
	for _, state := range p.state {
		states = append(states, state)
	}
	p.mu.RUnlock()

	for _, state := range states {
		state.published.Store(state.flush(now, p.cfg))
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
