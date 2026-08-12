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
	"errors"
	"sort"
	"time"

	"sigs.k8s.io/controller-runtime/pkg/log"

	logutil "github.com/llm-d/llm-d-router/pkg/common/observability/logging"
	fwkdl "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/datalayer"
	fwkplugin "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/plugin"
	attrlatency "github.com/llm-d/llm-d-router/pkg/epp/framework/plugins/datalayer/attribute/latency"
)

// Interval is the cadence at which the datalayer calls Dispatch.
func (p *Observer) Interval() time.Duration { return p.cfg.interval }

// AppendExtractor rejects extractors: this dispatcher publishes its own state
// rather than sourcing data for others, the shape cross-replica-publisher uses.
// The caller names both plugins when it wraps this error.
func (p *Observer) AppendExtractor(fwkplugin.Plugin) error {
	return errors.New("latency observer does not accept extractors")
}

// Dispatch recomputes and publishes one endpoint's snapshot. The datalayer's
// collector calls it once per Interval, for each endpoint.
//
// An endpoint that stops receiving traffic is still visited, so its window ages
// out rather than freezing on a stale snapshot.
func (p *Observer) Dispatch(ctx context.Context, ep fwkdl.Endpoint) error {
	if ep == nil || ep.GetMetadata() == nil {
		return nil
	}
	id := ep.GetMetadata().ID.String()
	p.publish(ctx, id, p.stateFor(id), time.Now())
	return nil
}

// publish recomputes one endpoint's snapshot and swaps it in.
//
// The anchors are otherwise only visible through StateDumper, which the
// standalone file-discovery mode does not serve, so this log is the only way to
// see them in a multi-cluster hub.
func (p *Observer) publish(ctx context.Context, id string, state *endpointState, now time.Time) {
	snapshot := state.flush(now, p.cfg)
	state.published.Store(snapshot)

	if debugLogger := log.FromContext(ctx).V(logutil.DEBUG); debugLogger.Enabled() {
		debugLogger.Info("ttft-percentiles published",
			"endpoint", id,
			"floorSeconds", snapshot.Floor(),
			"longTermFloorSeconds", snapshot.FloorTTFT,
			"lowLoadSeconds", snapshot.LowLoadTTFT,
			"typicalLoadSeconds", snapshot.TypicalLoadTTFT,
			"inflightAtLowLoad", snapshot.InflightAtLowLoad,
			"inflightAtTypicalLoad", snapshot.InflightAtTypicalLoad,
			"recentRequestCount", snapshot.RecentRequestCount,
			"observations", snapshot.Observations,
			"calibrationThreshold", snapshot.CalibrationThreshold)
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
		RecentRequestCount:   len(short),
		Observations:         s.observations,
		CalibrationThreshold: cfg.minRequests,
	}
	if len(short) > 0 {
		snapshot.WindowFloorTTFT = percentileOf(short, 0.10)
		snapshot.LowLoadTTFT = percentileOf(short, cfg.lowPercentile)
		snapshot.TypicalLoadTTFT = percentileOf(short, cfg.typicalPercentile)
		snapshot.InflightAtLowLoad = bandInflight(short, cfg.lowPercentile)
		snapshot.InflightAtTypicalLoad = bandInflight(short, cfg.typicalPercentile)
	}

	s.rollBucket(now, cfg)
	snapshot.FloorTTFT = s.floor
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
		// the slowest bucket at the end for dropMax.
		sort.Float64s(s.bucketP10s)
		if len(s.bucketP10s) > cfg.bucketHistorySize {
			s.bucketP10s = dropMax(s.bucketP10s)
		}
		s.floor = percentileFloat64(s.bucketP10s, 0.10)
	}
	s.bucketStart = now
}
