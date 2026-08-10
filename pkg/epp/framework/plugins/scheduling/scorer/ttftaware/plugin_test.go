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

package ttftaware

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	fwkdl "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/datalayer"
	fwksched "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/scheduling"
	attrconcurrency "github.com/llm-d/llm-d-router/pkg/epp/framework/plugins/datalayer/attribute/concurrency"
	attrlatency "github.com/llm-d/llm-d-router/pkg/epp/framework/plugins/datalayer/attribute/latency"
)

// trusted returns a snapshot that passes every admissibility check:
// floor 0.20, B at (4, 0.35), C at (9, 0.55).
func trusted() *attrlatency.TTFTPercentiles {
	return &attrlatency.TTFTPercentiles{
		P10LowTTFT:     0.20,
		LowTTFT:        0.35,
		HighTTFT:       0.55,
		InflightAtLow:  4,
		InflightAtHigh: 9,
		RecentN:        100,
		Observations:   100,
		MinRequests:    10,
	}
}

// with applies mutations to a copy of the trusted snapshot.
func with(mutate func(*attrlatency.TTFTPercentiles)) *attrlatency.TTFTPercentiles {
	m := trusted()
	mutate(m)
	return m
}

func newEndpoint(percentiles *attrlatency.TTFTPercentiles, inflight *int64) fwksched.Endpoint {
	attr := fwkdl.NewAttributes()
	if percentiles != nil {
		attr.Put(attrlatency.TTFTPercentilesDataKey.String(), percentiles)
	}
	if inflight != nil {
		attr.Put(attrconcurrency.InFlightLoadDataKey.String(), &attrconcurrency.InFlightLoad{Requests: *inflight})
	}
	return fwksched.NewEndpoint(nil, nil, attr)
}

func inflightPtr(v int64) *int64 { return &v }

func TestScorerFactory(t *testing.T) {
	t.Run("defaults", func(t *testing.T) {
		p, err := ScorerFactory("ttft", nil, nil)
		require.NoError(t, err)
		s, ok := p.(*Scorer)
		require.True(t, ok)

		assert.Equal(t, ScorerType, s.TypedName().Type)
		assert.Equal(t, "ttft", s.TypedName().Name)
		assert.Equal(t, fwksched.Distribution, s.Category())
		assert.Equal(t, defaultExplorationRate, s.explorationRate)
		assert.Equal(t, defaultMinInflightGap, s.minInflightGap)
		assert.Equal(t, defaultRoundTTFTStep, s.roundTTFTStep)
	})

	t.Run("parses parameters", func(t *testing.T) {
		params := json.NewDecoder(strings.NewReader(
			`{"explorationRate":0.25,"minInflightGap":3,"roundTTFTStep":0.01}`))
		p, err := ScorerFactory("ttft", params, nil)
		require.NoError(t, err)
		s := p.(*Scorer)

		assert.Equal(t, 0.25, s.explorationRate)
		assert.Equal(t, 3.0, s.minInflightGap)
		assert.Equal(t, 0.01, s.roundTTFTStep)
	})

	t.Run("explorationRate zero is allowed and disables probing", func(t *testing.T) {
		params := json.NewDecoder(strings.NewReader(`{"explorationRate":0}`))
		p, err := ScorerFactory("ttft", params, nil)
		require.NoError(t, err)
		assert.Zero(t, p.(*Scorer).explorationRate)
	})

	t.Run("rejects invalid parameters", func(t *testing.T) {
		for name, raw := range map[string]string{
			"explorationRate above 1": `{"explorationRate":1.5}`,
			"explorationRate below 0": `{"explorationRate":-0.1}`,
			"minInflightGap zero":     `{"minInflightGap":0}`,
			"minInflightGap negative": `{"minInflightGap":-1}`,
			"roundTTFTStep negative":  `{"roundTTFTStep":-0.01}`,
		} {
			t.Run(name, func(t *testing.T) {
				_, err := ScorerFactory("ttft", json.NewDecoder(strings.NewReader(raw)), nil)
				require.Error(t, err)
			})
		}
	})

	t.Run("Consumes declares both inputs as required", func(t *testing.T) {
		p, err := ScorerFactory("ttft", nil, nil)
		require.NoError(t, err)
		s := p.(*Scorer)

		required := s.Consumes().Required
		require.Len(t, required, 2)
		assert.Contains(t, required, attrlatency.TTFTPercentilesDataKey)
		assert.Contains(t, required, attrconcurrency.InFlightLoadDataKey)
	})

	t.Run("NewScorer applies every field of the config", func(t *testing.T) {
		s := NewScorer(Config{ExplorationRate: 0.3, MinInflightGap: 5, RoundTTFTStep: 0.02})
		assert.Equal(t, 0.3, s.explorationRate)
		assert.Equal(t, 5.0, s.minInflightGap)
		assert.Equal(t, 0.02, s.roundTTFTStep)
	})

	t.Run("producer name overrides select a different key", func(t *testing.T) {
		params := json.NewDecoder(strings.NewReader(
			`{"ttftPercentilesProducerName":"obs-a","inFlightLoadProducerName":"load-b"}`))
		p, err := ScorerFactory("ttft", params, nil)
		require.NoError(t, err)
		s := p.(*Scorer)

		assert.Contains(t, s.percentilesDataKey.String(), "obs-a")
		assert.Contains(t, s.inFlightLoadDataKey.String(), "load-b")
	})
}

func TestPredict(t *testing.T) {
	s := NewScorer(DefaultConfig)

	tests := []struct {
		name         string
		percentiles  *attrlatency.TTFTPercentiles
		cur          float64
		wantPred     float64
		wantTrusted  bool
		wantObserved bool
	}{
		// --- cold: no usable floor -------------------------------------------
		{
			name:        "zero value is cold",
			percentiles: &attrlatency.TTFTPercentiles{},
			cur:         5,
		},
		{
			name:        "too few observations is cold even with a floor",
			percentiles: with(func(m *attrlatency.TTFTPercentiles) { m.Observations = 9 }),
			cur:         5,
		},

		// --- observed but uncalibrated: predicts at the floor -----------------
		{
			name:         "short window below minRequests seeds at floor",
			percentiles:  with(func(m *attrlatency.TTFTPercentiles) { m.RecentN = 9 }),
			cur:          5,
			wantPred:     0.20,
			wantObserved: true,
		},
		{
			name:         "no high in-flight anchor seeds at floor",
			percentiles:  with(func(m *attrlatency.TTFTPercentiles) { m.InflightAtHigh = 0 }),
			cur:          5,
			wantPred:     0.20,
			wantObserved: true,
		},
		{
			name:         "high anchor at or below the floor seeds at floor",
			percentiles:  with(func(m *attrlatency.TTFTPercentiles) { m.HighTTFT = 0.20 }),
			cur:          5,
			wantPred:     0.20,
			wantObserved: true,
		},

		// --- trusted, low point admissible: two segments ----------------------
		{
			name:         "idle predicts exactly the floor",
			percentiles:  trusted(),
			cur:          0,
			wantPred:     0.20,
			wantTrusted:  true,
			wantObserved: true,
		},
		{
			name:         "below the low anchor rides segment A->B",
			percentiles:  trusted(),
			cur:          2,
			wantPred:     0.275, // 0.20 + 2*(0.35-0.20)/4
			wantTrusted:  true,
			wantObserved: true,
		},
		{
			name:         "at the low anchor both segments agree",
			percentiles:  trusted(),
			cur:          4,
			wantPred:     0.35, // continuity between A->B and B->C
			wantTrusted:  true,
			wantObserved: true,
		},
		{
			name:         "at the high anchor reproduces the measured point",
			percentiles:  trusted(),
			cur:          9,
			wantPred:     0.55,
			wantTrusted:  true,
			wantObserved: true,
		},
		{
			name:         "beyond the high anchor extends segment B->C",
			percentiles:  trusted(),
			cur:          20,
			wantPred:     0.99, // 0.35 + (20-4)*(0.55-0.35)/(9-4)
			wantTrusted:  true,
			wantObserved: true,
		},

		// --- trusted, low point inadmissible: single floor chord A->C ---------
		{
			name: "anchors too close in load fall back to the floor chord",
			percentiles: with(func(m *attrlatency.TTFTPercentiles) {
				m.InflightAtLow = 8 // gap of 1, below minInflightGap of 2
			}),
			cur:          9,
			wantPred:     0.55, // 0.20 + 9*(0.55-0.20)/9
			wantTrusted:  true,
			wantObserved: true,
		},
		{
			name: "inverted latency ordering falls back to the floor chord",
			percentiles: with(func(m *attrlatency.TTFTPercentiles) {
				m.HighTTFT = 0.30 // below LowTTFT of 0.35
			}),
			cur:          9,
			wantPred:     0.30, // 0.20 + 9*(0.30-0.20)/9
			wantTrusted:  true,
			wantObserved: true,
		},
		{
			name: "low anchor under the floor falls back to the floor chord",
			percentiles: with(func(m *attrlatency.TTFTPercentiles) {
				m.LowTTFT = 0.15 // below the floor of 0.20
			}),
			cur:          9,
			wantPred:     0.55,
			wantTrusted:  true,
			wantObserved: true,
		},
		{
			name: "zero low in-flight anchor falls back to the floor chord",
			percentiles: with(func(m *attrlatency.TTFTPercentiles) {
				m.InflightAtLow = 0
			}),
			cur:          9,
			wantPred:     0.55,
			wantTrusted:  true,
			wantObserved: true,
		},

		// --- floor fallback before the bucket history fills --------------------
		{
			name: "falls back to the short-window P10 when no bucket floor exists",
			percentiles: with(func(m *attrlatency.TTFTPercentiles) {
				m.P10LowTTFT = 0
				m.P10TTFT = 0.20
			}),
			cur:          9,
			wantPred:     0.55,
			wantTrusted:  true,
			wantObserved: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			pred, isTrusted, isObserved := s.predict(tc.percentiles, tc.cur)

			assert.InDelta(t, tc.wantPred, pred, 1e-9)
			assert.Equal(t, tc.wantTrusted, isTrusted, "trusted")
			assert.Equal(t, tc.wantObserved, isObserved, "observed")

			// The prediction must never fall below the endpoint's own floor:
			// a loaded endpoint predicted at its idle latency would win every
			// decision it appeared in.
			if isObserved {
				assert.GreaterOrEqual(t, pred, tc.percentiles.Floor())
			}
		})
	}
}

// The curve must be monotone non-decreasing in load: more in-flight can never
// predict a faster response, or the most loaded endpoint wins.
func TestPredictIsMonotoneInLoad(t *testing.T) {
	s := NewScorer(DefaultConfig)
	prev := 0.0
	for cur := 0.0; cur <= 50; cur += 0.5 {
		pred, isTrusted, _ := s.predict(trusted(), cur)
		require.True(t, isTrusted)
		assert.GreaterOrEqual(t, pred, prev, "prediction dropped at in-flight %v", cur)
		prev = pred
	}
}

func TestScore(t *testing.T) {
	ctx := context.Background()

	t.Run("all cold score equally so the picker spreads traffic", func(t *testing.T) {
		s := NewScorer(DefaultConfig).WithExplorationRate(0)
		endpoints := []fwksched.Endpoint{
			newEndpoint(nil, inflightPtr(0)),
			newEndpoint(&attrlatency.TTFTPercentiles{}, inflightPtr(3)),
		}

		scores := s.Score(ctx, nil, endpoints)
		require.Len(t, scores, 2)
		for _, endpoint := range endpoints {
			assert.Equal(t, 1.0, scores[endpoint])
		}
	})

	t.Run("lowest predicted TTFT scores 1.0", func(t *testing.T) {
		s := NewScorer(DefaultConfig).WithExplorationRate(0)

		// loaded: 20 in flight on the trusted curve -> 0.99s
		loaded := newEndpoint(trusted(), inflightPtr(20))
		// idle but intrinsically slower: floor 0.60, B at (2, 0.75), C at (6, 0.95)
		idle := newEndpoint(&attrlatency.TTFTPercentiles{
			P10LowTTFT: 0.60, LowTTFT: 0.75, HighTTFT: 0.95,
			InflightAtLow: 2, InflightAtHigh: 6,
			RecentN: 100, Observations: 100, MinRequests: 10,
		}, inflightPtr(1)) // -> 0.60 + 1*(0.75-0.60)/2 = 0.675s

		scores := s.Score(ctx, nil, []fwksched.Endpoint{loaded, idle})

		// The slower-but-idle endpoint wins: 0.675 < 0.99.
		assert.InDelta(t, 0.0, scores[loaded], 1e-9)
		assert.InDelta(t, 1.0, scores[idle], 1e-9)
	})

	t.Run("equal predictions score neutral", func(t *testing.T) {
		s := NewScorer(DefaultConfig).WithExplorationRate(0)
		endpoints := []fwksched.Endpoint{
			newEndpoint(trusted(), inflightPtr(9)),
			newEndpoint(trusted(), inflightPtr(9)),
		}

		scores := s.Score(ctx, nil, endpoints)
		for _, endpoint := range endpoints {
			assert.Equal(t, 1.0, scores[endpoint])
		}
	})

	t.Run("roundTTFTStep makes near-ties actual ties", func(t *testing.T) {
		fast := newEndpoint(trusted(), inflightPtr(19)) // 0.95s
		slow := newEndpoint(trusted(), inflightPtr(20)) // 0.99s

		unrounded := NewScorer(DefaultConfig).WithExplorationRate(0).WithRoundTTFTStep(0)
		scores := unrounded.Score(ctx, nil, []fwksched.Endpoint{fast, slow})
		assert.Equal(t, 1.0, scores[fast])
		assert.Equal(t, 0.0, scores[slow])

		// Both round to 1.0s, so neither wins on a 40ms difference.
		rounded := NewScorer(DefaultConfig).WithExplorationRate(0).WithRoundTTFTStep(0.5)
		scores = rounded.Score(ctx, nil, []fwksched.Endpoint{fast, slow})
		assert.Equal(t, 1.0, scores[fast])
		assert.Equal(t, 1.0, scores[slow])
	})

	t.Run("exploration always probes an uncalibrated endpoint at rate 1", func(t *testing.T) {
		s := NewScorer(DefaultConfig).WithExplorationRate(1.0)

		calibrated := newEndpoint(trusted(), inflightPtr(20))    // pred 0.99
		fastCalibrated := newEndpoint(trusted(), inflightPtr(0)) // pred 0.20, would win
		uncalibrated := newEndpoint(with(func(m *attrlatency.TTFTPercentiles) {
			m.RecentN = 2 // has a floor, but no trusted operating point
		}), inflightPtr(50))

		scores := s.Score(ctx, nil, []fwksched.Endpoint{calibrated, fastCalibrated, uncalibrated})
		assert.Equal(t, 1.0, scores[uncalibrated], "probe must be forced to the top score")
	})

	t.Run("exploration suppresses an uncalibrated endpoint when a calibrated one exists", func(t *testing.T) {
		// A rate this small makes a probe on any given call effectively
		// impossible (~1e-9 per iteration), so the suppression branch is what
		// runs. Repeated to catch an accidental inversion of the coin.
		s := NewScorer(DefaultConfig).WithExplorationRate(1e-9)

		for range 200 {
			calibrated := newEndpoint(trusted(), inflightPtr(20))
			uncalibrated := newEndpoint(with(func(m *attrlatency.TTFTPercentiles) {
				m.RecentN = 2
			}), inflightPtr(50))

			scores := s.Score(ctx, nil, []fwksched.Endpoint{calibrated, uncalibrated})
			require.Equal(t, 0.0, scores[uncalibrated])
		}
	})

	t.Run("a missing in-flight reading demotes an endpoint to uncalibrated", func(t *testing.T) {
		s := NewScorer(DefaultConfig).WithExplorationRate(1e-9)

		calibrated := newEndpoint(trusted(), inflightPtr(20))
		// Full anchors, but no InFlightLoad attribute: the curve cannot be
		// evaluated at the endpoint's real load.
		noInflight := newEndpoint(trusted(), nil)

		scores := s.Score(ctx, nil, []fwksched.Endpoint{calibrated, noInflight})
		assert.Equal(t, 0.0, scores[noInflight])
	})

	t.Run("no endpoints yields no scores", func(t *testing.T) {
		s := NewScorer(DefaultConfig)
		assert.Empty(t, s.Score(ctx, nil, nil))
	})
}
