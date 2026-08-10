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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// values extracts the TTFTs of a window, in the order returned.
func values(window []observation) []float64 {
	out := make([]float64, len(window))
	for i, o := range window {
		out[i] = o.value
	}
	return out
}

func TestSlidingWindowTracker(t *testing.T) {
	now := time.Now()

	t.Run("returns observations sorted by value, not by arrival", func(t *testing.T) {
		tracker := newSlidingWindowTracker(8)
		for i, v := range []float64{0.5, 0.1, 0.9, 0.3} {
			tracker.add(v, int64(i), now.Add(time.Duration(i)*time.Millisecond))
		}

		assert.Equal(t, []float64{0.1, 0.3, 0.5, 0.9}, values(tracker.window(now.Add(time.Second), time.Minute, 0)))
	})

	t.Run("overwrites the oldest once full", func(t *testing.T) {
		tracker := newSlidingWindowTracker(3)
		for i, v := range []float64{0.1, 0.2, 0.3, 0.4, 0.5} {
			tracker.add(v, 0, now.Add(time.Duration(i)*time.Millisecond))
		}

		// Capacity 3, so only the newest three survive.
		assert.Equal(t, []float64{0.3, 0.4, 0.5}, values(tracker.window(now.Add(time.Second), time.Minute, 0)))
	})

	t.Run("maxN keeps the newest, not the smallest", func(t *testing.T) {
		tracker := newSlidingWindowTracker(8)
		// Oldest are fast, newest are slow.
		for i, v := range []float64{0.1, 0.2, 0.8, 0.9} {
			tracker.add(v, 0, now.Add(time.Duration(i)*time.Millisecond))
		}

		assert.Equal(t, []float64{0.8, 0.9}, values(tracker.window(now.Add(time.Second), time.Minute, 2)))
	})

	t.Run("excludes observations older than maxAge", func(t *testing.T) {
		tracker := newSlidingWindowTracker(8)
		tracker.add(0.1, 0, now.Add(-10*time.Minute)) // stale
		tracker.add(0.2, 0, now.Add(-1*time.Minute))
		tracker.add(0.3, 0, now)

		assert.Equal(t, []float64{0.2, 0.3}, values(tracker.window(now, 3*time.Minute, 0)))
	})

	t.Run("an empty tracker yields an empty window", func(t *testing.T) {
		assert.Empty(t, newSlidingWindowTracker(4).window(now, time.Minute, 0))
	})

	t.Run("carries the in-flight count alongside each value", func(t *testing.T) {
		tracker := newSlidingWindowTracker(4)
		tracker.add(0.9, 20, now)
		tracker.add(0.1, 2, now.Add(time.Millisecond))

		window := tracker.window(now.Add(time.Second), time.Minute, 0)
		require.Len(t, window, 2)
		assert.Equal(t, int64(2), window[0].inflight, "sorted by value, so the fast one is first")
		assert.Equal(t, int64(20), window[1].inflight)
	})
}

func TestPercentileOf(t *testing.T) {
	sorted := []observation{{value: 0}, {value: 1}, {value: 2}, {value: 3}, {value: 4}}

	tests := map[string]struct {
		p    float64
		want float64
	}{
		"minimum":               {0.0, 0},
		"median":                {0.5, 2},
		"maximum":               {1.0, 4},
		"interpolates between":  {0.125, 0.5},
		"low percentile":        {0.10, 0.4},
		"high but not the last": {0.75, 3},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			assert.InDelta(t, tc.want, percentileOf(sorted, tc.p), 1e-9)
		})
	}

	t.Run("empty is zero", func(t *testing.T) {
		assert.Zero(t, percentileOf(nil, 0.5))
	})

	t.Run("single element is that element", func(t *testing.T) {
		assert.InDelta(t, 7.0, percentileOf([]observation{{value: 7}}, 0.9), 1e-9)
	})
}

func TestPercentileFloat64(t *testing.T) {
	sorted := []float64{0, 1, 2, 3, 4}

	assert.InDelta(t, 0.0, percentileFloat64(sorted, 0), 1e-9)
	assert.InDelta(t, 2.0, percentileFloat64(sorted, 0.5), 1e-9)
	assert.InDelta(t, 4.0, percentileFloat64(sorted, 1), 1e-9)
	assert.Zero(t, percentileFloat64(nil, 0.5))
	assert.InDelta(t, 7.0, percentileFloat64([]float64{7}, 0.9), 1e-9)
}

func TestDropMinMax(t *testing.T) {
	t.Run("drops both extremes of a sorted slice", func(t *testing.T) {
		assert.Equal(t, []float64{2, 3, 4}, dropMinMax([]float64{1, 2, 3, 4, 5}))
	})

	t.Run("leaves slices too short to trim", func(t *testing.T) {
		assert.Equal(t, []float64{1}, dropMinMax([]float64{1}))
		assert.Empty(t, dropMinMax(nil))
	})

	t.Run("two elements collapse to none", func(t *testing.T) {
		assert.Empty(t, dropMinMax([]float64{1, 2}))
	})
}

func TestBandInflight(t *testing.T) {
	// Ten observations whose in-flight count rises with their TTFT.
	sorted := make([]observation, 10)
	for i := range sorted {
		sorted[i] = observation{value: float64(i) / 10, inflight: int64(i)}
	}

	t.Run("averages the band around the percentile", func(t *testing.T) {
		// P50 of n=10 sits at index 4.5, band [0.4,0.6] -> indices 3..5.
		assert.InDelta(t, 4.0, bandInflight(sorted, 0.50), 1e-9)
	})

	t.Run("a low percentile clamps the band at the start", func(t *testing.T) {
		// P10 band would start below zero; indices 0..1.
		assert.InDelta(t, 0.5, bandInflight(sorted, 0.10), 1e-9)
	})

	t.Run("a high percentile clamps the band at the end", func(t *testing.T) {
		// Band [0.85,1.05] of n=10 -> indices 7..9 (the upper end clamped).
		assert.InDelta(t, 8.0, bandInflight(sorted, 0.95), 1e-9)
	})

	t.Run("empty is zero", func(t *testing.T) {
		assert.Zero(t, bandInflight(nil, 0.5))
	})

	t.Run("a single observation is its own band", func(t *testing.T) {
		assert.InDelta(t, 3.0, bandInflight([]observation{{inflight: 3}}, 0.25), 1e-9)
	})
}
