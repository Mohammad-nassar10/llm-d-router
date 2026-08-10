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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestResolveConfig(t *testing.T) {
	t.Run("defaults resolve", func(t *testing.T) {
		resolved, err := DefaultConfig.resolve()
		require.NoError(t, err)

		assert.Equal(t, time.Second, resolved.interval)
		assert.Equal(t, 3*time.Minute, resolved.maxObservationAge)
		assert.Equal(t, time.Minute, resolved.bucketDuration)
		assert.Equal(t, 5000, resolved.windowSize)
		assert.Equal(t, 100, resolved.maxRequests)
		assert.Equal(t, 10, resolved.minRequests)
		assert.Equal(t, 1000, resolved.bucketHistorySize)
		assert.InDelta(t, 0.25, resolved.lowPercentile, 1e-9)
		assert.InDelta(t, 0.50, resolved.highPercentile, 1e-9)
	})

	t.Run("rejects invalid configurations", func(t *testing.T) {
		tests := map[string]func(*Config){
			"zero window":              func(c *Config) { c.WindowSize = 0 },
			"zero maxRequests":         func(c *Config) { c.MaxRequests = 0 },
			"maxRequests above window": func(c *Config) { c.MaxRequests = c.WindowSize + 1 },
			"zero minRequests":         func(c *Config) { c.MinRequests = 0 },
			"bucket history below two": func(c *Config) { c.BucketHistorySize = 1 },
			"percentiles inverted":     func(c *Config) { c.LowPercentile, c.HighPercentile = 50, 25 },
			"percentile at zero":       func(c *Config) { c.LowPercentile = 0 },
			"percentile at hundred":    func(c *Config) { c.HighPercentile = 100 },
			"unparseable interval":     func(c *Config) { c.IntervalDuration = "soon" },
			"zero interval":            func(c *Config) { c.IntervalDuration = "0s" },
			"negative bucket":          func(c *Config) { c.BucketDuration = "-1m" },
			"empty observation age":    func(c *Config) { c.MaxObservationAge = "" },
		}
		for name, mutate := range tests {
			t.Run(name, func(t *testing.T) {
				cfg := DefaultConfig
				mutate(&cfg)
				_, err := cfg.resolve()
				require.Error(t, err)
			})
		}
	})
}

// flushConfig is tuned for tests: a short bucket so the floor path runs, and a
// low minRequests so a handful of observations is enough to be trusted.
func flushConfig() Config {
	cfg := DefaultConfig
	cfg.WindowSize = 64
	cfg.MaxRequests = 16
	cfg.MinRequests = 4
	cfg.BucketDuration = "1s"
	cfg.BucketHistorySize = 4
	return cfg
}

func TestFlush(t *testing.T) {
	now := time.Now()

	t.Run("publishes the operating anchors from the short window", func(t *testing.T) {
		p := newObserverWithConfig(t, flushConfig())
		// Ten observations: TTFT rises with the in-flight count it was
		// dispatched at, which is the relationship the scorer's curve models.
		for i := range 10 {
			p.record("default/a", 0.1+float64(i)*0.1, int64(i), now.Add(time.Duration(i)*time.Millisecond))
		}

		p.flushAll(now.Add(time.Second))

		snapshot := p.stateFor("default/a").published.Load()
		require.NotNil(t, snapshot)
		assert.Equal(t, 10, snapshot.RecentN)
		assert.Equal(t, int64(10), snapshot.Observations)
		assert.Equal(t, 4, snapshot.MinRequests)
		assert.Less(t, snapshot.LowTTFT, snapshot.HighTTFT, "P25 must sit below P50")
		assert.Less(t, snapshot.InflightAtLow, snapshot.InflightAtHigh,
			"the faster band must be the less loaded one")
	})

	t.Run("an endpoint with no observations publishes an empty snapshot", func(t *testing.T) {
		p := newObserverWithConfig(t, flushConfig())
		p.stateFor("default/a")

		p.flushAll(now)

		snapshot := p.stateFor("default/a").published.Load()
		require.NotNil(t, snapshot)
		assert.Zero(t, snapshot.RecentN)
		assert.Zero(t, snapshot.Floor(), "no observations means cold")
	})

	t.Run("the floor stays zero until a bucket closes", func(t *testing.T) {
		p := newObserverWithConfig(t, flushConfig())
		for i := range 10 {
			p.record("default/a", 0.5, 1, now.Add(time.Duration(i)*time.Millisecond))
		}

		// First flush only starts the bucket clock.
		p.flushAll(now)
		assert.Zero(t, p.stateFor("default/a").published.Load().P10LowTTFT)
	})

	t.Run("a closed bucket sets the floor from its low percentile", func(t *testing.T) {
		p := newObserverWithConfig(t, flushConfig())
		// A fast tail and a slow bulk: the floor must follow the fast tail.
		for i := range 10 {
			ttft := 1.0
			if i < 2 {
				ttft = 0.1
			}
			p.record("default/a", ttft, 1, now.Add(time.Duration(i)*time.Millisecond))
		}

		// Roll exactly one bucket later: the bucket window looks back
		// bucketDuration, so flushing further out would leave the observations
		// behind its cutoff and close an empty bucket.
		p.flushAll(now)                  // starts the bucket
		p.flushAll(now.Add(time.Second)) // bucket elapsed -> rolls

		snapshot := p.stateFor("default/a").published.Load()
		require.NotNil(t, snapshot)
		assert.Greater(t, snapshot.P10LowTTFT, 0.0)
		assert.Less(t, snapshot.P10LowTTFT, 1.0, "the floor must track the fast requests, not the bulk")
	})

	t.Run("the floor is gated until minRequests observations", func(t *testing.T) {
		p := newObserverWithConfig(t, flushConfig()) // minRequests 4
		for i := range 2 {
			p.record("default/a", 0.1, 1, now.Add(time.Duration(i)*time.Millisecond))
		}

		p.flushAll(now)
		p.flushAll(now.Add(time.Second))

		snapshot := p.stateFor("default/a").published.Load()
		require.NotNil(t, snapshot)
		assert.Greater(t, snapshot.P10LowTTFT, 0.0, "the raw floor is computed")
		assert.Zero(t, snapshot.Floor(), "but Floor() withholds it below minRequests")
	})

	t.Run("the bucket history is bounded", func(t *testing.T) {
		p := newObserverWithConfig(t, flushConfig()) // bucketHistorySize 4
		state := p.stateFor("default/a")

		at := now
		for range 20 {
			for i := range 5 {
				p.record("default/a", 0.1+float64(i)*0.1, 1, at.Add(time.Duration(i)*time.Millisecond))
			}
			p.flushAll(at)
			at = at.Add(2 * time.Second)
		}

		state.mu.Lock()
		defer state.mu.Unlock()
		assert.LessOrEqual(t, len(state.bucketP10s), 4)
	})

	t.Run("flushAll covers every known endpoint", func(t *testing.T) {
		p := newObserverWithConfig(t, flushConfig())
		p.record("default/a", 0.2, 1, now)
		p.record("default/b", 0.4, 2, now)

		p.flushAll(now.Add(time.Second))

		assert.NotNil(t, p.stateFor("default/a").published.Load())
		assert.NotNil(t, p.stateFor("default/b").published.Load())
	})
}

func TestFlushLoopStopsWithContext(t *testing.T) {
	cfg := flushConfig()
	cfg.IntervalDuration = "10ms"

	ctx, cancel := context.WithCancel(context.Background())
	p, err := NewObserver(ctx, "observer", cfg)
	require.NoError(t, err)
	p.record("default/a", 0.3, 1, time.Now())

	done := make(chan struct{})
	go func() {
		p.flushLoop(ctx)
		close(done)
	}()

	// The ticker must publish without anyone calling flushAll.
	require.Eventually(t, func() bool {
		return p.stateFor("default/a").published.Load() != nil
	}, time.Second, 5*time.Millisecond)

	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("flushLoop did not exit when its context was cancelled")
	}
}

// The observer feeds the scorer: an endpoint that has served enough requests
// publishes anchors that make it trusted rather than cold.
func TestFlushProducesATrustedSnapshot(t *testing.T) {
	now := time.Now()
	p := newObserverWithConfig(t, flushConfig())

	// Spread across in-flight counts so the two operating anchors separate.
	for i := range 16 {
		p.record("default/a", 0.2+float64(i)*0.05, int64(i), now.Add(time.Duration(i)*time.Millisecond))
	}
	p.flushAll(now)
	p.flushAll(now.Add(time.Second))

	snapshot := p.stateFor("default/a").published.Load()
	require.NotNil(t, snapshot)

	assert.GreaterOrEqual(t, snapshot.RecentN, snapshot.MinRequests)
	assert.Greater(t, snapshot.InflightAtHigh, 0.0)
	assert.Greater(t, snapshot.Floor(), 0.0, "a closed bucket gives a real floor")
	assert.Greater(t, snapshot.HighTTFT, snapshot.Floor())
}
