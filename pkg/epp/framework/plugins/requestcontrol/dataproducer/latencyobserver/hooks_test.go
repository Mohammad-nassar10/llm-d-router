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
	"k8s.io/apimachinery/pkg/types"

	fwkdl "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/datalayer"
	fwkplugin "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/plugin"
	"github.com/llm-d/llm-d-router/pkg/epp/framework/interface/requestcontrol"
	fwksched "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/scheduling"
	attrconcurrency "github.com/llm-d/llm-d-router/pkg/epp/framework/plugins/datalayer/attribute/concurrency"
)

const testProfile = "default"

func newObserver(t *testing.T) *Observer {
	t.Helper()
	return newObserverWithConfig(t, DefaultConfig)
}

func newObserverWithConfig(t *testing.T, cfg Config) *Observer {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	observer, err := NewObserver(ctx, "ttft-observer", cfg)
	require.NoError(t, err)
	return observer
}

// newSchedEndpoint builds a scheduling endpoint named id, optionally carrying a
// live in-flight load attribute.
func newSchedEndpoint(id string, inflight *int64) fwksched.Endpoint {
	attr := fwkdl.NewAttributes()
	if inflight != nil {
		attr.Put(attrconcurrency.InFlightLoadDataKey.String(), &attrconcurrency.InFlightLoad{Requests: *inflight})
	}
	meta := &fwkdl.EndpointMetadata{ID: types.NamespacedName{Name: id, Namespace: "default"}}
	return fwksched.NewEndpoint(meta, nil, attr)
}

func inflightPtr(v int64) *int64 { return &v }

func resultFor(endpoint fwksched.Endpoint) *fwksched.SchedulingResult {
	return &fwksched.SchedulingResult{
		PrimaryProfileName: testProfile,
		ProfileResults: map[string]*fwksched.ProfileRunResult{
			testProfile: {TargetEndpoints: []fwksched.Endpoint{endpoint}},
		},
	}
}

func TestProduce(t *testing.T) {
	ctx := context.Background()

	t.Run("captures each candidate's live in-flight count", func(t *testing.T) {
		p := newObserver(t)
		a := newSchedEndpoint("a", inflightPtr(7))
		b := newSchedEndpoint("b", inflightPtr(0))

		require.NoError(t, p.Produce(ctx, nil, []fwksched.Endpoint{a, b}))

		for _, tc := range []struct {
			endpoint fwksched.Endpoint
			want     int64
		}{{a, 7}, {b, 0}} {
			raw, ok := tc.endpoint.Get(p.inflightAtDispatchDataKey.String())
			require.True(t, ok)
			captured, ok := raw.(*attrconcurrency.InFlightLoad)
			require.True(t, ok)
			assert.Equal(t, tc.want, captured.Requests)
		}
	})

	t.Run("a missing in-flight attribute captures zero", func(t *testing.T) {
		p := newObserver(t)
		endpoint := newSchedEndpoint("a", nil)

		require.NoError(t, p.Produce(ctx, nil, []fwksched.Endpoint{endpoint}))

		raw, ok := endpoint.Get(p.inflightAtDispatchDataKey.String())
		require.True(t, ok)
		assert.Zero(t, raw.(*attrconcurrency.InFlightLoad).Requests)
	})

	t.Run("tolerates nil endpoints", func(t *testing.T) {
		p := newObserver(t)
		require.NoError(t, p.Produce(ctx, nil, []fwksched.Endpoint{nil}))
	})

	t.Run("declares its published and consumed keys", func(t *testing.T) {
		p := newObserver(t)
		assert.Contains(t, p.Produces(), p.percentilesDataKey)
		assert.Contains(t, p.Consumes().Required, p.inFlightLoadDataKey)
	})
}

func TestPreRequest(t *testing.T) {
	ctx := context.Background()

	t.Run("records the winner's captured in-flight count", func(t *testing.T) {
		p := newObserver(t)
		winner := newSchedEndpoint("winner", inflightPtr(4))
		loser := newSchedEndpoint("loser", inflightPtr(11))
		request := &fwksched.InferenceRequest{RequestID: "req-1"}

		require.NoError(t, p.Produce(ctx, request, []fwksched.Endpoint{winner, loser}))
		p.PreRequest(ctx, request, resultFor(winner))

		dispatch, err := fwkplugin.ReadPluginStateKey[*dispatchInfo](p.PluginState, "req-1", dispatchStateKey)
		require.NoError(t, err)
		assert.Equal(t, "default/winner", dispatch.endpointID)
		assert.Equal(t, int64(4), dispatch.inflight, "must be the winner's count, not the loser's")
		assert.False(t, dispatch.dispatchedAt.IsZero())
	})

	t.Run("without Produce the in-flight anchor defaults to zero", func(t *testing.T) {
		p := newObserver(t)
		winner := newSchedEndpoint("winner", inflightPtr(4))
		request := &fwksched.InferenceRequest{RequestID: "req-1"}

		p.PreRequest(ctx, request, resultFor(winner))

		dispatch, err := fwkplugin.ReadPluginStateKey[*dispatchInfo](p.PluginState, "req-1", dispatchStateKey)
		require.NoError(t, err)
		assert.Zero(t, dispatch.inflight)
	})

	t.Run("writes nothing when there is no usable dispatch", func(t *testing.T) {
		endpoint := newSchedEndpoint("a", inflightPtr(1))
		tests := map[string]struct {
			request *fwksched.InferenceRequest
			result  *fwksched.SchedulingResult
		}{
			"nil request":     {nil, resultFor(endpoint)},
			"empty requestID": {&fwksched.InferenceRequest{}, resultFor(endpoint)},
			"nil result":      {&fwksched.InferenceRequest{RequestID: "req-1"}, nil},
			"no profiles": {&fwksched.InferenceRequest{RequestID: "req-1"},
				&fwksched.SchedulingResult{PrimaryProfileName: testProfile}},
			"primary picked no endpoint": {&fwksched.InferenceRequest{RequestID: "req-1"},
				&fwksched.SchedulingResult{
					PrimaryProfileName: testProfile,
					ProfileResults:     map[string]*fwksched.ProfileRunResult{testProfile: {}},
				}},
		}
		for name, tc := range tests {
			t.Run(name, func(t *testing.T) {
				p := newObserver(t)
				p.PreRequest(ctx, tc.request, tc.result)

				_, err := fwkplugin.ReadPluginStateKey[*dispatchInfo](p.PluginState, "req-1", dispatchStateKey)
				assert.Error(t, err)
			})
		}
	})
}

func TestResponseBody(t *testing.T) {
	ctx := context.Background()

	// dispatch primes a request as if Produce and PreRequest had run.
	dispatch := func(p *Observer, requestID, endpointID string, inflight int64, at time.Time) {
		p.PluginState.Write(requestID, dispatchStateKey, &dispatchInfo{
			endpointID: endpointID, inflight: inflight, dispatchedAt: at,
		})
	}

	t.Run("the first chunk of a stream becomes a TTFT observation", func(t *testing.T) {
		p := newObserver(t)
		dispatch(p, "req-1", "default/a", 6, time.Now().Add(-250*time.Millisecond))

		p.ResponseBody(ctx, &fwksched.InferenceRequest{RequestID: "req-1"},
			&requestcontrol.Response{StartOfStream: true}, nil)

		state := p.stateFor("default/a")
		state.mu.Lock()
		defer state.mu.Unlock()
		assert.Equal(t, int64(1), state.observations)
		assert.Equal(t, int64(6), state.lastInflightAtDispatch)
		assert.InDelta(t, 0.25, state.lastTTFT, 0.15)
	})

	t.Run("middle chunks are ignored", func(t *testing.T) {
		p := newObserver(t)
		dispatch(p, "req-1", "default/a", 6, time.Now())

		p.ResponseBody(ctx, &fwksched.InferenceRequest{RequestID: "req-1"},
			&requestcontrol.Response{}, nil)

		state := p.stateFor("default/a")
		state.mu.Lock()
		defer state.mu.Unlock()
		assert.Zero(t, state.observations)
	})

	t.Run("a single-chunk response is discarded", func(t *testing.T) {
		p := newObserver(t)
		dispatch(p, "req-1", "default/a", 6, time.Now().Add(-2*time.Second))

		// StartOfStream and EndOfStream together: the whole body arrived at
		// once, so its latency is end-to-end and not a TTFT. Recording it would
		// push both the floor and the operating anchors far above the truth.
		p.ResponseBody(ctx, &fwksched.InferenceRequest{RequestID: "req-1"},
			&requestcontrol.Response{StartOfStream: true, EndOfStream: true}, nil)

		state := p.stateFor("default/a")
		state.mu.Lock()
		defer state.mu.Unlock()
		assert.Zero(t, state.observations, "e2e latency must not be recorded as a TTFT")
		assert.Zero(t, state.lastTTFT)
	})

	t.Run("end of stream releases the dispatch record", func(t *testing.T) {
		p := newObserver(t)
		dispatch(p, "req-1", "default/a", 6, time.Now())
		request := &fwksched.InferenceRequest{RequestID: "req-1"}

		p.ResponseBody(ctx, request, &requestcontrol.Response{StartOfStream: true}, nil)
		p.ResponseBody(ctx, request, &requestcontrol.Response{EndOfStream: true}, nil)

		_, err := fwkplugin.ReadPluginStateKey[*dispatchInfo](p.PluginState, "req-1", dispatchStateKey)
		assert.Error(t, err, "dispatch record must not outlive the request")
	})

	t.Run("a response with no dispatch record is a no-op", func(t *testing.T) {
		p := newObserver(t)

		p.ResponseBody(ctx, &fwksched.InferenceRequest{RequestID: "unknown"},
			&requestcontrol.Response{StartOfStream: true}, nil)

		assert.Empty(t, p.snapshotDebugState().Endpoints)
	})

	t.Run("tolerates nil arguments", func(t *testing.T) {
		p := newObserver(t)
		p.ResponseBody(ctx, nil, &requestcontrol.Response{StartOfStream: true}, nil)
		p.ResponseBody(ctx, &fwksched.InferenceRequest{RequestID: "req-1"}, nil, nil)
		p.ResponseBody(ctx, &fwksched.InferenceRequest{}, &requestcontrol.Response{StartOfStream: true}, nil)
	})
}

// The full path: Produce captures the load, PreRequest pins it to the winner,
// and the first response chunk turns it into an observation on that endpoint.
func TestObserveEndToEnd(t *testing.T) {
	ctx := context.Background()
	p := newObserver(t)

	winner := newSchedEndpoint("winner", inflightPtr(9))
	loser := newSchedEndpoint("loser", inflightPtr(2))
	request := &fwksched.InferenceRequest{RequestID: "req-1"}

	require.NoError(t, p.Produce(ctx, request, []fwksched.Endpoint{winner, loser}))
	p.PreRequest(ctx, request, resultFor(winner))
	p.ResponseBody(ctx, request, &requestcontrol.Response{StartOfStream: true}, nil)
	p.ResponseBody(ctx, request, &requestcontrol.Response{EndOfStream: true}, nil)

	dump := p.snapshotDebugState()
	require.Len(t, dump.Endpoints, 1, "only the endpoint that served the request is observed")
	assert.Equal(t, "default/winner", dump.Endpoints[0].Endpoint)
	assert.Equal(t, int64(1), dump.Endpoints[0].Observations)
	assert.Equal(t, int64(9), dump.Endpoints[0].LastInflightAtDispatch)
}
