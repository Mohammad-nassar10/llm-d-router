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

package epp

import (
	_ "embed"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"

	fwkplugin "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/plugin"
	integration "github.com/llm-d/llm-d-router/test/integration"
)

//go:embed testdata/ttft-aware-config.yaml
var ttftAwareTestConfig string

// observerEndpointState mirrors the JSON the latency observer publishes through
// StateDumper. Declared here rather than exported from the plugin so the debug
// payload stays an operator-facing contract rather than a Go API.
type observerEndpointState struct {
	Endpoint               string  `json:"endpoint"`
	LastTTFTSeconds        float64 `json:"lastTtftSeconds"`
	LastInflightAtDispatch int64   `json:"lastInflightAtDispatch"`
	Observations           int64   `json:"observations"`
	FloorSeconds           float64 `json:"floorSeconds"`
	RecentN                int     `json:"recentN"`
}

type observerState struct {
	Endpoints      []observerEndpointState `json:"endpoints"`
	TotalEndpoints int                     `json:"totalEndpoints"`
}

// dumpObserverState reads the observer's state straight out of the running EPP.
func dumpObserverState(t *testing.T, h *TestHarness) observerState {
	t.Helper()

	plugin := h.Runner.PluginHandle.Plugin("ttft-observer")
	require.NotNil(t, plugin, "the latency observer must be instantiated")

	dumper, ok := plugin.(fwkplugin.StateDumper)
	require.True(t, ok, "the latency observer must implement StateDumper")

	raw, err := dumper.DumpState()
	require.NoError(t, err)

	var state observerState
	require.NoError(t, json.Unmarshal(raw, &state))
	return state
}

// servedEndpoint returns the single endpoint that actually handled traffic.
//
// The observer allocates state for every endpoint the moment the datalayer
// announces it, because that is when it attaches its published attribute. So
// idle endpoints are present with zeroed counters, and a test that sent one
// request must look for the one that recorded something.
func servedEndpoint(t *testing.T, state observerState) observerEndpointState {
	t.Helper()

	var served []observerEndpointState
	for _, endpoint := range state.Endpoints {
		if endpoint.Observations > 0 {
			served = append(served, endpoint)
		}
	}
	require.Len(t, served, 1, "exactly one endpoint should have handled the request, got %+v", state.Endpoints)
	return served[0]
}

// TestTTFTAwareObservesStreamingResponse drives a full ext_proc transaction and
// checks that the observer turns the first response chunk into a TTFT.
//
// This is the one thing unit tests cannot show: that the director actually
// delivers ResponseBody with StartOfStream through its async response-body
// queue, and that the elapsed time measured from dispatch is a real number.
func TestTTFTAwareObservesStreamingResponse(t *testing.T) {
	ctx := t.Context()

	h := NewTestHarness(ctx, t, WithStandardMode(), WithConfigText(ttftAwareTestConfig)).WithBaseResources()

	pods := []PodState{P(0, 0, 0.1, modelMyModelTarget), P(1, 0, 0.1, modelMyModelTarget)}
	h.WithPods(pods).WaitForSync(len(pods), modelMyModel)
	h.WaitForReadyPodsMetric(len(pods))

	requests := integration.ReqLLM(reqLogger, "hello", "modelName", "modelName")
	requests = append(requests, ReqResponseOnly(
		map[string]string{"content-type": "text/event-stream", "status": "200"},
		`data: {"choices":[{"delta":{"content":"Hello! "},"index":0,"finish_reason":null}],"id":"1","created":1,"model":"modelName","object":"chat.completion.chunk"}
`,
		`data: {"choices":[{"delta":{"content":"How are you?"},"index":0,"finish_reason":null}],"id":"1","created":1,"model":"modelName","object":"chat.completion.chunk"}
`,
		`data: {"choices":[{"delta":{},"index":0,"finish_reason":"stop"}],"id":"1","created":1,"model":"modelName","object":"chat.completion.chunk","usage":{"completion_tokens":10,"prompt_tokens":32,"total_tokens":42}}
`,
	)...)

	// RequestHeaders, RequestBody, ResponseHeaders, and three body chunks.
	_, err := integration.StreamedRequest(t, h.Client, requests, 6)
	require.NoError(t, err)

	// The final chunk drains the async response-body queue before returning, so
	// the first chunk has certainly been processed by now.
	state := dumpObserverState(t, h)
	require.Len(t, state.Endpoints, len(pods), "every known endpoint has state, served or not")

	observed := servedEndpoint(t, state)
	require.Equal(t, int64(1), observed.Observations, "the first chunk must produce one TTFT observation")
	require.Positive(t, observed.LastTTFTSeconds, "TTFT must be a positive elapsed time")
	require.Less(t, observed.LastTTFTSeconds, 60.0, "TTFT must be plausible, not a garbage timestamp delta")
}

// A response that arrives as a single chunk has no time-to-first-token: its
// latency is end-to-end. The observer must discard it, because feeding e2e
// latency into the percentiles would inflate both the floor and the operating
// anchors. A non-streaming deployment therefore records nothing at all and
// every endpoint stays cold, which is the documented limitation.
func TestTTFTAwareDiscardsNonStreamingResponse(t *testing.T) {
	ctx := t.Context()

	h := NewTestHarness(ctx, t, WithStandardMode(), WithConfigText(ttftAwareTestConfig)).WithBaseResources()

	pods := []PodState{P(0, 0, 0.1, modelMyModelTarget)}
	h.WithPods(pods).WaitForSync(len(pods), modelMyModel)
	h.WaitForReadyPodsMetric(len(pods))

	requests := integration.ReqLLM(reqLogger, "hello", "modelName", "modelName")
	requests = append(requests, ReqResponseOnly(
		map[string]string{"content-type": "application/json", "status": "200"},
		`{"choices":[{"finish_reason":"stop","index":0,"message":{"content":"Hi","role":"assistant"}}],"created":1,"id":"1","model":"modelName","object":"chat.completion","usage":{"completion_tokens":10,"prompt_tokens":32,"total_tokens":42}}`,
	)...)

	// RequestHeaders, RequestBody, ResponseHeaders, one body chunk.
	_, err := integration.StreamedRequest(t, h.Client, requests, 4)
	require.NoError(t, err)

	state := dumpObserverState(t, h)
	require.Len(t, state.Endpoints, len(pods), "the endpoint is still known to the observer")
	for _, endpoint := range state.Endpoints {
		require.Zero(t, endpoint.Observations, "end-to-end latency must not be recorded as a TTFT")
		require.Zero(t, endpoint.LastTTFTSeconds)
		require.Zero(t, endpoint.FloorSeconds, "with no observations the endpoint reads cold")
	}
}
