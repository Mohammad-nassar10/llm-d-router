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

package runner

import (
	"context"
	"slices"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/llm-d/llm-d-router/pkg/epp/datalayer"
	"github.com/llm-d/llm-d-router/pkg/epp/datastore"
	fwkplugin "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/plugin"
	inflightloadconstants "github.com/llm-d/llm-d-router/pkg/epp/framework/plugins/requestcontrol/dataproducer/inflightload/constants"
	observerconstants "github.com/llm-d/llm-d-router/pkg/epp/framework/plugins/requestcontrol/dataproducer/latencyobserver/constants"
	"github.com/llm-d/llm-d-router/pkg/epp/framework/plugins/scheduling/scorer/ttftaware"
	runserver "github.com/llm-d/llm-d-router/pkg/epp/server"
)

// loadConfig runs both configuration phases over configText, returning the
// runner so callers can inspect the instantiated plugin set.
func loadConfig(t *testing.T, configText string) *Runner {
	t.Helper()

	opts := runserver.NewOptions()
	opts.ConfigText = configText
	opts.PoolName = "ttft-config-pool"
	opts.PoolNamespace = "ttft-config-ns"
	opts.AllowExperimentalPlugins = true

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	r := NewRunner()
	rawConfig, err := r.parseConfigurationPhaseOne(ctx, opts)
	require.NoError(t, err, "config must parse and validate")

	ds := datastore.NewDatastore(ctx, r.setupMetricsCollection(opts))
	_, err = r.parseConfigurationPhaseTwo(ctx, rawConfig, ds)
	require.NoError(t, err, "config must instantiate against the registry")

	return r
}

// pluginNamed returns the instantiated plugin with the given name.
func pluginNamed(t *testing.T, r *Runner, name string) fwkplugin.Plugin {
	t.Helper()
	p := r.PluginHandle.Plugin(name)
	require.NotNil(t, p, "plugin %q must resolve", name)
	return p
}

// TestTTFTAwareScorerAutoWiresItsProducers is the wiring contract for the
// TTFT-aware routing stack: configuring the scorer alone is enough.
//
// The scorer declares TTFTPercentiles as a Required data key, so the framework
// auto-creates latency-observer-producer for it. That producer in turn declares
// InFlightLoad as Required, so inflight-load-producer is created transitively.
// Nothing here is named in the config but the scorer itself.
func TestTTFTAwareScorerAutoWiresItsProducers(t *testing.T) {
	r := loadConfig(t, `apiVersion: llm-d.ai/v1alpha1
kind: EndpointPickerConfig
plugins:
  - type: ttft-aware-scorer
    name: ttft
  - type: max-score-picker
  - type: single-profile-handler
schedulingProfiles:
  - name: default
    plugins:
      - pluginRef: ttft
      - pluginRef: max-score-picker
`)

	scorer := pluginNamed(t, r, "ttft")
	assert.Equal(t, ttftaware.ScorerType, scorer.TypedName().Type)

	observer := pluginNamed(t, r, observerconstants.LatencyObserverProducerType)
	assert.Equal(t, observerconstants.LatencyObserverProducerType, observer.TypedName().Type)

	inflight := pluginNamed(t, r, inflightloadconstants.InFlightLoadProducerType)
	assert.Equal(t, inflightloadconstants.InFlightLoadProducerType, inflight.TypedName().Type)
}

// The data-layer DAG must order the in-flight producer ahead of the observer:
// the observer reads the live in-flight count in its Produce hook, so the
// producer that publishes that attribute has to have run first.
func TestTTFTAwareProducerOrdering(t *testing.T) {
	r := loadConfig(t, `apiVersion: llm-d.ai/v1alpha1
kind: EndpointPickerConfig
plugins:
  - type: ttft-aware-scorer
    name: ttft
  - type: max-score-picker
  - type: single-profile-handler
schedulingProfiles:
  - name: default
    plugins:
      - pluginRef: ttft
      - pluginRef: max-score-picker
`)

	order, err := datalayer.ValidateAndOrderDataDependencies(r.PluginHandle.GetAllPlugins())
	require.NoError(t, err, "the data dependencies must form an acyclic graph")

	indexOf := func(pluginType string) int {
		return slices.IndexFunc(order, func(entry string) bool {
			return len(entry) >= len(pluginType) && entry[:len(pluginType)] == pluginType
		})
	}

	inflightIdx := indexOf(inflightloadconstants.InFlightLoadProducerType)
	observerIdx := indexOf(observerconstants.LatencyObserverProducerType)
	require.NotEqual(t, -1, inflightIdx, "inflight-load-producer must appear in the DAG order: %v", order)
	require.NotEqual(t, -1, observerIdx, "latency-observer-producer must appear in the DAG order: %v", order)

	assert.Less(t, inflightIdx, observerIdx,
		"inflight-load-producer must run before latency-observer-producer, got order %v", order)
}

// An explicitly configured observer is used instead of an auto-created one, and
// its parameters are honored.
func TestTTFTAwareExplicitObserverConfig(t *testing.T) {
	r := loadConfig(t, `apiVersion: llm-d.ai/v1alpha1
kind: EndpointPickerConfig
plugins:
  - type: latency-observer-producer
    name: observer
    parameters:
      intervalDuration: 500ms
      minRequests: 20
      lowPercentile: 20
      highPercentile: 60
  - type: ttft-aware-scorer
    name: ttft
    parameters:
      explorationRate: 0.2
      minInflightGap: 3
      ttftPercentilesProducerName: observer
  - type: max-score-picker
  - type: single-profile-handler
schedulingProfiles:
  - name: default
    plugins:
      - pluginRef: ttft
      - pluginRef: max-score-picker
`)

	observer := pluginNamed(t, r, "observer")
	assert.Equal(t, observerconstants.LatencyObserverProducerType, observer.TypedName().Type)

	// No second observer was auto-created: the scorer's producer-name override
	// points its required key at this instance.
	assert.Nil(t, r.PluginHandle.Plugin(observerconstants.LatencyObserverProducerType),
		"the configured observer must satisfy the scorer's required key on its own")
}

// Invalid parameters must fail at load time rather than at the first request.
func TestTTFTAwareConfigRejectsInvalidParameters(t *testing.T) {
	tests := map[string]string{
		"exploration rate above one": `
  - type: ttft-aware-scorer
    name: ttft
    parameters:
      explorationRate: 1.5`,
		"non-positive inflight gap": `
  - type: ttft-aware-scorer
    name: ttft
    parameters:
      minInflightGap: 0`,
		"inverted percentiles": `
  - type: latency-observer-producer
    name: observer
    parameters:
      lowPercentile: 60
      highPercentile: 20`,
		"unparseable interval": `
  - type: latency-observer-producer
    name: observer
    parameters:
      intervalDuration: soon`,
	}

	for name, pluginBlock := range tests {
		t.Run(name, func(t *testing.T) {
			opts := runserver.NewOptions()
			opts.ConfigText = `apiVersion: llm-d.ai/v1alpha1
kind: EndpointPickerConfig
plugins:` + pluginBlock + `
  - type: max-score-picker
  - type: single-profile-handler
`
			opts.PoolName = "ttft-config-pool"
			opts.PoolNamespace = "ttft-config-ns"
			opts.AllowExperimentalPlugins = true

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			// Either phase may reject; what matters is that neither accepts.
			r := NewRunner()
			rawConfig, err := r.parseConfigurationPhaseOne(ctx, opts)
			if err == nil {
				ds := datastore.NewDatastore(ctx, r.setupMetricsCollection(opts))
				_, err = r.parseConfigurationPhaseTwo(ctx, rawConfig, ds)
			}
			require.Error(t, err, "invalid parameters must be rejected at load time")
		})
	}
}

// Both plugins are Alpha, so a deployment must opt in explicitly.
func TestTTFTAwarePluginsAreExperimental(t *testing.T) {
	r := loadConfig(t, `apiVersion: llm-d.ai/v1alpha1
kind: EndpointPickerConfig
plugins:
  - type: ttft-aware-scorer
    name: ttft
  - type: max-score-picker
  - type: single-profile-handler
schedulingProfiles:
  - name: default
    plugins:
      - pluginRef: ttft
      - pluginRef: max-score-picker
`)

	require.Error(t, fwkplugin.ValidatePluginStability(r.PluginHandle, false),
		"Alpha plugins must be rejected without --allow-experimental-plugins")
	require.NoError(t, fwkplugin.ValidatePluginStability(r.PluginHandle, true))
}
