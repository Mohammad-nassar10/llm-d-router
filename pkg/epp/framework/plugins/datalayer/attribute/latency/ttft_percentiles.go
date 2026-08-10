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

package latency

import (
	fwkdl "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/datalayer"
	"github.com/llm-d/llm-d-router/pkg/epp/framework/interface/plugin"
	observerconstants "github.com/llm-d/llm-d-router/pkg/epp/framework/plugins/requestcontrol/dataproducer/latencyobserver/constants"
)

// TTFTPercentilesDataKey carries the per-endpoint TTFT summary the
// ttft-aware-scorer reads, published by the latency-observer-producer.
var TTFTPercentilesDataKey = plugin.NewDataKey("TTFTPercentilesDataKey", observerconstants.LatencyObserverProducerType)

// TTFTPercentiles summarises an endpoint's recent time-to-first-token
// distribution. All TTFT values are in seconds.
//
// The current in-flight count is deliberately not a field: the scorer reads it
// live from the InFlightLoad attribute, so its prediction reflects the load
// right now rather than the load when this snapshot was computed.
type TTFTPercentiles struct {
	// Load-invariant service floor: a low percentile over a history of
	// per-bucket low percentiles. See Floor.
	P10LowTTFT float64
	// Short-window P10, used as the floor before the bucket history fills.
	P10TTFT float64

	// TTFT at the low and high operating percentiles, default P25 and P50.
	LowTTFT  float64
	HighTTFT float64
	// Average in-flight-at-dispatch in the bands around them. A band rather
	// than a single observation stabilises the estimate.
	InflightAtLow  float64
	InflightAtHigh float64

	// Observations in the short window; gates whether the operating points are
	// trusted.
	RecentN int
	// Cumulative count feeding the floor, so an endpoint that has already
	// calibrated does not read cold again after going briefly idle.
	Observations int64
	// Threshold copied from the producer's config, so consumers need no
	// separate parameter to interpret RecentN and Observations.
	MinRequests int
}

// Floor returns the service floor, or zero when the endpoint is cold.
//
// Fewer than MinRequests observations also reads as cold: a percentile over a
// handful of cold-start requests is noise, and returning it would let a
// barely-observed endpoint compete on a value that happens to look good.
func (m *TTFTPercentiles) Floor() float64 {
	if m == nil || m.Observations < int64(m.MinRequests) {
		return 0
	}
	if m.P10LowTTFT > 0 {
		return m.P10LowTTFT
	}
	return m.P10TTFT
}

// Clone returns an independent copy.
func (m *TTFTPercentiles) Clone() fwkdl.Cloneable {
	if m == nil {
		return nil
	}
	cp := *m
	return &cp
}
