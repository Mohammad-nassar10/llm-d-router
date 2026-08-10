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

// TTFTPercentilesDataKey carries the per-endpoint TTFT distribution summary the
// ttft-aware-scorer reads. Published by the latency-observer-producer, which
// recomputes it on its own cadence rather than per request.
var TTFTPercentilesDataKey = plugin.NewDataKey("TTFTPercentilesDataKey", observerconstants.LatencyObserverProducerType)

// TTFTPercentiles summarises an endpoint's recent time-to-first-token
// distribution as the three points of a TTFT-vs-load curve, plus the evidence
// needed to decide whether that curve can be trusted.
//
//	A — (0, P10LowTTFT)                 the queue-free service floor
//	B — (InflightAtLow,  LowTTFT)       low operating point  (default P25)
//	C — (InflightAtHigh, HighTTFT)      high operating point (default P50)
//
// The current in-flight count is deliberately NOT a field here. It is read live
// from the InFlightLoad attribute at scoring time, so the prediction is
// evaluated at the endpoint's load right now rather than at its load when this
// snapshot was last computed.
//
// All TTFT values are in seconds.
type TTFTPercentiles struct {
	// P10LowTTFT is the load-invariant service floor: a low percentile over a
	// history of per-bucket low percentiles. Zero until enough observations
	// have accumulated, which reads as "cold". See Floor.
	P10LowTTFT float64

	// P10TTFT is the 10th percentile over the short window. Used as the floor
	// before the bucket history has filled.
	P10TTFT float64

	// LowTTFT and HighTTFT are the TTFTs at the low and high operating
	// percentiles (default P25 and P50) over the short window.
	LowTTFT  float64
	HighTTFT float64

	// InflightAtLow and InflightAtHigh are the average in-flight-at-dispatch
	// counts in the percentile bands around LowTTFT and HighTTFT. Averaging a
	// band rather than a single observation stabilises the estimate.
	InflightAtLow  float64
	InflightAtHigh float64

	// RecentN is the number of observations in the short window. Gates whether
	// the operating points are trusted.
	RecentN int

	// Observations is the cumulative count that has fed the floor. Gates Floor.
	// Cumulative rather than windowed so an endpoint that has already
	// calibrated does not read as cold again after going briefly idle.
	Observations int64

	// MinRequests is the observation threshold, copied from the producer's
	// config so consumers need no separate parameter to interpret RecentN and
	// Observations.
	MinRequests int
}

// Floor returns the load-invariant service floor: P10LowTTFT, or P10TTFT before
// the bucket history has filled.
//
// Zero means the endpoint is cold. An endpoint observed fewer than MinRequests
// times is also reported cold: a percentile over a handful of cold-start
// requests is noise, and returning it would let a barely-observed endpoint
// compete on a value that happens to look good.
func (m *TTFTPercentiles) Floor() float64 {
	if m == nil || m.Observations < int64(m.MinRequests) {
		return 0
	}
	if m.P10LowTTFT > 0 {
		return m.P10LowTTFT
	}
	return m.P10TTFT
}

// Clone returns an independent copy. The value-copy idiom covers every field
// automatically; new fields need no change here as long as they stay value
// types.
func (m *TTFTPercentiles) Clone() fwkdl.Cloneable {
	if m == nil {
		return nil
	}
	cp := *m
	return &cp
}
