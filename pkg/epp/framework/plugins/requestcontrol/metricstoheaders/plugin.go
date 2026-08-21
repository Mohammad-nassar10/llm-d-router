/*
Copyright 2026 The llm-d Authors.

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

// Package metricstoheaders echoes the load metrics of the endpoint that served a
// request back to the caller as response headers. It lets a client adapt to
// backend pressure -- shedding, backing off, or compacting its prompt -- without
// scraping Prometheus or knowing anything about the pool topology.
package metricstoheaders

import (
	"context"
	"encoding/json"
	"strconv"
	"time"

	"sigs.k8s.io/controller-runtime/pkg/log"

	"github.com/llm-d/llm-d-router/pkg/common/observability/logging"
	fwkdl "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/datalayer"
	fwkplugin "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/plugin"
	"github.com/llm-d/llm-d-router/pkg/epp/framework/interface/requestcontrol"
	fwksched "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/scheduling"
)

const (
	// MetricsToHeadersType is the ResponseHeader plugin type used in the plugins registry.
	MetricsToHeadersType = "metrics-to-headers"

	// KVCacheUtilizationHeader carries the serving endpoint's KV cache usage as
	// reported by the model server. vLLM reports a fraction in [0,1], not a percentage.
	KVCacheUtilizationHeader = "x-llm-d-kv-cache-utilization"
	// WaitingQueueHeader carries the number of requests queued on the serving endpoint.
	WaitingQueueHeader = "x-llm-d-waiting-queue"
	// RunningRequestsHeader carries the number of requests in flight on the serving endpoint.
	RunningRequestsHeader = "x-llm-d-running-requests"
	// MetricsAgeHeader carries the age of the snapshot in milliseconds. Metrics are
	// scraped on an interval, so a caller acting on the values above needs to know
	// how old they are. Omitted when the endpoint has never been scraped.
	MetricsAgeHeader = "x-llm-d-metrics-age-ms"
)

var _ requestcontrol.ResponseHeaderProcessor = &MetricsToHeaders{}

// MetricsToHeaders is a ResponseHeaderProcessor that reports the serving
// endpoint's latest metrics snapshot to the client.
//
// It reports the endpoint that served this request rather than a pool-wide
// aggregate. That is the pressure that actually shaped this response, and under
// session affinity it is also the endpoint most likely to serve the next turn.
type MetricsToHeaders struct {
	typedName fwkplugin.TypedName
}

// TypedName returns the type and name tuple of this plugin instance.
func (m *MetricsToHeaders) TypedName() fwkplugin.TypedName {
	return m.typedName
}

// WithName sets the name of the plugin.
func (m *MetricsToHeaders) WithName(name string) *MetricsToHeaders {
	m.typedName.Name = name
	return m
}

// Factory defines the factory function for MetricsToHeaders.
func Factory(name string, _ *json.Decoder, _ fwkplugin.Handle) (fwkplugin.Plugin, error) {
	return New().WithName(name), nil
}

// New returns a MetricsToHeaders plugin.
func New() *MetricsToHeaders {
	return &MetricsToHeaders{
		typedName: fwkplugin.TypedName{Type: MetricsToHeadersType},
	}
}

// ResponseHeader is the handler for the ResponseHeader extension point.
//
// Headers written here land in the same map the ext_proc layer drains when it
// builds the response-headers mutation, so they reach the client as-is. None of
// these names are system-owned, so none are filtered on the way out.
func (m *MetricsToHeaders) ResponseHeader(ctx context.Context, request *fwksched.InferenceRequest,
	response *requestcontrol.Response, _ *fwkdl.EndpointMetadata) {
	logger := log.FromContext(ctx).WithName(m.TypedName().String())

	metrics := servingEndpointMetrics(request)
	if metrics == nil {
		logger.V(logging.DEBUG).Info("No metrics for the serving endpoint, skipping headers")
		return
	}

	response.Headers[KVCacheUtilizationHeader] = strconv.FormatFloat(metrics.KVCacheUsagePercent, 'f', 4, 64)
	response.Headers[WaitingQueueHeader] = strconv.Itoa(metrics.WaitingQueueSize)
	response.Headers[RunningRequestsHeader] = strconv.Itoa(metrics.RunningRequestsSize)

	// A zero UpdateTime means the endpoint was never scraped. Reporting an age of
	// "now minus the epoch" would be worse than reporting nothing.
	if !metrics.UpdateTime.IsZero() {
		age := time.Since(metrics.UpdateTime).Milliseconds()
		response.Headers[MetricsAgeHeader] = strconv.FormatInt(age, 10)
	}

	logger.V(logging.TRACE).Info("Added endpoint metrics headers",
		"kvCacheUtilization", metrics.KVCacheUsagePercent,
		"waitingQueue", metrics.WaitingQueueSize,
		"runningRequests", metrics.RunningRequestsSize)
}

// servingEndpointMetrics returns the latest metrics snapshot of the endpoint the
// primary profile selected, or nil when the scheduling cycle left nothing to read.
func servingEndpointMetrics(request *fwksched.InferenceRequest) *fwkdl.Metrics {
	if request == nil || request.SchedulingResult == nil {
		return nil
	}
	profile := request.SchedulingResult.ProfileResults[request.SchedulingResult.PrimaryProfileName]
	if profile == nil || len(profile.TargetEndpoints) == 0 {
		return nil
	}
	return profile.TargetEndpoints[0].GetMetrics()
}
