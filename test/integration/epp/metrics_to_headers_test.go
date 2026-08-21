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

package epp

import (
	_ "embed"
	"strconv"
	"strings"
	"testing"
	"time"

	extProcPb "github.com/envoyproxy/go-control-plane/envoy/service/ext_proc/v3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/llm-d/llm-d-router/pkg/epp/framework/plugins/requestcontrol/metricstoheaders"
	integration "github.com/llm-d/llm-d-router/test/integration"
)

//go:embed testdata/metrics-to-headers-config.yaml
var metricsToHeadersTestConfig string

// Metrics of the single pod the scheduler has to choose. They are echoed back
// verbatim, so they double as the expected header values.
const (
	servedQueueSize    = 5
	servedKVCacheUsage = 0.25
)

// TestMetricsToHeaders drives a full ext_proc exchange and asserts the serving
// endpoint's metrics come back in the response-headers mutation. The unit tests
// cover the plugin in isolation; this covers the seam the plugin depends on --
// that headers written into the request context are drained into the mutation
// Envoy actually receives.
func TestMetricsToHeaders(t *testing.T) {
	ctx := t.Context()

	h := NewTestHarness(ctx, t, WithStandardMode(), WithConfigText(metricsToHeadersTestConfig)).WithBaseResources()

	pods := []PodState{P(0, servedQueueSize, servedKVCacheUsage, modelMyModelTarget)}
	h.WithPods(pods).WaitForSync(len(pods), modelMyModel)
	h.WaitForReadyPodsMetric(len(pods))

	requests := integration.ReqLLM(reqLogger, "hello", "modelName", "modelName")
	requests = append(requests, ReqResponseOnly(
		map[string]string{"content-type": "application/json", "status": "200"},
		`{"choices":[{"finish_reason":"stop","index":0,"message":{"content":"Hi.","role":"assistant"}}],`+
			`"model":"modelName","object":"chat.completion","usage":{"completion_tokens":2,"prompt_tokens":3,"total_tokens":5}}`,
	)...)

	// Request headers, request body, response headers, response body.
	responses, err := integration.StreamedRequest(t, h.Client, requests, 4)
	require.NoError(t, err)
	require.Len(t, responses, 4)

	headers := setHeaders(t, responses[2])

	assert.Equal(t, strconv.FormatFloat(servedKVCacheUsage, 'f', 4, 64), headers[metricstoheaders.KVCacheUtilizationHeader])
	assert.Equal(t, strconv.Itoa(servedQueueSize), headers[metricstoheaders.WaitingQueueHeader])
	assert.Equal(t, "0", headers[metricstoheaders.RunningRequestsHeader], "the mock backend reports no running requests")

	// The mock data source stamps UpdateTime on every refresh, so the snapshot
	// must read as fresh rather than as missing or epoch-aged.
	require.Contains(t, headers, metricstoheaders.MetricsAgeHeader)
	age, err := strconv.ParseInt(headers[metricstoheaders.MetricsAgeHeader], 10, 64)
	require.NoError(t, err, "age header should be an integer")
	assert.GreaterOrEqual(t, age, int64(0))
	assert.Less(t, age, int64(5*time.Minute/time.Millisecond), "metrics should be freshly scraped")
}

// setHeaders extracts the response-headers mutation as a lowercase-keyed map.
func setHeaders(t *testing.T, response *extProcPb.ProcessingResponse) map[string]string {
	t.Helper()

	mutation := response.GetResponseHeaders().GetResponse().GetHeaderMutation()
	require.NotNil(t, mutation, "expected a header mutation on the response-headers response")

	headers := make(map[string]string, len(mutation.GetSetHeaders()))
	for _, option := range mutation.GetSetHeaders() {
		header := option.GetHeader()
		value := string(header.GetRawValue())
		if value == "" {
			value = header.GetValue()
		}
		headers[strings.ToLower(header.GetKey())] = value
	}
	return headers
}
