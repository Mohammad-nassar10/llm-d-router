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

package steps

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"strings"
	"time"

	"sigs.k8s.io/controller-runtime/pkg/log"

	logutil "github.com/llm-d/llm-d-router/pkg/common/observability/logging"

	"github.com/llm-d/llm-d-router/pkg/coordinator/gateway"
	"github.com/llm-d/llm-d-router/pkg/coordinator/pipeline"
)

const ResponsesHydrateStepName = "responses-hydrate"

// Paths on the hydration service (agentic-api's cluster-internal API).
const (
	hydratePath = "/internal/hydrate"
	persistPath = "/internal/persist"
)

func init() {
	pipeline.Register(ResponsesHydrateStepName, NewResponsesHydrateStep)
}

// ResponsesHydrateStep turns a stateful /v1/responses request into a stateless
// one by calling the hydration service, which resolves previous_response_id
// into the full conversation. It also returns an opaque context that the decode
// step's persist hook echoes back after inference.
//
// Not fail-open: answering without the history would silently reply to a
// truncated conversation, so any failure fails the request.
type ResponsesHydrateStep struct {
	serviceAddress string
	client         *http.Client
}

func NewResponsesHydrateStep(_ *gateway.Client, params map[string]any) (pipeline.Step, error) {
	timeout := 30 * time.Second
	if v, ok, err := paramDuration(params, "timeout"); err != nil {
		return nil, err
	} else if ok {
		timeout = v
	}

	address, err := paramString(params, "address")
	if err != nil {
		return nil, err
	}
	if address == "" {
		return nil, fmt.Errorf("%s: address is required", ResponsesHydrateStepName)
	}

	transport := &http.Transport{
		DialContext: (&net.Dialer{
			Timeout:   30 * time.Second,
			KeepAlive: 30 * time.Second,
		}).DialContext,
		MaxIdleConnsPerHost: 100,
		IdleConnTimeout:     90 * time.Second,
		ForceAttemptHTTP2:   true,
	}

	return &ResponsesHydrateStep{
		serviceAddress: address,
		client:         &http.Client{Timeout: timeout, Transport: transport},
	}, nil
}

// SetServiceAddress overrides the hydration service address. Used in tests.
func (s *ResponsesHydrateStep) SetServiceAddress(addr string) {
	s.serviceAddress = addr
}

func (s *ResponsesHydrateStep) Name() string { return ResponsesHydrateStepName }

func (s *ResponsesHydrateStep) Execute(ctx context.Context, reqCtx *pipeline.RequestContext) error {
	logger := log.FromContext(ctx).WithName(ResponsesHydrateStepName)

	if !strings.Contains(reqCtx.OriginalPath, gateway.PathResponses) {
		logger.V(logutil.DEFAULT).Info("skipping: path is not a responses endpoint", "path", reqCtx.OriginalPath)
		return nil
	}

	body, err := json.Marshal(reqCtx.Body)
	if err != nil {
		return fmt.Errorf("%s: marshal body: %w", ResponsesHydrateStepName, err)
	}

	url := s.serviceAddress + hydratePath
	logger.V(logutil.DEFAULT).Info("sending request", "url", url, "body_size", len(body))

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("%s: build request: %w", ResponsesHydrateStepName, err)
	}
	req.ContentLength = int64(len(body))
	req.Header.Set(gateway.ContentTypeHeader, gateway.ContentTypeJSON)
	for k, v := range reqCtx.ForwardedHeaders() {
		req.Header.Set(k, v)
	}

	resp, err := s.client.Do(req)
	if err != nil {
		return fmt.Errorf("%s: request failed: %w", ResponsesHydrateStepName, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return upstreamError(ResponsesHydrateStepName, resp.StatusCode, readErrorBody(resp.Body))
	}

	var hydration struct {
		Request map[string]any  `json:"request"`
		Context json.RawMessage `json:"context"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&hydration); err != nil {
		return fmt.Errorf("%s: decode response: %w", ResponsesHydrateStepName, err)
	}
	if len(hydration.Request) == 0 || len(hydration.Context) == 0 {
		return fmt.Errorf("%s: hydration service returned an empty request or context", ResponsesHydrateStepName)
	}

	reqCtx.Body = hydration.Request
	reqCtx.ResponsesHydration = hydration.Context
	logger.V(logutil.DEFAULT).Info("complete: request hydrated", "hydrated_body_fields", len(hydration.Request))
	return nil
}
