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
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"time"

	"github.com/go-logr/logr"
	"sigs.k8s.io/controller-runtime/pkg/log"

	logutil "github.com/llm-d/llm-d-router/pkg/common/observability/logging"
	reqcommon "github.com/llm-d/llm-d-router/pkg/common/request"

	"github.com/llm-d/llm-d-router/pkg/coordinator/connectors/kv"
	"github.com/llm-d/llm-d-router/pkg/coordinator/gateway"
	"github.com/llm-d/llm-d-router/pkg/coordinator/pipeline"
)

const DecodeStepName = "decode"

func init() {
	pipeline.Register(DecodeStepName, NewDecodeStep)
}

// persistResponseLimitBytes caps how much of a decode response the persist
// hook buffers before handing it to the persistence service.
const persistResponseLimitBytes = 32 << 20 // 32 MB

// defaultMaxToolRounds is a backstop, not the real budget: the state service
// ends its own loop first, stopping at ten rounds and marking the turn
// incomplete. This only bounds a service that never reports done.
const defaultMaxToolRounds = 12

// toolRoundDone is the status the state service reports when the response it
// was just given is the final one.
const toolRoundDone = "done"

// toolRoundResponse is one round of the tool loop: either the next request to
// send the model, or the signal that this turn is final. The context grows each
// round, so the last one is what persist must receive.
type toolRoundResponse struct {
	Status  string          `json:"status"`
	Request json.RawMessage `json:"request"`
	Context json.RawMessage `json:"context"`
}

type DecodeStep struct {
	useOpenAIFormat bool
	gwClient        *gateway.Client
	kv              kv.Connector
	// prefillPresent is false for aggregated pipelines; the decode request then
	// omits kv_transfer_params, since no prefill leg produced any blocks.
	prefillPresent bool
	// persistAddress, when set, enables the /v1/responses persist hook: the
	// decode response is stored by the persistence service and replaced with
	// the envelope it returns.
	persistAddress string
	persistClient  *http.Client
	// toolsAddress, when set, enables the gateway tool loop: between inference
	// and persist, the state service executes the tools it owns and returns the
	// next request to send. Only requests it flagged at hydration use it.
	toolsAddress  string
	toolsClient   *http.Client
	maxToolRounds int
	// toolRoundSteps are replayed for every round after the first: the steps
	// between hydration and serving. Without them a tool round would go straight
	// to the gateway, so a guard or compaction step would apply to round 1 only.
	// Decode is deliberately not in this list, which is what makes a replayed
	// round unable to start a loop of its own.
	toolRoundSteps []pipeline.Step
}

// SetToolRoundSteps wires the replay list. The builder calls it once the whole
// pipeline is constructed, since a step cannot see its siblings at build time.
func (s *DecodeStep) SetToolRoundSteps(steps []pipeline.Step) {
	s.toolRoundSteps = steps
}

func NewDecodeStep(gwClient *gateway.Client, params map[string]any) (pipeline.Step, error) {
	if gwClient == nil {
		return nil, errors.New("decode: gateway client is required")
	}
	useOpenAI, err := parseUseOpenAIFormat(params)
	if err != nil {
		return nil, fmt.Errorf("decode: %w", err)
	}
	kvName, err := paramString(params, ParamKVConnector)
	if err != nil {
		return nil, fmt.Errorf("decode: %w", err)
	}
	kvConn, err := kv.Build(kvName)
	if err != nil {
		return nil, fmt.Errorf("decode: %w", err)
	}
	persistAddress, err := paramString(params, "persist_address")
	if err != nil {
		return nil, fmt.Errorf("decode: %w", err)
	}
	persistTimeout := 30 * time.Second
	if v, ok, err := paramDuration(params, "persist_timeout"); err != nil {
		return nil, fmt.Errorf("decode: %w", err)
	} else if ok {
		persistTimeout = v
	}
	prefillPresent := true
	if v, ok, err := paramBool(params, ParamPrefillPresent); err != nil {
		return nil, fmt.Errorf("decode: %w", err)
	} else if ok {
		prefillPresent = v
	}
	toolsAddress, err := paramString(params, "tools_address")
	if err != nil {
		return nil, fmt.Errorf("decode: %w", err)
	}
	// A tool round is bounded by the tools it runs, not by an inference call.
	toolsTimeout := 120 * time.Second
	if v, ok, err := paramDuration(params, "tools_timeout"); err != nil {
		return nil, fmt.Errorf("decode: %w", err)
	} else if ok {
		toolsTimeout = v
	}
	maxToolRounds := defaultMaxToolRounds
	if v, ok, err := paramInt(params, "max_tool_rounds"); err != nil {
		return nil, fmt.Errorf("decode: %w", err)
	} else if ok {
		if v < 1 {
			return nil, fmt.Errorf("decode: max_tool_rounds must be at least 1, got %d", v)
		}
		maxToolRounds = v
	}
	return &DecodeStep{
		useOpenAIFormat: useOpenAI,
		gwClient:        gwClient,
		kv:              kvConn,
		prefillPresent:  prefillPresent,
		persistAddress:  persistAddress,
		persistClient:   &http.Client{Timeout: persistTimeout},
		toolsAddress:    toolsAddress,
		toolsClient:     &http.Client{Timeout: toolsTimeout},
		maxToolRounds:   maxToolRounds,
	}, nil
}

func (s *DecodeStep) Name() string { return DecodeStepName }

func (s *DecodeStep) Execute(ctx context.Context, reqCtx *pipeline.RequestContext) error {
	logger := log.FromContext(ctx).WithName(DecodeStepName)

	s.prepareDecodeBody(ctx, reqCtx)

	logger.V(logutil.DEFAULT).Info("sending request", "path", reqCtx.OriginalPath, "stream", reqCtx.Stream)

	proxyReq, err := newDecodeProxyRequest(ctx, logger, DecodeStepName, reqCtx, s.gwClient, reqCtx.Body, nil)
	if err != nil {
		return err
	}

	var modifyResponse func(*http.Response) error
	if s.persistAddress != "" && len(reqCtx.ResponsesHydration) > 0 {
		// The hook rewrites the body, which a content-encoded one would defeat.
		proxyReq.Header.Del("Accept-Encoding")
		modifyResponse = s.persistHydratedResponse(ctx, logger, reqCtx)
	}

	proxy := newDecodeProxy(logger, s.gwClient.Transport(), modifyResponse)
	proxy.ServeHTTP(reqCtx.ResponseWriter, proxyReq)
	return nil
}

// persistHydratedResponse returns the ModifyResponse hook for a hydrated
// /v1/responses request: it sends the decode response and the hydration context
// to the persistence service, and replaces the body with the envelope it
// returns (which carries the stored response id). Failures become 502s — a
// response whose turn was not persisted carries an id that can never be
// continued, so it must not look successful.
func (s *DecodeStep) persistHydratedResponse(ctx context.Context, logger logr.Logger, reqCtx *pipeline.RequestContext) func(*http.Response) error {
	return func(resp *http.Response) error {
		if resp.StatusCode != http.StatusOK {
			// Upstream errors pass through untouched; there is nothing to persist.
			return nil
		}
		upstream, err := readLimitedBody(resp.Body, "decode response")
		if closeErr := resp.Body.Close(); closeErr != nil {
			logger.V(logutil.DEFAULT).Info("closing decode response body", "err", closeErr)
		}
		if err != nil {
			return err
		}

		// Tools the state service owns run before the turn is stored: the answer
		// being persisted has to be the one that used their results.
		if s.toolsAddress != "" && reqCtx.ResponsesToolLoop {
			if upstream, err = s.runToolRounds(ctx, logger, reqCtx, upstream); err != nil {
				return err
			}
		}

		payload, err := json.Marshal(map[string]json.RawMessage{
			"context":  reqCtx.ResponsesHydration,
			"response": upstream,
		})
		if err != nil {
			return fmt.Errorf("%s: marshal persist request: %w", DecodeStepName, err)
		}
		envelope, err := s.postInternal(ctx, s.persistClient, s.persistAddress+persistPath, payload, "persist")
		if err != nil {
			return err
		}

		resp.Body = io.NopCloser(bytes.NewReader(envelope))
		resp.ContentLength = int64(len(envelope))
		resp.Header.Set("Content-Length", strconv.Itoa(len(envelope)))
		logger.V(logutil.DEFAULT).Info("complete: turn persisted, envelope forwarded", "envelope_bytes", len(envelope))
		return nil
	}
}

// runToolRounds drives the gateway tool loop and returns the final upstream
// response. Each round hands the state service the request that was sent and the
// response that came back; it runs the tools it owns, appends their outputs and
// returns the next request, so every model call still goes through the gateway
// and its endpoint picker. The coordinator inspects none of it: the request and
// the context are opaque JSON it only moves.
func (s *DecodeStep) runToolRounds(ctx context.Context, logger logr.Logger, reqCtx *pipeline.RequestContext, upstream []byte) ([]byte, error) {
	request, err := json.Marshal(reqCtx.Body)
	if err != nil {
		return nil, fmt.Errorf("%s: marshal decode body for the tool loop: %w", DecodeStepName, err)
	}

	for round := 0; round < s.maxToolRounds; round++ {
		payload, err := json.Marshal(map[string]json.RawMessage{
			"context":  reqCtx.ResponsesHydration,
			"request":  request,
			"response": upstream,
		})
		if err != nil {
			return nil, fmt.Errorf("%s: marshal tool round request: %w", DecodeStepName, err)
		}
		body, err := s.postInternal(ctx, s.toolsClient, s.toolsAddress+toolsPath, payload, "tool round")
		if err != nil {
			return nil, err
		}

		var next toolRoundResponse
		if err := json.Unmarshal(body, &next); err != nil {
			return nil, fmt.Errorf("%s: decode tool round response: %w", DecodeStepName, err)
		}
		if len(next.Context) == 0 {
			return nil, fmt.Errorf("%s: tool round returned no context", DecodeStepName)
		}
		// The context accumulates this turn's calls and outputs, so persist has
		// to receive the one from the last round.
		reqCtx.ResponsesHydration = next.Context
		if next.Status == toolRoundDone {
			logger.V(logutil.DEFAULT).Info("tool loop complete", "rounds", round)
			return upstream, nil
		}
		if len(next.Request) == 0 {
			return nil, fmt.Errorf("%s: tool round status %q carried no request", DecodeStepName, next.Status)
		}

		request = next.Request
		logger.V(logutil.DEFAULT).Info("tool round: tools ran, calling the model again",
			"round", round+1, "replayed_steps", len(s.toolRoundSteps))
		if upstream, err = s.serveOnce(ctx, reqCtx, request); err != nil {
			return nil, err
		}
	}
	return nil, fmt.Errorf("%s: tool loop exceeded %d rounds", DecodeStepName, s.maxToolRounds)
}

// serveOnce runs one extra round: it replays the pipeline steps between
// hydration and serving against the new request, then makes the inference call.
// It runs exactly once and contains no loop, so re-entry cannot nest.
func (s *DecodeStep) serveOnce(ctx context.Context, reqCtx *pipeline.RequestContext, request []byte) ([]byte, error) {
	inner, err := reqCtx.ForToolRound(request)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", DecodeStepName, err)
	}
	if inner.ToolRoundDepth > 1 {
		return nil, fmt.Errorf("%s: tool round re-entered at depth %d", DecodeStepName, inner.ToolRoundDepth)
	}

	for _, step := range s.toolRoundSteps {
		if err := step.Execute(ctx, inner); err != nil {
			return nil, fmt.Errorf("%s: tool round step %q: %w", DecodeStepName, step.Name(), err)
		}
	}

	body, err := json.Marshal(inner.Body)
	if err != nil {
		return nil, fmt.Errorf("%s: marshal tool round request: %w", DecodeStepName, err)
	}
	return s.serveToolRound(ctx, inner, body)
}

// serveToolRound makes one extra inference call for the tool loop. It cannot be
// proxied like the first: its response is consumed here rather than streamed, so
// it goes through the gateway client the way the prefill leg does.
func (s *DecodeStep) serveToolRound(ctx context.Context, reqCtx *pipeline.RequestContext, body []byte) ([]byte, error) {
	headers := reqCtx.ForwardedHeaders()
	headers[reqcommon.RequestIDHeaderKey] = reqCtx.RequestID
	headers[gateway.EPPProfileHeader] = gateway.PhaseDecode

	resp, err := s.gwClient.Post(ctx, reqCtx.OriginalPath, body, headers)
	if err != nil {
		return nil, fmt.Errorf("%s: tool round inference request: %w", DecodeStepName, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, upstreamError(DecodeStepName, resp.StatusCode, readErrorBody(resp.Body))
	}
	return readLimitedBody(resp.Body, "tool round response")
}

// postInternal calls one of the state service's cluster-internal endpoints and
// returns its body. Every failure is an error, never a fake success: a turn that
// was not stored carries an id the client can never continue from.
func (s *DecodeStep) postInternal(ctx context.Context, client *http.Client, url string, payload []byte, what string) ([]byte, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(payload))
	if err != nil {
		return nil, fmt.Errorf("%s: build %s request: %w", DecodeStepName, what, err)
	}
	req.ContentLength = int64(len(payload))
	req.Header.Set(gateway.ContentTypeHeader, gateway.ContentTypeJSON)

	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("%s: %s request failed: %w", DecodeStepName, what, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("%s: %s service returned HTTP %d: %s",
			DecodeStepName, what, resp.StatusCode, readErrorBody(resp.Body))
	}
	return readLimitedBody(resp.Body, what+" response")
}

// readLimitedBody buffers a body, refusing one past persistResponseLimitBytes.
// Buffering is an exposure the streaming path never had, so it stays bounded.
func readLimitedBody(body io.Reader, what string) ([]byte, error) {
	data, err := io.ReadAll(io.LimitReader(body, persistResponseLimitBytes+1))
	if err != nil {
		return nil, fmt.Errorf("%s: reading %s: %w", DecodeStepName, what, err)
	}
	if len(data) > persistResponseLimitBytes {
		return nil, fmt.Errorf("%s: %s exceeds the limit of %d bytes", DecodeStepName, what, persistResponseLimitBytes)
	}
	return data, nil
}

// prepareDecodeBody mutates reqCtx.Body in place rather than on a clone (unlike
// prefill and conditional-decode). decode is the terminal pipeline step: its body
// is streamed straight to the client and no later step reads reqCtx.Body. A clone
// would also be insufficient, since injectUUIDs mutates nested values that a shallow
// maps.Clone would still share. This is sound only while the pipeline runs steps
// sequentially; if it ever goes concurrent, decode must copy like the others.
func (s *DecodeStep) prepareDecodeBody(ctx context.Context, reqCtx *pipeline.RequestContext) {
	kvParams := s.kv.PrepareDecodeKVParams(ctx, reqCtx)
	s.injectUUIDs(reqCtx)

	format := resolveFormat(s.useOpenAIFormat, reqCtx.OriginalPath)
	switch format {
	case gateway.FormatChatCompletions:
		if s.prefillPresent {
			reqCtx.Body[reqcommon.FieldKVTransferParams] = kvParams
		}
		s.injectTokensField(reqCtx)
	case gateway.FormatCompletions:
		if s.prefillPresent {
			reqCtx.Body[reqcommon.FieldKVTransferParams] = kvParams
		}
		if len(reqCtx.TokenIDs) > 0 {
			reqCtx.Body["prompt"] = reqCtx.TokenIDs
		}
	case gateway.FormatGenerate:
		// The /inference/v1/generate engine reads transfer params only from
		// sampling_params.extra_args; a top-level kv_transfer_params is ignored,
		// so the decode worker never pulls the prefill KV over NIXL. Merge into
		// the client's sampling_params to preserve max_tokens and other fields.
		if s.prefillPresent {
			sampling, ok := reqCtx.Body[reqcommon.FieldSamplingParams].(map[string]any)
			if !ok {
				sampling = map[string]any{}
				reqCtx.Body[reqcommon.FieldSamplingParams] = sampling
			}
			setGenerateTransferParams(sampling, kvParams, nil)
		}
	}
}

func (s *DecodeStep) injectTokensField(reqCtx *pipeline.RequestContext) {
	tokens := map[string]any{
		"token_ids": reqCtx.TokenIDs,
	}
	if features := buildMMFeatures(reqCtx.MultimodalEntries, false); features != nil {
		tokens["features"] = features
	}
	reqCtx.Body["tokens"] = tokens
}

func (s *DecodeStep) injectUUIDs(reqCtx *pipeline.RequestContext) {
	messages, ok := reqCtx.Body["messages"].([]any)
	if !ok {
		return
	}

	hashIdx := 0
	for _, msg := range messages {
		msgMap, ok := msg.(map[string]any)
		if !ok {
			continue
		}
		content, ok := msgMap["content"].([]any)
		if !ok {
			continue
		}
		for _, part := range content {
			partMap, ok := part.(map[string]any)
			if !ok {
				continue
			}
			if partMap["type"] != "image_url" {
				continue
			}
			if hashIdx < len(reqCtx.MultimodalEntries) {
				partMap["uuid"] = reqCtx.MultimodalEntries[hashIdx].Hash
				hashIdx++
			}
		}
	}
}
