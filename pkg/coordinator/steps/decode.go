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

type DecodeStep struct {
	useOpenAIFormat bool
	gwClient        *gateway.Client
	kv              kv.Connector
	// persistAddress, when set, enables the /v1/responses persist hook: hydrated
	// requests have their decode response stored by the persistence service and
	// replaced with the returned response envelope.
	persistAddress string
	persistClient  *http.Client
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
	return &DecodeStep{
		useOpenAIFormat: useOpenAI,
		gwClient:        gwClient,
		kv:              kvConn,
		persistAddress:  persistAddress,
		persistClient:   &http.Client{Timeout: persistTimeout},
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
		upstream, err := io.ReadAll(io.LimitReader(resp.Body, persistResponseLimitBytes+1))
		if closeErr := resp.Body.Close(); closeErr != nil {
			logger.V(logutil.DEFAULT).Info("closing decode response body", "err", closeErr)
		}
		if err != nil {
			return fmt.Errorf("%s: reading decode response for persist: %w", DecodeStepName, err)
		}
		if len(upstream) > persistResponseLimitBytes {
			return fmt.Errorf("%s: decode response exceeds persist limit of %d bytes", DecodeStepName, persistResponseLimitBytes)
		}

		payload, err := json.Marshal(map[string]json.RawMessage{
			"context":  reqCtx.ResponsesHydration,
			"response": upstream,
		})
		if err != nil {
			return fmt.Errorf("%s: marshal persist request: %w", DecodeStepName, err)
		}

		persistReq, err := http.NewRequestWithContext(ctx, http.MethodPost, s.persistAddress+persistPath, bytes.NewReader(payload))
		if err != nil {
			return fmt.Errorf("%s: build persist request: %w", DecodeStepName, err)
		}
		persistReq.ContentLength = int64(len(payload))
		persistReq.Header.Set(gateway.ContentTypeHeader, gateway.ContentTypeJSON)

		persistResp, err := s.persistClient.Do(persistReq)
		if err != nil {
			return fmt.Errorf("%s: persist request failed: %w", DecodeStepName, err)
		}
		defer persistResp.Body.Close()

		if persistResp.StatusCode != http.StatusOK {
			return fmt.Errorf("%s: persist service returned HTTP %d: %s",
				DecodeStepName, persistResp.StatusCode, readErrorBody(persistResp.Body))
		}
		envelope, err := io.ReadAll(io.LimitReader(persistResp.Body, persistResponseLimitBytes))
		if err != nil {
			return fmt.Errorf("%s: reading persist response: %w", DecodeStepName, err)
		}

		resp.Body = io.NopCloser(bytes.NewReader(envelope))
		resp.ContentLength = int64(len(envelope))
		resp.Header.Set("Content-Length", strconv.Itoa(len(envelope)))
		logger.V(logutil.DEFAULT).Info("complete: turn persisted, envelope forwarded", "envelope_bytes", len(envelope))
		return nil
	}
}

// prepareDecodeBody mutates reqCtx.Body in place rather than on a clone (unlike
// prefill and conditional-decode). decode is the terminal pipeline step: its body
// is streamed straight to the client and no later step reads reqCtx.Body. A clone
// would also be insufficient, since injectUUIDs mutates nested values that a shallow
// maps.Clone would still share. This is sound only while the pipeline runs steps
// sequentially; if it ever goes concurrent, decode must copy like the others.
func (s *DecodeStep) prepareDecodeBody(ctx context.Context, reqCtx *pipeline.RequestContext) {
	// No params (kv-none) means there is no transfer to announce; sending the
	// field anyway asks the pod to fetch KV blocks no prefill leg produced.
	kvParams := s.kv.PrepareDecodeKVParams(ctx, reqCtx)
	hasKVTransfer := len(kvParams) > 0
	s.injectUUIDs(reqCtx)

	format := resolveFormat(s.useOpenAIFormat, reqCtx.OriginalPath)
	switch format {
	case gateway.FormatChatCompletions:
		if hasKVTransfer {
			reqCtx.Body[reqcommon.FieldKVTransferParams] = kvParams
		}
		s.injectTokensField(reqCtx)
	case gateway.FormatCompletions:
		if hasKVTransfer {
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
		if hasKVTransfer {
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
