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

package kv

import (
	"context"

	"github.com/llm-d/llm-d-router/pkg/coordinator/pipeline"
)

// noneKV disables KV-transfer coordination for aggregated pipelines, where one
// pod both prefills and decodes. Both methods return nil so the steps omit
// kv_transfer_params: announcing do_remote_prefill with no peer sends the pod
// looking for blocks nobody produced, which kills vLLM's engine on
// `assert num_external_tokens == 0`.
type noneKV struct{}

func (noneKV) Name() string { return None }

func (noneKV) PreparePrefillKVParams(_ context.Context, _ *pipeline.RequestContext) map[string]any {
	return nil
}

func (noneKV) PrepareDecodeKVParams(_ context.Context, _ *pipeline.RequestContext) map[string]any {
	return nil
}
