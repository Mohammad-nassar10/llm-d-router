# Draft issue: latency-aware routing from observed TTFT

---

**What would you like to be added**:

Endpoint scoring by observed time-to-first-token, using statistics the EPP
measures itself rather than metrics scraped from the endpoint.

- `latency-observer-producer` — measures each request's actual
  time-to-first-token, correlates it with the endpoint's in-flight count at
  dispatch, and publishes a per-endpoint summary.
- `ttft-aware-scorer` — reads that summary plus the live in-flight count,
  predicts TTFT under the endpoint's current load, and ranks candidates.

```yaml
plugins:
  - type: ttft-aware-scorer
    name: ttft
```

The observer is auto-created from the scorer's required data key, which in turn
auto-creates `inflight-load-producer`.

Both plugins key on endpoint identity alone, so they work unchanged whether an
endpoint is a pod or a peer cluster gateway. **No multi-cluster variant is
needed** — unlike the metric scorers, which need one because the per-pod metric
fields are empty on a cluster endpoint.

**Why is this needed**:

Every existing load-aware scorer reads something the endpoint reports about
itself: queue depth, KV-cache utilisation, running requests. That works when the
endpoint is a pod the EPP scrapes directly.

It stops working at a trust boundary. A multi-cluster hub reaches each peer over
its gateway, and today's only cross-cluster signals —
`llm-d.ai/multicluster-kv-cache-utilization` and
`llm-d.ai/multicluster-queue-size` — are scraped from that peer, so the peer must
be a cooperating llm-d EPP exposing pool metrics across the boundary. That rules
out any cluster the operator does not run, cannot instrument, or can only reach
through a gateway exposing nothing but the inference API.

Observed latency needs nothing from the endpoint but a response. The EPP already
sees every request it dispatched and every response that came back, and
currently discards it.

### Why an observer plugin

EPP has no data source that watches traffic: polling, k8s notification and
endpoint lifecycle all describe state that exists independently of any request.
Latency does not — it exists *because* a request happened.

So measurement comes from the request-control hooks (`Produce` captures the
in-flight count, `PreRequest` pins it to the winner, `ResponseBody` times the
first chunk), while the result reaches the scorer as an endpoint attribute,
which is a datalayer concept. The plugin spans both layers, publishing through a
`DynamicAttribute` attached once per endpoint — the shape
`inflight-load-producer` already uses.

The scorer reads the snapshot for the endpoint's latency curve and the in-flight
count live, so the prediction reflects the load right now.
