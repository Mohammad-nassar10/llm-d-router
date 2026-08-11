# Latency Observer Producer (`latency-observer-producer`)


Observes each endpoint's actual TTFTs and publishes a small `TTFTPercentiles` snapshot to the
endpoint's attribute store every `intervalDuration`. The
[latency-observation scorer](../../../scheduling/scorer/latencyobservation/README.md) consumes that snapshot to predict
TTFT under load; this package only measures and summarises — it carries no prediction formula of its
own.

## Motivation

The [latency-observation scorer](../../../scheduling/scorer/latencyobservation/README.md) routes each request to
whichever endpoint is predicted to give the best TTFT under its *current* load, e.g. offloading from
a saturated deployment to an idle one. To predict TTFT at an arbitrary load it interpolates between
a handful of anchor points: an estimated load-invariant floor, plus TTFT/in-flight pairs at two
operating percentiles. This producer exists to measure and publish exactly those anchor points, so
measurement and prediction stay in separate packages.

Unlike every other load-aware signal in EPP, nothing here is scraped from the endpoint — the only
input is the endpoint's own response. That is what lets it rank endpoints that expose no metrics at
all.

## How it works

EPP has no data source that watches traffic, so measurement happens in the request-control hooks
while the result is published as a datalayer attribute:

| hook | what it does |
|---|---|
| `Produce` | records each candidate's live in-flight count, before this request is added to it |
| `PreRequest` | the winner is known — pins its count and starts the clock |
| `ResponseBody`, first chunk | TTFT observed; appended to that endpoint's window |
| background ticker | every `intervalDuration`, recomputes the percentiles and publishes |

In-flight is read in `Produce` rather than `PreRequest` because `InFlightLoad` is a live view of
`inflight-load-producer`'s counters, and that producer increments them in its *own* `PreRequest`.
Hook order between plugins is undefined, so reading there would include or exclude this very request
depending on registration order. `Produce` is DAG-ordered, so the captured value is well defined.

The snapshot is exposed through a `DynamicAttribute` attached once per endpoint, so each flush swaps
a pointer rather than writing the attribute map.

> **Note:** intended for EPP hub deployments, which route across peer clusters rather than selecting
> model-serving endpoints.

## Streaming responses only

A time-to-first-token exists only when the response arrives in chunks. If the whole body comes back
at once its latency is end-to-end — prefill plus every decode step — which sits far above the real
TTFT, so it is discarded rather than recorded.

A deployment whose responses are not streamed therefore produces no observations, every endpoint
stays cold, and the scorer scores them all equally. That is correct, but it means the plugin
contributes nothing there.

## Published fields

| Field | Meaning |
|---|---|
| `P10LowTTFT` | load-invariant service floor (see below) |
| `P10TTFT` | 10th percentile TTFT over the short window (floor fallback) |
| `LowTTFT`, `HighTTFT` | TTFT at the low / high operating percentiles (default P25 / P50) — the scorer's two anchors |
| `InflightAtLow` | average in-flight-at-dispatch in the band around `lowPercentile` |
| `InflightAtHigh` | average in-flight-at-dispatch in the band around `highPercentile` |
| `RecentN` | observation count in the capped short window |
| `Observations` | cumulative observations that have fed the floor (gates `Floor()`) |
| `MinRequests` | trust threshold, copied from config so the scorer needs no separate param |

Two internal structures produce these:

**Short window** (`maxObservationAge` / `maxRequests`, kept responsive) — a value-sorted, capped,
age-bounded snapshot of recent observations. It yields `P25`, `P50`, `P10`, and the two banded
in-flight averages. Averaging in-flight over a percentile band rather than a single observation
stabilises the operating-point estimate.

**Bucket history** (long-term) — produces the floor.

## The floor (`P10Low`)

Every TTFT decomposes as `TTFT = prefill_time + queue_wait`. Prefill time does not change with queue
depth, so a low percentile of TTFT recovers the hardware-bound prefill floor. Computing it robustly:

- once per `bucketDuration`, record the **P10 TTFT of that bucket** into a bounded history;
- `P10Low` is the **P10 of that history**;
- when the history is full (`bucketHistorySize` entries) the **largest** entry is evicted — not the
  oldest — so a single anomalously slow bucket never sticks.

Because the history spans both idle and busy buckets, taking a low percentile of it locks onto the
idle buckets (the true floor) instead of drifting up with recent load — it is load-invariant. Only
the slowest entry is evicted: the floor is already a low percentile of this history, so dropping
the fastest as well would discard the samples it reads and let it drift upward through a long
high-load period.

### Sample guard

`Floor()` returns **0 until at least `MinRequests` observations have fed it**, and only then the real
`P10Low` (or `P10` before the history fills). An endpoint with a handful of observations publishes a
floor that is percentile-noise from cold-start requests — a genuine idle TTFT of ~0.037 s can read as
0.56 s from two or three samples. Returning that would make a barely-observed endpoint look
confidently slow. Returning 0 instead makes the scorer treat it as **cold**; the scorer's exploration
then sends it calibration probes until it crosses `MinRequests`.

## Parameters

| Parameter | Default | Description |
|---|---|---|
| `intervalDuration` | 1s | How often the snapshot is recomputed and published |
| `maxObservationAge` | 3m | Time bound for the short window (P25 / P50 / P10) |
| `maxRequests` | 100 | Cap the short window to the most recent N observations |
| `minRequests` | 10 | Minimum observations before the floor and operating point are trusted |
| `lowPercentile` | 25 | Low operating-anchor percentile (published as `LowTTFT` / `InflightAtLow`) |
| `highPercentile` | 50 | High operating-anchor percentile (`HighTTFT` / `InflightAtHigh`); must satisfy `0 < low < high < 100` |
| `windowSize` | 5000 | Ring buffer capacity per endpoint, allocated up front (~200 KB) |
| `bucketDuration` | 1m | Window for each floor-history entry's P10; keep `<= maxObservationAge` |
| `bucketHistorySize` | 1000 | Per-bucket P10s kept for the floor; the slowest is evicted when full. Must be >= 2 |
| `inFlightLoadProducerName` | "" | Which `inflight-load-producer` instance to read. Empty uses the default producer |

## Configuration

Usually nothing: the scorer declares this producer's data key as `Required`, so it is auto-created
with defaults. Configure it explicitly only to tune the windows:

```yaml
plugins:
  - type: latency-observer-producer
    name: ttft-observer
    parameters:
      intervalDuration: 1s
      minRequests: 10
      bucketDuration: 1m
      bucketHistorySize: 1000
  - type: latency-observation-scorer
    name: ttft
    parameters:
      ttftPercentilesProducerName: ttft-observer
```

See the [scorer README](../../../scheduling/scorer/latencyobservation/README.md) for the prediction model and
an end-to-end pipeline.
