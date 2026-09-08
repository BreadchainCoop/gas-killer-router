# Executor Performance Baseline

Snapshot p50/p95 values here after each significant testnet deploy. Use the PromQL
queries below against the in-cluster Prometheus (or Grafana Explore) over a window
that includes at least ~50 completed rounds.

## How to capture a baseline

```promql
# Replace 5m with whatever window gives you a stable rate (10m+ preferred)
histogram_quantile(0.50, rate(gas_killer_executor_hash_preflight_seconds_bucket[5m]))
histogram_quantile(0.95, rate(gas_killer_executor_hash_preflight_seconds_bucket[5m]))

histogram_quantile(0.50, rate(gas_killer_executor_supports_interface_seconds_bucket[5m]))
histogram_quantile(0.95, rate(gas_killer_executor_supports_interface_seconds_bucket[5m]))

histogram_quantile(0.50, rate(gas_killer_executor_tx_send_seconds_bucket[5m]))
histogram_quantile(0.95, rate(gas_killer_executor_tx_send_seconds_bucket[5m]))

histogram_quantile(0.50, rate(gas_killer_executor_receipt_confirmation_seconds_bucket[5m]))
histogram_quantile(0.95, rate(gas_killer_executor_receipt_confirmation_seconds_bucket[5m]))

histogram_quantile(0.50, rate(gas_killer_execution_duration_seconds_bucket[5m]))
histogram_quantile(0.95, rate(gas_killer_execution_duration_seconds_bucket[5m]))

histogram_quantile(0.50, rate(gas_killer_p2p_round_trip_seconds_bucket[5m]))
histogram_quantile(0.99, rate(gas_killer_p2p_round_trip_seconds_bucket[5m]))
```

## Snapshots

| Date | Commit | hash_preflight p50/p95 | supports_interface p50/p95 | tx_send p50/p95 | receipt_confirmation p50/p95 | execution_duration p50/p95 | p2p_round_trip p50/p99 | Notes |
|------|--------|------------------------|---------------------------|-----------------|------------------------------|---------------------------|------------------------|-------|
| 02/06/2026 | [d4ba093](https://github.com/gas-killer/service/commit/d4ba093511f6e49ae2c98efad247389beb960051) | 25/29.5ms | 15.0/19.5ms | 75.0/97.5ms | 8.0/14.7s | 8.0/15.6s | 1.50/1.99s | First deploy with per-phase metrics |

## Aggregation speed

End-to-end throughput in **rounds per minute** — each round is one successful, on-chain
`verifyAndUpdate`. Same window guidance as above (10m+ preferred for a stable rate).

```promql
# Successful aggregation rounds per minute
sum(rate(gas_killer_aggregation_rounds_completed_total[5m])) * 60

# Failed rounds per minute (context)
sum(rate(gas_killer_aggregation_rounds_failed_total[5m])) * 60
```

`W` is the number of aggregation heights the router drives concurrently. It is 1 today; the
column exists so later rows are comparable as the window opens.

| Date | Commit | W | aggregation_speed (rounds/min) | Notes |
|------|--------|---|--------------------------------|-------|
| 02/06/2026 | [d4ba093](https://github.com/gas-killer/service/commit/d4ba093511f6e49ae2c98efad247389beb960051) | 1 | 0.571 | - |

## Window and heights

The shape of the aggregation pipeline rather than the cost of one round. Capture these alongside
the aggregation speed for each `W`: a throughput gain that comes with a rising skip ratio or a
rising directive-drop rate is not a gain, it is a pipeline about to stall.

```promql
# Concurrent heights actually being driven — the value W buys
avg_over_time(gas_killer_in_flight_heights[10m])
max_over_time(gas_killer_in_flight_heights[10m])

# Oldest unresolved height, in seconds. Should stay within ROUND_TIMEOUT plus settlement.
max_over_time(gas_killer_height_age_seconds[10m])

# Height dispositions per minute. Anything but `executed` produced no on-chain effect.
sum by (outcome) (rate(gas_killer_height_outcomes_total[10m])) * 60

# Skip ratio — the ramp gate. Hold this under 0.01 before raising W further.
sum(rate(reporter_skipped_total[10m])) / sum(rate(reporter_certified_total[10m]))

# Directive delivery at the send site. Any sustained rate_limited is the split-digest hazard.
sum by (result) (rate(gas_killer_directive_sends_total[10m])) * 60

# What the receiving peers throttled, by channel (data_1 is the task directives).
sum by (message) (increase(network_spawner_messages_rate_limited_total[10m]))

# Must be exactly 1: more means the fleet disagrees on a consensus-critical setting.
count(count by (fingerprint) (gas_killer_config_fingerprint))
```

| Date | Commit | W | in-flight avg/max | height_age max | skip ratio | directive rate_limited/min | Notes |
|------|--------|---|-------------------|----------------|------------|----------------------------|-------|

## EVMSketch phases

Where the time inside one gas analysis goes. Capture these before sizing node CPU or a dedicated
simulation endpoint, because the two classes have different fixes and the total cannot tell them
apart.

```promql
# The headline question: which class of phase consumes more wall clock per second of runtime.
# Taken from the histogram sums, so the two are comparable magnitudes.
sum(rate(gas_killer_evmsketch_trace_fetch_seconds_sum[10m])) + sum(rate(gas_killer_evmsketch_state_prefetch_seconds_sum[10m]))
sum(rate(gas_killer_evmsketch_parse_seconds_sum[10m])) + sum(rate(gas_killer_evmsketch_revm_estimate_seconds_sum[10m]))

# Per-phase p95. trace_fetch and executor_build overlap inside the analyzer — never add them.
histogram_quantile(0.95, sum by (le) (rate(gas_killer_evmsketch_trace_fetch_seconds_bucket[10m])))
histogram_quantile(0.95, sum by (le) (rate(gas_killer_evmsketch_parse_seconds_bucket[10m])))
histogram_quantile(0.95, sum by (le) (rate(gas_killer_evmsketch_executor_build_seconds_bucket[10m])))
histogram_quantile(0.95, sum by (le) (rate(gas_killer_evmsketch_state_prefetch_seconds_bucket[10m])))
histogram_quantile(0.95, sum by (le) (rate(gas_killer_evmsketch_revm_estimate_seconds_bucket[10m])))

# prestate-net vs struct-log: mean seconds per run, by extractor. The net form has no parse
# series at all, which is the saving.
sum by (extraction) (rate(gas_killer_evmsketch_trace_fetch_seconds_sum[10m])) / sum by (extraction) (rate(gas_killer_evmsketch_trace_fetch_seconds_count[10m]))
sum by (extraction) (rate(gas_killer_evmsketch_parse_seconds_sum[10m])) / sum by (extraction) (rate(gas_killer_evmsketch_parse_seconds_count[10m]))

# How often each extractor ran. A high prestate_fallback share means the net form is being
# attempted and wasted.
sum by (extraction) (rate(gas_killer_evmsketch_trace_fetch_seconds_count[10m])) * 60

# Cache hit ratios; without these the histograms above describe an unknown number of runs.
sum(rate(gas_killer_evmsketch_executor_cache_total{result="hit"}[10m])) / sum(rate(gas_killer_evmsketch_executor_cache_total[10m]))
sum(rate(gas_killer_evmsketch_digest_cache_total{result="hit"}[10m])) / sum(rate(gas_killer_evmsketch_digest_cache_total[10m]))

# The same analysis on both sides: the router pays it serially on the assignment path.
histogram_quantile(0.95, sum by (le) (rate(gas_killer_node_evmsketch_duration_seconds_bucket{job=~".*router.*"}[10m])))
histogram_quantile(0.95, sum by (le) (rate(gas_killer_node_evmsketch_duration_seconds_bucket{job=~".*node.*"}[10m])))
```

| Date | Commit | STATE_ENCODING | extraction mix | trace_fetch p95 | parse p95 | revm_estimate p95 | RPC vs CPU (s/s) | executor hit % | Notes |
|------|--------|----------------|----------------|-----------------|-----------|-------------------|------------------|----------------|-------|
