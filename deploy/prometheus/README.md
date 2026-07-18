# Prometheus rules & scrape config

This directory ships a ready-to-load Prometheus setup for the resilience toolkit:

| File             | Purpose                                                                 |
| ---------------- | ----------------------------------------------------------------------- |
| `prometheus.yml` | Scrape config + `rule_files` wiring + commented Alertmanager block.      |
| `rules.yml`      | **Recording rules** — precomputed ratios/rates backing the dashboard.   |
| `alerts.yml`     | **Alerting rules** — breaker-open, denial spikes, bulkhead saturation.  |

All rules reference **only metrics the library actually emits** via the
`metric.Recorder` Prometheus adapter (`metric/prometheus/prometheus.go`), served
on the demo server's `/metrics` endpoint:

```
resilience_ratelimit_requests_total{algorithm,result}            # result=allowed|denied
resilience_ratelimit_decision_duration_seconds_bucket{algorithm,le}
resilience_circuitbreaker_state{name}                            # 0=closed,1=half-open,2=open
resilience_circuitbreaker_requests_total{name,result}            # result=success|failure|rejected
resilience_circuitbreaker_state_transitions_total{name,from,to}
resilience_circuitbreaker_execution_duration_seconds_bucket{name,le}
resilience_bulkhead_inflight{name}
resilience_bulkhead_rejected_total{name}
resilience_http_requests_total{method,path,status}               # demo-server HTTP layer
resilience_http_request_duration_seconds_bucket{method,path,le}  # demo-server HTTP layer
```

## Recording rules (`rules.yml`)

Precompute the expensive ratios/quantiles once so both the Grafana dashboard and
the alerts read cheap series:

| Recorded series                                  | Meaning                                          |
| ------------------------------------------------ | ------------------------------------------------ |
| `resilience:ratelimit_requests:rate5m`           | Per-algorithm request throughput (req/s).        |
| `resilience:ratelimit_denied:rate5m`             | Per-algorithm denied throughput (req/s).         |
| `resilience:ratelimit_denial_ratio:rate5m`       | Fraction of decisions denied, 0..1.              |
| `resilience:ratelimit_decision_latency:p99_5m`   | p99 decision latency (s).                        |
| `resilience:cb_requests:rate5m`                  | Per-breaker execution throughput (req/s).        |
| `resilience:cb_failure_ratio:rate5m`             | Per-breaker failure ratio, 0..1.                 |
| `resilience:cb_rejected:rate5m`                  | Per-breaker short-circuited throughput (req/s).  |
| `resilience:cb_execution_latency:p99_5m`         | p99 protected-call latency (s).                  |
| `resilience:bulkhead_rejected:rate5m`            | Per-bulkhead rejection throughput (req/s).       |

Division rules guard against `0/0 = NaN` with `clamp_min(..., 1e-9)` so an idle
algorithm/breaker reports a clean `0`.

## Alerting rules (`alerts.yml`)

| Alert                             | Fires when                                             | Severity |
| --------------------------------- | ------------------------------------------------------ | -------- |
| `RateLimitHighDenialRatio`        | >50% of decisions denied for 10m                       | warning  |
| `RateLimitDenialCritical`         | >90% of decisions denied for 5m                        | critical |
| `RateLimitDecisionLatencyHigh`    | p99 decision latency >50ms for 10m                     | warning  |
| `CircuitBreakerStuckOpen`         | breaker state `== 2` (open) for 5m                     | critical |
| `CircuitBreakerFlapping`          | breaker state `== 1` (half-open) for 15m               | warning  |
| `CircuitBreakerHighFailureRatio`  | >25% failures for 10m                                  | warning  |
| `BulkheadRejectionsElevated`      | rejection rate >1 req/s for 5m                         | warning  |
| `BulkheadNearSaturation`          | in-flight count >50 for 5m (tune to `MaxConcurrency`)  | warning  |

Thresholds are demo defaults — tune `expr` values and `for` windows to your
traffic and SLOs. `BulkheadNearSaturation` in particular should be set just below
the bulkhead's configured `MaxConcurrency`, since no capacity gauge is emitted.

## Loading the rules

The shipped `prometheus.yml` already references both files:

```yaml
rule_files:
  - rules.yml
  - alerts.yml
```

In the docker-compose stack (`docker-compose.yml`), this whole directory is
mounted into the Prometheus container, so the rules load automatically. To load
them into an existing Prometheus after editing:

```bash
# Validate first (see below), then hot-reload without a restart:
curl -X POST http://localhost:9090/-/reload     # requires --web.enable-lifecycle
```

Confirm they loaded under **Status → Rules** in the Prometheus UI, or:

```bash
curl -s http://localhost:9090/api/v1/rules | jq '.data.groups[].name'
```

## Wiring Alertmanager

Alerts only page if an Alertmanager is attached. Uncomment the `alerting:` block
in `prometheus.yml` and point it at your instance:

```yaml
alerting:
  alertmanagers:
    - static_configs:
        - targets: ["alertmanager:9093"]
```

A minimal Alertmanager alongside the docker-compose stack:

```yaml
# alertmanager.yml
route:
  receiver: default
  group_by: ["alertname", "severity"]
receivers:
  - name: default
    # Add slack_configs / pagerduty_configs / webhook_configs here.
```

```yaml
# add to docker-compose.yml services:
alertmanager:
  image: prom/alertmanager:latest
  ports: ["9093:9093"]
  volumes:
    - ./alertmanager.yml:/etc/alertmanager/alertmanager.yml:ro
```

## Standalone scrape example (`/metrics`)

If you are not using docker-compose and just want to scrape a locally running
demo server (`go run ./server`, default `:8080`):

```yaml
global:
  scrape_interval: 15s
  evaluation_interval: 15s

rule_files:
  - rules.yml
  - alerts.yml

scrape_configs:
  - job_name: "resilience-demo"
    metrics_path: /metrics
    static_configs:
      - targets: ["localhost:8080"]
```

> If the demo server was started with an API key, `/metrics` is auth-gated
> (`server/api/router.go`). Either run it unauthenticated (demo mode) or add the
> key as an `authorization` / bearer-token setting in the scrape config.

## Validating

Rules are checked with `promtool`. With the Prometheus toolbox installed:

```bash
promtool check rules deploy/prometheus/rules.yml deploy/prometheus/alerts.yml
promtool check config deploy/prometheus/prometheus.yml
```

Or via Docker (no local install needed):

```bash
docker run --rm -v "$PWD/deploy/prometheus:/rules" prom/prometheus:latest \
  promtool check rules /rules/rules.yml /rules/alerts.yml
```
