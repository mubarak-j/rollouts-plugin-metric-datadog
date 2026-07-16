# Argo Rollouts Datadog Metric Plugin — Design

**Date:** 2026-06-18
**Status:** Design — approved for spec review
**Upstream repo:** `github.com/mubarak-j/rollouts-plugin-metric-datadog` (internal, for now)

## 1. Summary

A single Argo Rollouts **metric plugin** (a standalone Go binary) that lets
AnalysisTemplates query Datadog across five data sources (three in Phase 1, two
in Phase 2 — see §13):

- **metrics** — scalar metric queries (parity with the built-in Datadog provider) — *Phase 1*
- **monitor** — monitor (group) search by tags, or a single monitor by ID — *Phase 1*
- **slo** — SLO search by tags, or a single SLO by ID — *Phase 1*
- **apm** — APM/span analytics aggregation — *Phase 2*
- **logs** — log analytics aggregation — *Phase 2*

The plugin's purpose is to extend Datadog analysis beyond the metrics-only
built-in provider to monitors, SLOs, APM traces, and logs — with **first-class
tag-based filtering across every source** — so that canary/blue-green analysis
can ask questions like *"are any monitors for `service:checkout env:production`
alerting?"* or *"is any matching SLO breached?"* without hand-built HTTP jobs.

## 2. Background and motivation

### 2.1 The built-in Datadog provider (what exists today)
Argo Rollouts ships a built-in Datadog **metric** provider
(`metricproviders/datadog`). It is **metrics-only**: it issues a scalar query
(`/api/v2/query/scalar` for v2, `/api/v1/query` for v1), parses a single value,
and evaluates it against `successCondition`/`failureCondition`. It has no concept
of monitors, SLOs, APM spans, or logs.

### 2.2 The current Ibotta workaround
Monitor checks today are done with Argo's generic **`web`** provider against the
Monitor Group Search API, e.g.:

```yaml
provider:
  web:
    url: "https://api.datadoghq.com/api/v1/monitor/groups/search?query=muted:false%20service:{{ args.monitor-analysis-service-name }}{{ args.monitor-analysis-extra-tags }}"
    headers:
      - { key: DD-APPLICATION-KEY, value: "{{ args.app-key }}" }
      - { key: DD-API-KEY,         value: "{{ args.api-key }}" }
successCondition: result.counts.status == nil || any(result.counts.status, {.name != "Alert"})
failureCondition: result.counts.status != nil && any(result.counts.status, {.name == "Alert" && .count > 0})
```

Limitations this plugin removes:
- **Manual URI-encoding of tags** — the chart must pre-encode a space-separated
  tag string and thread it through args. The plugin takes a structured
  `tags: [...]` list and builds the query itself.
- **Credentials leak into the CR** — `api-key`/`app-key` are passed through
  `args` into request headers, so they materialize in the AnalysisRun/Rollout
  objects. The plugin resolves the secret in-process; keys never enter the CRs.
- **Monitors only** — the same `web`-per-endpoint pattern would have to be
  hand-rolled again for SLOs, APM, and logs. The plugin unifies all five.

### 2.3 Prior art
No dedicated community Datadog metric plugin exists. The canonical template is
`argoproj-labs/rollouts-plugin-metric-sample-prometheus`. Datadog support today
lives only in the metrics-only built-in provider. Monitors/SLOs/APM/logs as
analysis sources are genuinely new functionality.

### 2.4 Alternatives considered
**Extend the built-in `metricproviders/datadog` provider upstream** instead of a
standalone plugin. Rejected: new capabilities are introduced as standalone plugins
because it is safer and more flexible — the plugin ships and versions
independently of the controller (no argo-rollouts release or controller upgrade is
needed to deliver or fix a source), and it isolates blast radius from the in-tree
provider. Accepted costs: a separate repo to own, per-arch binary builds/releases,
binary distribution + verification, and tracking the argo-rollouts module version
the binary builds against (see §4.4).

## 3. Goals and non-goals

### Goals
- One plugin binary covering all five sources, selectable per metric.
- Tag-based filtering as a first-class, shared concept across all sources.
- Drop-in-compatible monitor behavior with the existing `web`-provider conditions.
- Credentials resolved securely in-process (never in CRs), mirroring the built-in
  provider's resolution order so existing `datadog` secrets work unchanged.
- Multi-region/site support (US1/EU/US3/US5/AP1/gov).
- Identical condition semantics to every other Argo provider (reuse
  `evaluate.EvaluateResult`).
- Fleet-scale rate-limit safety: shared client-side limiting, coalescing, and
  caching so many rollouts sharing one Datadog org budget don't throttle each other
  or fail canaries on `429`s (see §15).

### Non-goals
- Async/long-running queries. All sources are synchronous (one request per
  `Run()`), using aggregation endpoints rather than raw event search + polling.
- Multiple data sources inside a single metric. Composition is done via multiple
  `metrics[]` entries (see §10). One source per metric.
- Re-implementing arbitrary Datadog API surface. Only the endpoints needed by the
  five sources are used.

## 4. Architecture

### 4.1 One binary, internal `DataSource` strategy
A single executable implements the Rollouts `MetricProviderPlugin` RPC interface
once. Inside, a small `DataSource` interface has one implementation per source,
selected by which config sub-block is present:

```go
type DataSource interface {
    // Query fetches and returns a value (scalar float, string, or structured
    // map) plus metadata describing what was queried. The caller feeds the
    // value to evaluate.EvaluateResult and maps errors to the Measurement.
    Query(ctx context.Context, run *v1alpha1.AnalysisRun, metric v1alpha1.Metric,
        cfg *Config) (value interface{}, metadata map[string]string, err error)
}
```

Shared infrastructure lives once in the `Run()` wrapper: Datadog client
construction, credential resolution, site mapping, tag application, the
`evaluate.EvaluateResult` call, and error→`Measurement` mapping. Each source is a
focused, independently testable unit. New sources (apm, logs in phase 2) are new
`DataSource` implementations with zero churn to the core.

### 4.2 Datadog access via the official Go SDK
Built on `github.com/DataDog/datadog-api-client-go/v2`
(sub-packages `datadogV1`, `datadogV2`, `datadog`). The SDK covers every endpoint
with typed request/response, one auth+site path, and built-in pagination — so we
avoid hand-rolling structs and parsing across ~7 heterogeneous endpoints.

| Source | SDK call | Endpoint |
|---|---|---|
| metrics | `datadogV2.MetricsApi.QueryScalarData` (v2) / v1 query | `/api/v2/query/scalar` |
| monitor (search) | `datadogV1.MonitorsApi.SearchMonitorGroups` | `/api/v1/monitor/groups/search` |
| monitor (by id) | `datadogV1.MonitorsApi.GetMonitor` | `/api/v1/monitor/{id}` |
| slo (search) | `datadogV1.ServiceLevelObjectivesApi.SearchSLO` | `/api/v1/slo/search` |
| slo (by id) | `datadogV1.ServiceLevelObjectivesApi.GetSLOHistory` | `/api/v1/slo/{id}/history` |
| apm | `datadogV2.SpansApi.AggregateSpans` | `/api/v2/spans/analytics/aggregate` |
| logs | `datadogV2.LogsApi.AggregateLogs` | `/api/v2/logs/analytics/aggregate` |

### 4.3 Plugin mechanism (fixed by Argo Rollouts)
- **Transport:** HashiCorp go-plugin over `net/rpc`/gob (not gRPC). The controller
  launches the plugin as a managed, long-lived child process and talks over a
  local socket. Plugins must be Go.
- **Handshake** (must match exactly in `main.go`):
  - `ProtocolVersion: 1`
  - `MagicCookieKey: "ARGO_ROLLOUTS_RPC_PLUGIN"`
  - `MagicCookieValue: "metricprovider"`
  - plugin map key dispensed by the controller: `"RpcMetricProviderPlugin"`
- **Interface implemented:** `InitPlugin`, `Run`, `Resume`, `Terminate`,
  `GarbageCollect`, `Type`, `GetMetadata`. Because the plugin is synchronous,
  `Resume`/`Terminate` return the measurement unchanged and `GarbageCollect`
  returns an empty `RpcError{}`. All work happens in `Run`.
- **Install / discovery:** the controller `argo-rollouts-config` ConfigMap lists
  the plugin under `metricProviderPlugins`:
  ```yaml
  data:
    metricProviderPlugins: |-
      - name: "mubarak-j/rollouts-plugin-metric-datadog"
        location: "file://./plugin-bin/mubarak-j/rollouts-plugin-metric-datadog"
        # or location: "https://github.com/mubarak-j/rollouts-plugin-metric-datadog/releases/download/vX/...-linux-amd64"
        #    sha256: "<hex>"
  ```
  The binary is resolved under `<cwd>/plugin-bin/<namespace>/<name>`. Two install
  methods: (1) initContainer copies the binary into a shared `plugin-bin` volume,
  or (2) `https://` location with `sha256` verification at controller startup.
- The plugin **name in the ConfigMap must equal the map key** used in
  `provider.plugin.<name>` in AnalysisTemplates. The canonical name is
  `mubarak-j/rollouts-plugin-metric-datadog`; operators may alias it.

### 4.4 Argo Rollouts version compatibility
The plugin imports argo-rollouts packages (`v1alpha1`, `utils/evaluate`,
`utils/metric`, RPC types) as a pinned Go-module dependency and runs as a child
process of the controller, which may be a different version. The `net/rpc`/gob
handshake (`ProtocolVersion: 1`) and the `Metric`/`Measurement` struct shapes are
the compatibility surface; a binary built against a drifted module can mis-marshal
or fail at `Run()`. Policy: each release states the supported argo-rollouts
controller version range, the plugin's `go.mod` argo-rollouts version should track
the controller it runs in, and a `ProtocolVersion` mismatch surfaces as a
handshake failure in the controller log at plugin startup.

## 5. Configuration schema

Config is schemaless from the CRD's perspective: the plugin receives
`metric.Provider.Plugin["mubarak-j/rollouts-plugin-metric-datadog"]` as raw
JSON and unmarshals it into its own struct.

### 5.1 Top-level shape (per-source nested blocks)

```yaml
provider:
  plugin:
    mubarak-j/rollouts-plugin-metric-datadog:
      # --- shared connection (all optional; resolved via the credential chain) ---
      site: "datadoghq.com"          # or datadoghq.eu / us3.datadoghq.com / us5.datadoghq.com / ap1.datadoghq.com / gov
      address: "https://api.datadoghq.com"   # optional full-URL override (built-in compat)
      secretRef:
        name: my-datadog-secret
        namespaced: false
      timeoutSeconds: 30             # optional; default 30
      # rateLimit / cache / retry: fleet-scale controls (all optional) — see §15.4

      # --- shared tag filter, applied per source (see §7) ---
      tags:
        - "service:my-cool-service"
        - "env:production"

      # --- EXACTLY ONE of the five source blocks (validated) ---
      metrics: { ... }
      monitor: { ... }
      slo:     { ... }
      apm:     { ... }
      logs:    { ... }
```

**Validation rules:**
- Exactly one of `metrics`/`monitor`/`slo`/`apm`/`logs` must be present
  (`apm`/`logs` are Phase 2 — see §13).
- `metrics`: exactly one of `query`/`queries`; `formula` requires `queries`;
  `>1` query requires `formula`; `aggregator` only valid for v2; `apiVersion` ∈
  {v1, v2} (default v2).
- `tags` combined with a by-id mode (`monitor.id`/`slo.id`) are rejected at validation time (§7.4).
- `secretRef.namespaced: true` requires a non-empty `name` — rejected at
  validation time (there is no fallback when the name is empty).
- `site` and `address` are mutually exclusive ways to target a region; if both are
  set, `address` wins.

### 5.2 Per-source blocks

#### metrics (built-in parity)
```yaml
metrics:
  apiVersion: v2                 # v1 | v2 (default v2)
  query: "avg:trace.http.request.duration{*}"   # v1, or single v2 query
  queries:                       # v2 multi-query
    a: "sum:trace.http.request.hits{*}.as_count()"
    b: "sum:trace.http.request.errors{*}.as_count()"
  formula: "(a - b) / a"         # v2 only; required when >1 query
  aggregator: "last"             # v2 only
  interval: "5m"                 # Datadog lookback window (default 5m)
```
`result` = scalar float. Example: `successCondition: "result >= 0.95"`. In the
SDK `aggregator` is a property of each scalar query, not of the formula; the single
block-level value is applied to every (sub-)query in a formula.

#### monitor
```yaml
monitor:
  mode: search                   # default; or set `id:` for by-ID mode
  query: "muted:false"           # optional extra query terms ANDed with shared tags
  # id: 1234567                  # by-ID mode → GetMonitor
```
- **search** (default): `SearchMonitorGroups` with `query = <tags> <extra query>`.
  `result` = the group-search response (`counts.status` `[{name, count}]`,
  `groups`, `metadata`) — unchanged shape, so existing conditions work:
  ```yaml
  failureCondition: "any(result.counts.status, {.name == 'Alert' && .count > 0})"
  successCondition: "result.counts.status == nil || any(result.counts.status, {.name != 'Alert'})"
  ```
- **by-id**: `GetMonitor`. `result` = monitor JSON; e.g.
  `successCondition: "result.overall_state == 'OK'"`.

#### slo
```yaml
slo:
  mode: search                   # default; or set `id:` for by-ID mode
  query: "slo_type:metric"       # optional extra query terms ANDed with shared tags
  # id: "abcd1234..."            # by-ID mode → GetSLOHistory
  # interval: "7d"               # by-ID lookback window for history (default 7d)
  # field: sli                   # by-ID only: sli only (error-budget-remaining is not returned by GetSLOHistory — use search mode)
```
- **search** (default): `SearchSLO` with `query = <tags> <extra query>`. Breach
  state is returned **inline** per SLO. `result` is lightly flattened for
  ergonomics to `{ slos: [<attributes>...], facets: <...> }`, where each element
  carries `overall_status` (`[{state, status, target, errorBudgetRemaining, ...}]`,
  `state` ∈ breached|warning|ok|no_data), `name`, `all_tags`/`service_tags`/
  `env_tags`/`team_tags`, `groups`, `thresholds`:
  ```yaml
  failureCondition: "any(result.slos, {any(.overall_status, {.state == 'breached'})})"
  successCondition: "all(result.slos, {all(.overall_status, {.state != 'breached'})})"
  ```
- **by-id**: `GetSLOHistory` over `interval`. `result` = SLI attainment % float;
  e.g. `successCondition: "result >= 99.9"`. Error-budget-remaining is **not**
  returned by `GetSLOHistory`; use **search** mode, which carries
  `errorBudgetRemaining` inline per SLO via `overall_status`.

#### apm
```yaml
apm:
  query: "operation_name:http.request"   # span filter; shared tags ANDed in
  aggregation: pc95                       # count|cardinality|avg|min|max|sum|median|pc75|pc90|pc95|pc98|pc99
  metric: "@duration"                     # required for measure aggregations (percentiles, avg, …); count/cardinality ignore it
  interval: "5m"                          # lookback window
```
`result` = scalar float (hits, p95 latency, error rate, …). Example:
`failureCondition: "result > 500"`. Extraction: a single unnamed `compute` with
no group-by yields one bucket; the plugin reads that bucket's value. Zero buckets →
nil (empty-result handling, §9).

#### logs
```yaml
logs:
  query: "status:error"          # log search filter; shared tags ANDed in
  aggregation: count             # count|cardinality|avg|min|max|sum|median|pc75|pc90|pc95|pc98|pc99
  metric: "@duration"            # required for measure aggregations
  indexes: ["main"]              # optional
  interval: "5m"                 # lookback window
```
`result` = scalar float (e.g. error-log count). Example:
`failureCondition: "result > 10"`. Extraction uses the same single-bucket rule as
apm above; `indexes` maps to the SDK request `Filter.Indexes`.

### 5.3 Time windows (`interval`)
`interval` is a Go duration string (e.g. `5m`, `7d`). At call time the plugin
computes `to = now`, `from = now - interval`, and converts to the unit each
endpoint expects (v2 scalar/spans/logs take epoch **milliseconds**; `GetSLOHistory`
takes epoch **seconds**). Defaults: 5m for metrics/apm/logs, 7d for slo by-id.

## 6. Result and verdict model

**Core principle:** the plugin never hard-codes pass/fail. Each source resolves to
a value (scalar, string, or structured map). `Run()` calls
`evaluate.EvaluateResult(value, metric, logCtx)` — the same evaluator every Argo
provider uses — so `successCondition`/`failureCondition` semantics are identical.
`evaluate.EvaluateResult` accepts `interface{}`, which is how the `web` provider
already evaluates structured JSON; this is what makes the monitor/SLO structured
results work. **Important:** the Datadog SDK returns *typed Go structs*, whereas
the `web` provider feeds raw `map[string]interface{}`. To keep existing
`web`-provider conditions (`result.counts.status`, …) working unchanged, each
structured source marshals its typed SDK response to JSON and re-unmarshals it into
`map[string]interface{}` before `EvaluateResult`, with the field names verified to
match the raw Datadog API JSON the conditions traverse (covered by a §12 test).
Skipping this conversion would let a migrated condition silently stop matching.

Result shapes by source:
- metrics, apm, logs → scalar float
- monitor (by-id) → monitor object (`result.overall_state`)
- monitor (search) → group-search response (`result.counts.status`, …)
- slo (by-id) → scalar float (SLI %)
- slo (search) → `{ slos: [...], facets: {...} }`

## 7. Tag filtering (shared `tags`)

`tags` is a top-level list of raw Datadog tag terms, AND-combined. List-of-strings
(not a map) so negation (`!env:staging`) and wildcards (`service:web-*`) work.
Applied differently per source because the sources differ structurally:

### 7.1 Query-string sources (apm, logs)
Tags are ANDed into the search `filter.query` string:
`<user query> service:my-cool-service env:production`.

### 7.2 Metric-scope source (metrics)
The metric scope lives inside the query braces. Tags are merged into the `{…}`
scope of **each** (sub-)query in a v2 formula. The scope brace is the first `{…}`
immediately following a metric name; a trailing `by {…}` grouping clause (and any
`.as_count()`/function suffix) is parsed separately and preserved, and each
sub-query in a formula is rewritten independently. Merge rules:
- `metric{*}` or `metric{}` → `metric{service:my-cool-service,env:production}`
- `metric{existing:tag}` → `metric{existing:tag,service:my-cool-service,env:production}`
- grouping (`by {…}`) is preserved.
- a query with no scope brace gets one appended after the metric name.
- same-key collision (a key present in both the query scope and shared `tags`):
  both terms are kept (ANDed); the plugin does not dedupe or override, so the
  condition author is responsible for non-contradictory keys.

### 7.3 Search sources (monitor, slo)
Tags are joined into the search `query` (e.g. `service:my-cool-service env:production`),
combined with the per-source optional `query` terms (e.g. `muted:false`).

### 7.4 By-ID modes
When `monitor.id`/`slo.id` is set, the object's scope is fixed; `tags` are not
applicable. Behavior: tags are **rejected at validation time** when combined with
a by-ID mode (config error with a clear message). Tag-driven group/SLI selection
within a single object is out of scope; use search mode for tag filtering.

## 8. Credentials, RBAC, region

### 8.1 Resolution order (mirrors the built-in provider)
1. Per-metric `secretRef` (`name`, `namespaced`). `namespaced: true` reads from the
   AnalysisRun's namespace; otherwise the controller namespace.
2. Environment variables: `DD_API_KEY`, `DD_APP_KEY`, `DD_ADDRESS`.
3. A secret literally named `datadog` in the controller namespace.

Secret keys: `api-key`, `app-key`, optional `address`. (The existing Ibotta
`datadog` secret with `api-key`/`app-key` drops straight into step 3.) Region is
otherwise selected by the top-level `site` config field (§5.1, §8.3); the built-in
provider has no `DD_SITE` env var and no `site` secret key.

### 8.2 RBAC
The plugin runs as a child process of the controller and inherits its
ServiceAccount, using in-cluster config to read secrets. The default argo-rollouts
install grants the controller SA a cluster-scoped `secrets` get/list/watch
ClusterRole, which already covers the `namespaced: true` cross-namespace case — so
on a default install **no new RBAC** is required. Installs that have narrowed the
SA to namespace-scoped secret access must add a grant for any namespace targeted by
a `namespaced: true` secretRef.

### 8.3 Region / site
Prefer `site` mapped to the SDK's `datadog.ContextServerVariables{"site": …}`.
`address` (full URL) is accepted for built-in compatibility and overrides the
derived server. Auth is set via `datadog.ContextAPIKeys` with `apiKeyAuth` and
`appKeyAuth`.

## 9. Error handling and empty results

- Any SDK error, `401`/`403` auth failure, or parse error →
  `metricutil.MarkMeasurementError(m, err)` (`Phase: Error` + message). This returns
  the measurement by value, so the result must be captured
  (`m = metricutil.MarkMeasurementError(m, err)`). The controller's
  `consecutiveErrorLimit` (default 4) governs transient-error tolerance.
- HTTP timeout default 30s (aggregations are slower than point queries),
  overridable via top-level `timeoutSeconds`.
- **Aggregation latency/throttling (apm/logs):** wide windows, high-cardinality
  filters, or Datadog rate limiting (`429`) can make a synchronous
  `AggregateSpans`/`AggregateLogs` call exceed the timeout, which maps to
  `Phase: Error` and consumes `consecutiveErrorLimit` — failing a canary on Datadog
  latency rather than a real regression. Defenses are detailed in §15 (shared
  client-side rate limiter, header-adaptive control, request coalescing + short-TTL
  cache, header-aware SDK retry, and last-known-good degradation — never `Failed`).
- **Empty results:**
  - *Structured sources* (monitor/slo) pass the real (possibly empty) response to
    expr, so conditions like `result.counts.status == nil || …` keep working;
    emptiness is the condition author's concern.
  - *Scalar sources* (metrics/apm/logs) pass a nil value on empty →
    `evaluate.EvaluateResult` surfaces a clear "no value" error (`Phase: Error`),
    identical to the built-in provider.
- **Observability:** `Measurement.Metadata` (and `GetMetadata`) records the
  resolved query/endpoint and applied tags so operators see exactly what was
  queried from the AnalysisRun status.

## 10. Multi-source composition

One source per metric (validated). To combine sources — e.g. "fail if either a
monitor is alerting or an SLO is breached" — list multiple `metrics[]` in one
AnalysisTemplate. The engine runs each independently; the AnalysisRun **fails if
any metric fails** and succeeds only if all pass. Each signal gets its own
`interval`, `failureLimit`, and `consecutiveErrorLimit`.

```yaml
spec:
  metrics:
    - name: monitors
      interval: 1m
      failureLimit: 2
      provider:
        plugin:
          mubarak-j/rollouts-plugin-metric-datadog:
            tags: ["service:my-cool-service", "env:production"]
            monitor: {}
      failureCondition: "any(result.counts.status, {.name == 'Alert' && .count > 0})"
    - name: slos
      interval: 1m
      failureLimit: 2
      provider:
        plugin:
          mubarak-j/rollouts-plugin-metric-datadog:
            tags: ["service:my-cool-service", "env:production"]
            slo: {}
      failureCondition: "any(result.slos, {any(.overall_status, {.state == 'breached'})})"
```

The multiple-`metrics[]` model expresses OR-of-failures cleanly (any metric failing
fails the run) but cannot express a single cross-source **AND** in one expression.
A use case like "monitor alerting AND SLO budget < 10%" must therefore be modeled
as two independent signals (OR semantics) rather than one combined condition —
genuine cross-source AND within a single expression is explicitly out of scope (see
§3 non-goals).

## 11. Repository layout

Repo: `github.com/mubarak-j/rollouts-plugin-metric-datadog` (internal, for now;
follows the argoproj-labs `<org>/<plugin-name>` naming convention).

```
main.go                      # go-plugin handshake + Serve (from sample)
internal/plugin/
  plugin.go                  # RpcPlugin: InitPlugin/Run/Resume/Terminate/GC/Type/GetMetadata
  config.go                  # config structs + validation
internal/datasource/
  datasource.go              # DataSource interface + dispatch by config block
  metrics.go monitor.go slo.go    # Phase 1
  apm.go logs.go                  # Phase 2
  tags.go                    # shared tag application per source
internal/datadog/
  client.go                  # SDK client construction, site mapping
  credentials.go             # credential resolver + kube client
docs/  examples/  Dockerfile  Makefile  .github/workflows/
```

Code reuse: import argo-rollouts as a Go module dependency (`v1alpha1` types,
`metricproviders/plugin/rpc`, `utils/plugin/types`, `utils/evaluate`,
`utils/metric`) rather than copying types — so condition semantics stay identical.

## 12. Testing strategy

- Each `DataSource` unit-tested against an `httptest.Server` returning canned
  Datadog JSON (the SDK targets a configurable server URL). Table-driven cases for
  value parsing, tag injection/merge, empty results, and error mapping.
- Config validation tested directly (exactly-one-source, metrics field rules,
  tags+by-id rejection).
- One `evaluate.EvaluateResult` integration test per source proving condition
  semantics (incl. the structured monitor/SLO conditions).
- A migration-parity test: feed canned `SearchMonitorGroups` JSON through the
  struct→`map[string]interface{}` conversion (§6) and assert the existing
  `web`-provider conditions (`result.counts.status`, …) still match — guarding the
  drop-in goal against SDK field-name drift.
- CI (GitHub Actions): build per-arch binaries, run tests, publish artifacts +
  `sha256`.

## 13. Phasing

**Phase 1** — scaffolding + the synchronous lookup sources:
- go-plugin handshake, RPC impl, config + validation, SDK client construction,
  credential resolver, `DataSource` interface and dispatch.
- `metrics` (built-in parity), `monitor` (tag search + by-id — the migration
  target), `slo` (tag search + by-id).
- Rate-limit & scale core (§15): shared limiter, header-adaptive control,
  coalescing + short-TTL cache, header-aware SDK retry, last-known-good degradation.
- Docs + examples including the Ibotta `web`-provider → plugin migration.

**Phase 2** — the aggregation sources:
- `apm` (`AggregateSpans`) and `logs` (`AggregateLogs`) as two new `DataSource`
  implementations, no core churn.
- apm/logs cache/interval tuning and usage guidance for the much stricter analytics
  budgets (§15).
- Expanded examples.

## 14. Distribution and migration

- GitHub releases with per-arch binaries + `sha256`. Controller
  `argo-rollouts-config` references via `file://` (initContainer copy) or
  `https://` with sha verification. Guidance: prefer the **initContainer**
  (`file://`) method when the cluster has no outbound internet access or already
  manages binaries via image workflows; use **`https://`** for simple clusters with
  internet egress where image management is a burden.
- Migration doc: the Ibotta `cluster-datadog-monitor.yaml` (`web` provider, manual
  tag encoding, keys-in-args) → the plugin form (structured `tags`, in-process
  credential resolution, same conditions).

## 15. Rate limiting and scale resilience

### 15.1 The problem
Datadog API rate limits are enforced **per-organization** (also per-API-key and
per-user), so every rollout in the cluster shares one budget — and shares it with
all other Datadog API consumers in the org (Terraform, dashboards, automation). At
fleet scale (e.g. 200 concurrent rollouts) the plugin can both exhaust that budget
and be starved by other consumers. The failure that matters: sustained `429`s
become `Phase: Error`, and after `consecutiveErrorLimit` (default 4) consecutive
errors the rollout is **aborted on Datadog throttling rather than on a real
regression**. A throttle must never map to `Failed` — `failureLimit` defaults to 0,
so a single `Failed` aborts immediately.

Known limits (confirmed where cited; others discovered at runtime — §15.3):
- `/api/v1/query`: **1,600 req / 60s (~26/s)** per-org (confirmed).
- `v2/query/scalar`, monitor, and SLO endpoints: not publicly documented.
- apm (`spans/analytics/aggregate`) and logs (`logs/analytics/aggregate`): reported
  ~**300 req/hour** (~0.08/s) — if accurate, ~300× stricter; ~5 rollouts polling
  logs every 60s saturate the whole org budget, 200 would be ~40× over.

Two scale effects:
- **Burst alignment.** The controller adds no jitter and requeues each metric
  exactly `interval` after the previous finishes; canaries co-started in a deploy
  wave fire aligned, so N rollouts produce an N-call spike within ~1s even when
  steady-state QPS is safe.
- **Steady-state aggregate.** 200 rollouts × M metrics ÷ `interval`, summed per
  rate-limit bucket; the analytics endpoints are the binding constraint.

### 15.2 Why a client-side defense works here
The plugin is a **singleton process** per controller (§4.3) and, with leader
election on by default, exactly one controller is active cluster-wide — so a single
in-process layer sees **100% of the cluster's Datadog traffic** and can coordinate
globally. The controller calls `Run()` from up to 30 concurrent workers (plus
per-run metric goroutines), so this shared state must be goroutine-safe.

### 15.3 Defense layers (shared `Run()` wrapper — Phase 1 core)
1. **Shared token-bucket limiter**, one bucket per Datadog rate-limit group (keyed
   by the `X-RateLimit-Name` response header), sized to a configurable fraction of
   the budget. `Run()` blocks on the limiter before issuing, converting bursts into
   a sustained sub-ceiling rate. Goroutine-safe (`golang.org/x/time/rate`).
2. **Adaptive control from headers.** Every response carries
   `X-RateLimit-Limit/Remaining/Reset/Name`; the limiter learns the (often
   undocumented) ceiling from `X-RateLimit-Limit`, slows as `Remaining`→0, and on
   `429` waits `X-RateLimit-Reset` seconds (Datadog sends no `Retry-After`).
3. **Coalescing + short-TTL cache.** Identical concurrent queries are collapsed via
   singleflight, and results are cached for a short TTL (≤ the metric `interval`),
   keyed by the fully-resolved query + tags + window + site. Many rollouts sharing
   the same `service:`/`env:` tags then cost one upstream call instead of N — this
   is what makes apm/logs survivable at scale. Opt-out per metric for teams that
   need strictly per-call freshness.
4. **Concurrency semaphore** caps in-flight outbound calls regardless of how many
   `Run()`s arrive at once.
5. **SDK retry enabled.** `RetryConfiguration.EnableRetry = true` (the SDK default
   is off) so `429`/5xx are retried honoring `X-Ratelimit-Reset`, with
   `MaxRetries`/`HTTPRetryTimeout` bounded below the plugin `timeoutSeconds`.
6. **Safe degradation — never `Failed`.** The order on pressure is: limiter wait →
   coalesced/cached value → SDK retry; if still throttled within the `Run()`
   deadline, serve the **last-known-good cached value** when one exists within
   `maxStaleness`, otherwise return `Error` (bounded by `consecutiveErrorLimit`,
   auto-recovering on the next success). A throttle never yields `Failed`.
7. **Observability.** `Measurement.Metadata` records the bucket (`X-RateLimit-Name`),
   remaining budget, limiter wait, and whether the value was fresh / coalesced /
   cached / last-good, so operators see budget pressure in AnalysisRun status.

### 15.4 Configuration surface
Top-level, alongside `site`/`timeoutSeconds`; all optional with safe defaults:
```yaml
rateLimit:
  enabled: true                 # default true
  budgetFraction: 0.5           # use at most 50% of the discovered/declared org budget
  buckets:                      # optional static ceilings used until headers are seen
    metrics_query: { rps: 13 }  # e.g. ~half of 1600/60s
  maxConcurrent: 16             # outbound in-flight cap
cache:
  enabled: true                 # default true; opt-out per metric via cache: {enabled: false}
  ttl: "30s"                    # default min(interval, 30s)
  maxStaleness: "5m"            # last-known-good ceiling for degradation
retry:
  maxRetries: 3                 # SDK retry; X-Ratelimit-Reset honored
```

### 15.5 Operator guidance
- Coordinate `budgetFraction` with the org's other Datadog API consumers.
- Prefer longer `interval`s and shared tags for apm/logs metrics; per-rollout
  high-frequency log/APM polling does not scale and should be the exception.
- Request per-bucket limit increases from Datadog using the `X-RateLimit-Name` value
  surfaced in `Measurement.Metadata`.
- Stagger large deploy waves where possible; the limiter de-synchronizes bursts but
  cannot create budget that doesn't exist.

### 15.6 Testing
- Limiter/concurrency: assert issued QPS stays under the configured ceiling under N
  concurrent `Run()`s.
- Coalescing/cache: assert K identical concurrent queries produce 1 upstream call;
  assert TTL, `maxStaleness`, and last-good degradation.
- 429 handling: an `httptest.Server` returning `429` + `X-RateLimit-Reset` asserts
  retry timing and the Error-vs-last-good path, and that a throttle never yields
  `Failed`.

## 16. References

Verified during design (2026-06-18):
- Argo Rollouts plugin system & analysis plugin docs
  (argo-rollouts.readthedocs.io) — go-plugin/`net/rpc`, handshake, ConfigMap,
  `plugin-bin` resolution.
- Built-in Datadog provider source (`metricproviders/datadog`) — credential
  resolution order, v1/v2 endpoints, scalar parsing.
- `argoproj-labs/rollouts-plugin-metric-sample-prometheus` — template structure.
- Datadog Go SDK `github.com/DataDog/datadog-api-client-go/v2` — confirmed:
  module path; `datadogV1`/`datadogV2` sub-packages; auth via
  `datadog.ContextAPIKeys` (`apiKeyAuth`/`appKeyAuth`); site via
  `datadog.ContextServerVariables{"site"}`.
- SLO search inline status — confirmed from SDK source: `SearchSLOResponse →
  data.attributes.slos[] (SearchServiceLevelObjective)`, whose attributes carry
  `OverallStatus []SLOOverallStatuses` with `state` (breached/warning/ok/no_data),
  `status` (SLI), `target`, `errorBudgetRemaining`, plus `all_tags`/`service_tags`/
  `env_tags`/`team_tags`, `name`, `groups`, `thresholds`.
- Ibotta current approach: `kubernetes-deployments`
  `.../argo-rollouts/analysisTemplate/cluster-datadog-monitor.yaml` (Monitor Group
  Search via the `web` provider).

Verified during rate-limit analysis (2026-06-19):
- Datadog API rate limits — per-org scope; `X-RateLimit-Limit/Period/Remaining/Reset/Name`
  headers on every response; no `Retry-After`
  (docs.datadoghq.com/api/latest/rate-limits/).
- `/api/v1/query` = 1,600 req/60s — confirmed via a verbatim Datadog throttle error
  (kedacore/keda#5521). apm/logs analytics aggregate ~300/hr is reported by secondary
  sources only (treat as unconfirmed; discover via `X-RateLimit-Limit` at runtime).
- Datadog Go SDK retry — `RetryConfiguration{EnableRetry (default false), MaxRetries 3,
  BackOffBase 2, BackOffMultiplier 2, HTTPRetryTimeout 60s}`, honors `X-Ratelimit-Reset`
  on 429, client-global only (`api/datadog/client.go` `shouldRetryRequest`,
  `api/datadog/configuration.go`).
- Argo Rollouts execution model — singleton plugin process
  (`metricproviders/plugin/client/client.go`, `sync.Once`); 30 analysis workers
  (`DefaultAnalysisThreads`); leader election on by default; no interval jitter; phase
  semantics (`Error`/`Failed`/`Inconclusive`, `consecutiveErrorLimit` default 4,
  `failureLimit`/`inconclusiveLimit` default 0) — `analysis/analysis.go`,
  `pkg/apis/rollouts/v1alpha1/analysis_types.go`.
