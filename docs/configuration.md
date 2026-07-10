# Configuration Reference

Every metric in an AnalysisTemplate that uses this plugin is configured under the plugin key `mubarak-j/rollouts-plugin-metric-datadog`. Exactly one of the three source blocks (`metrics`, `monitor`, `slo`) must be present per metric entry.

---

## Full Schema

```yaml
provider:
  plugin:
    mubarak-j/rollouts-plugin-metric-datadog:
      # --- Connection ---
      site: "datadoghq.com"          # Datadog site (see Site Values below). Default: datadoghq.com
      address: ""                    # Full URL override (https://...). Wins over site when set.
      secretRef:
        name: "my-dd-secret"         # Name of the Kubernetes Secret containing api-key/app-key
        namespaced: false            # If true, read from the AnalysisRun's namespace instead of the controller's
      timeoutSeconds: 30             # Per-measurement HTTP timeout. Default: 30s. See Timeout note below.

      # --- Shared tag filter ---
      tags: ["service:my-service", "env:production"]

      # --- Rate limiting (optional) ---
      rateLimit:
        enabled: true                # Default: true
        budgetFraction: 0.5          # Fraction of Datadog's reported limit to use. Default: 0.5
        maxConcurrent: 16            # Max concurrent in-flight requests. Default: 16
        buckets:                     # Per-bucket static RPS ceilings (keyed by X-RateLimit-Name)
          metrics: {rps: 5.0}

      # --- Caching (optional) ---
      cache:
        enabled: true                # Default: true
        ttl: "30s"                   # How long to serve a cached result. Default: 30s

      # --- SDK retry (optional) ---
      retry:
        maxRetries: 3                # SDK-level retries on 429/5xx. Default: 3

      # --- Source block (exactly one required) ---
      metrics: ...                   # See Metrics Source below
      monitor: ...                   # See Monitor Source below
      slo: ...                       # See SLO Source below
```

---

## Credential Resolution Order

Credentials are resolved in this order (first match wins):

1. **`secretRef.name`** — reads the named Kubernetes Secret. If `namespaced: false` (default), the Secret is read from the controller's namespace. If `namespaced: true`, it is read from the AnalysisRun's namespace.
2. **Environment variables** — `DD_API_KEY` and `DD_APP_KEY` (both required; `DD_ADDRESS` optional).
3. **`datadog` Secret in the controller namespace** — a Secret literally named `datadog` in the namespace the controller runs in. This is a common convention for the Datadog secret; the plugin reads it automatically with no additional config.

All Secrets must have `api-key` and `app-key` keys. An optional `address` key overrides the endpoint URL.

**Security note (Fix #3 — secretRef escalation):** Because `secretRef.name` is unrestricted and `namespaced: true` reads from the AnalysisRun's namespace via the controller ServiceAccount, **anyone who can author AnalysisTemplates or AnalysisRuns in a namespace can instruct the controller to read any Secret in that namespace and forward its `api-key`/`app-key` to Datadog.** Operators must treat "can author AnalysisTemplates in namespace N" as equivalent to "can read all Secrets in N", or restrict eligible Secret names by convention, label, or admission policy. See [install.md](install.md#rbac) for details.

---

## Site Values

The `site` field controls the Datadog regional endpoint. Common values:

| Site | Endpoint |
|---|---|
| `datadoghq.com` (default) | US1 |
| `us3.datadoghq.com` | US3 |
| `us5.datadoghq.com` | US5 |
| `datadoghq.eu` | EU1 |
| `ap1.datadoghq.com` | AP1 |
| `ddog-gov.com` | US1-FED |

Use `address` instead of `site` to route to a custom proxy or local test server.

---

## Timeout and Rate-Limit Retry (Fix #5)

`timeoutSeconds` sets the hard per-measurement deadline. The SDK's internal retry timer is bounded to `timeout - timeout/5` so the plugin can observe a clean context cancellation.

For `metrics` and `monitor`, a 429 response triggers the SDK to honor the `X-RateLimit-Reset` header, which can be up to ~60 seconds for the `/api/v1/query` endpoint. The retry fires only if time remains within the SDK retry window (`timeout * 4/5`).

**Recommendation:** Set `timeoutSeconds` to at least **60–90 seconds** if you need the header-aware retry to fire before the context deadline. With the default of 30s, the SDK retry window is only 24s — a Reset value near 60s will exhaust the deadline before the retry can run.

---

## Metrics Source

```yaml
metrics:
  apiVersion: v2          # v1 or v2. Default: v2
  query: "..."            # Single DQL query (v1 or v2). Mutually exclusive with queries.
  queries:                # Named queries for multi-formula (v2 only).
    a: "sum:trace.http.request.hits{*}.as_count()"
    b: "sum:trace.http.request.errors{*}.as_count()"
  formula: "(a - b) / a"  # Required when queries has more than one entry.
  aggregator: "last"      # v2 aggregator. One of: avg, min, max, sum, last. Default: last
  interval: "5m"          # Query window width. Default: 5m. Supports Go durations and "Nd" for days.
```

Tags in `tags` are merged into the query's `{...}` scope brace (or appended if none). For `query`, the raw string is merged. For `queries`, each named query is merged independently.

**Result shape:** a scalar `float64` (or `nil` if the query returns no data). Use `result` in conditions.

```yaml
successCondition: "result >= 0.95"
```

### Time window semantics

`interval` defines the width of the `[now - interval, now]` window sent to the API. If omitted, the default is 5 minutes. For v1, the window bounds are seconds-precision Unix timestamps; for v2, milliseconds. The window is computed at query time, not cached.

---

## Monitor Source

```yaml
monitor:
  query: "muted:false"    # Additional search terms (ANDed with tags). Optional.
  id: 12345678            # Integer monitor ID. Switches to by-id mode (tags not allowed with id).
```

**Search mode** (no `id`): calls `SearchMonitorGroups` with a query built from `tags` joined with `monitor.query`. Result is the full `SearchMonitorGroupsResponse` marshalled to a map.

**By-id mode** (`id` set): calls `GetMonitor`. Result is the Monitor object marshalled to a map. Tags must be empty in this mode.

**Result shape (search):** `map[string]interface{}` matching the Datadog API JSON. The useful path for status rollup is `result.counts.status`, a list of `{name, count}` objects.

```yaml
failureCondition: "any(result.counts.status, {.name == 'Alert' && .count > 0})"
successCondition: "result.counts.status == nil || any(result.counts.status, {.name != 'Alert'})"
```

---

## SLO Source

```yaml
slo:
  query: ""               # Additional search terms (ANDed with tags). Optional.
  id: "abc123def456"      # String SLO ID. Switches to by-id mode (tags not allowed with id).
  interval: "7d"          # History window for by-id mode. Default: 7d. Supports "Nd" for days.
```

**Search mode** (no `id`): calls `SearchSLO` with a query built from `tags` joined with `slo.query`. Result is `{slos: [...], facets: {...}}` where each element of `slos` is the SLO attributes object.

**By-id mode** (`id` set): calls `GetSLOHistory` over the configured `interval`. Result is the scalar SLI value (`float64`). Tags must be empty in this mode.

**Result shape (search):**

```yaml
failureCondition: "any(result.slos, {any(.overall_status, {.state == 'breached'})})"
successCondition: "all(result.slos, {all(.overall_status, {.state != 'breached'})})"
```

**Result shape (by-id):** scalar `float64` SLI value.

```yaml
successCondition: "result >= 99.9"
```

---

## Result Shape Table

| Source | Mode | `result` type | Example condition |
|---|---|---|---|
| `metrics` | v1 or v2 | `float64` (nil if no data) | `result >= 0.95` |
| `monitor` | search | `map` — `result.counts.status[].{name, count}` | `any(result.counts.status, {.name == 'Alert' && .count > 0})` |
| `monitor` | by-id | `map` — full Monitor JSON | field access per Monitor schema |
| `slo` | search | `map` — `result.slos[].overall_status[].{state, ...}` | `any(result.slos, {any(.overall_status, {.state == 'breached'})})` |
| `slo` | by-id | `float64` SLI value | `result >= 99.9` |

---

## Service-Tag Guidance (Fix #7)

Each metric **should** carry a `service:...` tag so the resolved query uniquely identifies the resource being gated. A monitor or SLO search scoped only by `env:production` (or similar broad tags) may match resources across multiple rollouts, causing the shared cache to coalesce results from different services.

The plugin emits a log warning (surfaced by `Config.Warnings()`) when a monitor or SLO source is in search mode and has neither a `service:...` tag nor a per-source `query`. The examples in this repository all use `tags: ["service:my-cool-service", "env:production"]` as the recommended baseline.

---

## Rate Limit and Cache Defaults

| Field | Default |
|---|---|
| `rateLimit.enabled` | `true` |
| `rateLimit.budgetFraction` | `0.5` (50% of reported Datadog limit) (applied by the limiter layer at runtime, not by config defaulting — holds even when omitted or zero) |
| `rateLimit.maxConcurrent` | `16` (applied by the limiter layer at runtime, not by config defaulting — holds even when omitted or zero) |
| `cache.enabled` | `true` |
| `cache.ttl` | `30s` |
| `retry.maxRetries` | `3` |
| `timeoutSeconds` | `30` |
| `metrics.apiVersion` | `v2` |
| `metrics.interval` | `5m` |
| `slo.interval` (by-id) | `7d` |

The rate limiter is shared across all concurrent measurements in the same plugin process. It learns per-endpoint Datadog bucket names from `X-RateLimit-Name` response headers and adapts its token-bucket ceiling accordingly (layer 2 adaptation). The concurrency semaphore (layer 4) prevents thundering-herd bursts during large canary waves.
