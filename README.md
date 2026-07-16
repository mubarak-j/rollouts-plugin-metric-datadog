# rollouts-plugin-metric-datadog

An [Argo Rollouts](https://argoproj.github.io/argo-rollouts/) metric plugin that gates canary and blue-green rollouts on Datadog observability data. Phase 1 supports three sources: **metrics** (v1 and v2 scalar queries with multi-query formulas), **monitor** (group search or by-id), and **SLO** (search or by-id history). The plugin resolves Datadog credentials in-process, merges structured `tags` into queries automatically, and provides shared rate-limiting and short-TTL caching across concurrent rollout waves. Built and tested against Argo Rollouts v1.9.0.

## Supported sources (Phase 1)

| Source | Modes | Result |
|---|---|---|
| `metrics` | v1 query, v2 scalar formula | scalar `float64` |
| `monitor` | search, by-id | map (search) or map (by-id) |
| `slo` | search, by-id history | map (search) or scalar `float64` (by-id) |

## Documentation

- [docs/install.md](docs/install.md) — install via initContainer or URL download; RBAC notes
- [docs/configuration.md](docs/configuration.md) — full config schema, credential resolution, site values, result shapes
- [docs/migration-web-provider.md](docs/migration-web-provider.md) — migrating from the built-in `web` provider

## Examples

- [examples/metrics-analysistemplate.yaml](examples/metrics-analysistemplate.yaml) — error-rate gate using v2 multi-query formula
- [examples/monitor-analysistemplate.yaml](examples/monitor-analysistemplate.yaml) — monitor group search gate
- [examples/slo-analysistemplate.yaml](examples/slo-analysistemplate.yaml) — SLO search gate
- [examples/multi-source-analysistemplate.yaml](examples/multi-source-analysistemplate.yaml) — monitors + SLOs combined (OR-of-failures)

## Quick start

Add the plugin to the `argo-rollouts-config` ConfigMap and reference it in an AnalysisTemplate:

```yaml
# ConfigMap
metricProviderPlugins: |-
  - name: "mubarak-j/rollouts-plugin-metric-datadog"
    location: "file://./plugin-bin/mubarak-j/rollouts-plugin-metric-datadog"

# AnalysisTemplate metric entry
provider:
  plugin:
    mubarak-j/rollouts-plugin-metric-datadog:
      tags: ["service:my-service", "env:production"]
      metrics:
        apiVersion: v2
        queries:
          a: "sum:trace.http.request.hits{*}.as_count()"
          b: "sum:trace.http.request.errors{*}.as_count()"
        formula: "(a - b) / a"
successCondition: "result >= 0.95"
```
