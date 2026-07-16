# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

A standalone Go binary that implements the Argo Rollouts **`MetricProviderPlugin`** RPC interface, letting AnalysisTemplates gate canary/blue-green rollouts on Datadog data beyond the metrics-only built-in provider. Phase 1 supports three sources: **metrics** (v1/v2 scalar + formula), **monitor** (search or by-id), **slo** (search or by-id history). APM and logs are Phase 2.

The controller launches this binary as a subprocess and speaks to it over `hashicorp/go-plugin` RPC. `main.go` only wires the handshake and serves `internal/plugin.RpcPlugin`.

## Commands

```bash
make build        # CGO_ENABLED=0 go build -o dist/<binary> .
make test         # go test ./... -race -count=1   (matches CI)
make vet          # go vet ./...
test -z "$(gofmt -l .)"                    # formatting gate CI enforces

# single package / single test
go test ./internal/datasource -run TestMetrics -race -count=1

docker build -t rollouts-plugin-metric-datadog .   # produces the initContainer image
```

CI (`.github/workflows/ci.yaml`) runs vet, `-race` tests, the gofmt gate, and a `CGO_ENABLED=0` build on every push/PR.

### Critical: do not run `go mod tidy` casually

`go mod tidy` (and `make tidy`) **strips `github.com/DataDog/zstd`**, a cgo-only transitive dep, which breaks the `-race` build. If you must tidy, restore it immediately:

```bash
go get github.com/DataDog/zstd@v1.5.2
```

Dependabot ignores this dep for the same reason (`.github/dependabot.yml`).

## Architecture

### Request flow (`internal/plugin/plugin.go`)

`InitPlugin()` runs **once per process** — it wires the Kubernetes secret getter, the credential `Resolver`, and the *shared* `Limiter` and `Cache`. `Run()` is the per-measurement hot path:

1. `config.ParseConfig(metric)` — decodes+validates the plugin JSON.
2. `resolver.Resolve(...)` — obtains Datadog credentials in-process.
3. `newClient(...)` — builds a Datadog SDK client whose `Transport` is the shared rate-limit `RoundTripper`.
4. `datasource.Select(cfg)` — picks the one configured source.
5. Build the cache key, then `cache.Do(key, ...)` wraps `ds.Query(...)` with singleflight + TTL.
6. `evaluate.EvaluateResult(value, metric, ...)` → `Measurement`.

Any error becomes `MarkMeasurementError` (a bounded, auto-recovering `Error` phase) — **never `Failed`**. This is load-bearing: `failureLimit` defaults to 0, so mapping a Datadog throttle to `Failed` would abort a rollout on infra flakiness instead of a real regression.

### DataSource abstraction (`internal/datasource/`)

Each source implements a tiny interface — `Query(ctx, client, cfg) (Result, error)` and `Key(cfg) string` — in its own file (`metrics.go`, `monitor.go`, `slo.go`). They are pure request-builders/response-parsers: no credentials, no caching, no rate-limit logic. **Adding a source = new file + a case in `Select()`; the core doesn't change** (this is how Phase 2 apm/logs will land).

Search-mode results are returned as a `map[string]interface{}` produced by re-marshalling the typed SDK response through JSON (`structToMap`). This is deliberate: `expr` conditions in `successCondition`/`failureCondition` must traverse the **raw Datadog API field names**, preserving compatibility with the `web`-provider queries this plugin replaces. Scalar sources return `float64`.

### Scale core (`internal/datadog/`) — two independent layers

Datadog rate limits are **per-organization**, so every rollout in the cluster shares one budget. Two layers protect it:

- **`ratelimit.go` (`Limiter`)** — a shared `http.RoundTripper` installed into every Datadog client: token-bucket QPS ceiling + adaptation from `X-RateLimit-*` response headers + a concurrency semaphore. Shapes outbound rate so 429s are rare; it does **not** duplicate the SDK's 429/5xx retry.
- **`cache.go` (`Cache`)** — `Run()`-level `singleflight` (collapses identical concurrent queries) + short-TTL cache (default 30s). The cache key includes source query + `site` + `address` + credential identity + a **time-window bucket** (`windowBucket`), so entries roll forward as the query window advances and a value from one window/account can't mask a regression in another. Keying is by account/endpoint, **not** by rollout, so cross-rollout coalescing works.

### Credentials (`internal/datadog/credentials.go`)

Resolved in-process; API/app keys **never enter the AnalysisRun/Rollout CRs**. Resolution order:

1. explicit `secretRef` (`namespaced: true` → AnalysisRun's namespace; else controller namespace),
2. `DD_API_KEY` / `DD_APP_KEY` env vars,
3. a Secret literally named `datadog` in the controller namespace.

Security caveat to preserve: an unrestricted same-namespace `secretRef` means "can author AnalysisTemplates in namespace N" ≈ "can read all Secrets in N" — documented in `docs/configuration.md`.

### Config (`internal/config/config.go`)

Parsed from `metric.Provider.Plugin["mubarak-j/rollouts-plugin-metric-datadog"]`. Exactly one of `metrics` / `monitor` / `slo` may be set; `monitor`/`slo` switch to by-id mode when `id` is present, otherwise search. `Config.Warnings()` surfaces non-fatal issues (e.g. under-scoped searches) to logs.

## Testing conventions

- Data sources are table-tested against `net/http/httptest` stubs — assert request shape and response parsing, not live Datadog.
- `Run` tests are **guarded against ambient `DD_*` env** (they'd otherwise resolve real credentials); keep that guard when adding cases.
- `RpcPlugin` exposes test indirections `selectSource` and `newClient` so tests inject fakes without a real kube client or Datadog endpoint.

## Deeper references

- `docs/design.md` — full design spec (architecture §4, result model §6, tag filtering §7, rate-limit/scale §15, phasing §13).
- `docs/implementations.md` — the task-by-task Phase 1 implementation plan and applied review fixes.
- `docs/configuration.md`, `docs/install.md`, `docs/migration-web-provider.md` — user-facing docs.
