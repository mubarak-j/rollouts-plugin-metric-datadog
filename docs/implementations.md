# Argo Rollouts Datadog Metric Plugin — Phase 1 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a standalone Go binary that implements the Argo Rollouts `MetricProviderPlugin` RPC interface and answers AnalysisTemplate queries from three Datadog sources — `metrics`, `monitor`, `slo` — with shared tag filtering, in-process credential resolution, multi-site support, and a fleet-scale rate-limit/cache core.

**Architecture:** One binary implements the go-plugin RPC interface once. A `Run()` wrapper owns the shared work (config parse, credential resolution, Datadog client construction, evaluate, error→Measurement mapping) and dispatches to a small internal `DataSource` interface with one implementation per source. Rate-limit safety is split into two transparent layers: a shared `http.RoundTripper` (token-bucket limiter + response-header adaptation + concurrency cap, installed into every Datadog client) and a `Run()`-level singleflight + short-TTL cache keyed by the fully-resolved query — so each `DataSource` stays a pure, table-testable request-builder/response-parser.

**Tech Stack:** Go 1.24; `github.com/argoproj/argo-rollouts v1.9.0` (RPC types, `v1alpha1`, `utils/evaluate`, `utils/metric`); `github.com/DataDog/datadog-api-client-go/v2 v2.62.0`; `github.com/hashicorp/go-plugin v1.6.3`; `github.com/sirupsen/logrus v1.9.3`; `k8s.io/client-go v0.34.1`; `golang.org/x/time/rate` and `golang.org/x/sync/singleflight` for the scale core. Tests use `net/http/httptest`.

## Applied Review Fixes (2026-06-30 review)

> These corrections came out of a multi-persona review of this plan. Each amends the task noted — apply it when executing that task. (Three consistency fixes were already applied inline above: the `internal/config/` entry in the File Structure block, the `WindowFrom(now, …)` signature, and `DataSource`/`Select` using `*config.Config`.)

1. **Tasks 11 & 13 — defer last-known-good degradation to Phase 2 (P1; 2026-06-30 review Q2).** Remove §15.3 layer 6 (last-known-good) from Phase 1: drop the last-good branch of `Cache.Do`, the `maxStaleness` wiring, and `TestRun_ThrottleServesLastGoodNeverFailed`, and move layer 6 from the §13 Phase-1 bullet to Phase 2. Phase 1 degrades on sustained throttling via SDK retry (layer 5) + short-TTL cache, then a bounded `Error` (auto-recovering on the next success, capped by `consecutiveErrorLimit`) — never a stale `Successful`, never `Failed`. **When last-good is built in Phase 2 it must return `AnalysisPhaseInconclusive` (pausing the rollout), not a stale `Successful`** — a sustained outage or 5xx during a real regression must not let a canary promote on a pre-regression value during the exact window analysis-gating exists to catch. (Keeping layers 1–5 in Phase 1 protects the monitor-search migration target against undocumented limits + deploy-wave burst alignment at fleet scale; layer 6 is the complex piece whose binding justification — apm/logs survivability — is Phase 2.)

2. **Task 11 / Tasks 6–9 — include the resolved time window in the cache key (P1).** Compute the `TimeWindow` once in `Run()` and thread it into the cache key, e.g. quantize `now` to an `interval`-derived bucket and append it: `key := ds.Key(cfg) + "|site=" + cfg.Site + "|win=" + windowBucket`. Per design §15.3 layer 3 the key is "query + tags + **window** + site"; without the window a value computed for `[now-5m, now]` is reused for TTL+maxStaleness, masking a regression that begins after the cached window while still reporting `fresh`/`cached`.

3. **Task 3 / Task 12 docs — document the `secretRef` same-namespace escalation (P1).** In `docs/configuration.md` (Task 12 Step 7) and the §8.2 RBAC note (Step 6), state explicitly: because `secretRef.name` is unrestricted and `namespaced: true` reads from the AnalysisRun's namespace via the controller ServiceAccount, anyone who can author AnalysisTemplates/AnalysisRuns in a namespace can make the controller read *any* Secret there and forward its `api-key`/`app-key` bytes to Datadog. Operators must treat "can author AnalysisTemplates in namespace N" as equivalent to "can read all Secrets in N," or restrict eligible Secret names by convention/label/admission policy.

4. **Task 4 — enforce HTTPS on address overrides (P1).** Reject any scheme other than `https` in `hostScheme`, gated by a default-false `allowInsecure` option (add `AllowInsecure bool` to `ClientOptions`, thread it into `hostScheme`; the `httptest` test helpers set it true). Add a test asserting a production `http://` address is rejected. Both `address` and the `DD_ADDRESS` fallback feed `hostScheme` and build the client that sends `DD-API-KEY`/`DD-APPLICATION-KEY`, so an `http://` target leaks live keys in cleartext.

5. **Task 4 — bound SDK retry below the plugin timeout (P2).** Set `cfg.RetryConfiguration.HTTPRetryTimeout` to a fraction of the plugin timeout (e.g. `timeout - timeout/5`) rather than the full `timeout`, per design §15.3 layer 5 ("bounded below `timeoutSeconds`"). Because a 429 waits `X-RateLimit-Reset` (up to 60s for `/api/v1/query`), also raise the metrics/monitor default `timeoutSeconds` (or document the minimum) so at least one Reset wait fits before the outer `context` deadline cancels — otherwise the header-aware retry can never fire.

6. **Task 2 / Task 6 — author config in `internal/config` from the start (P2).** Change Task 2's Files to create `internal/config/config.go` + `internal/config/config_test.go` with `package config`, and drop Task 6 Step 1's `git mv` + package-rename step. The import-direction conflict (`datasource` needs `Config`, `plugin` needs `DataSource`) is knowable from the architecture in this plan's opening paragraph, so placing `Config` in a leaf package up front reaches the same end state without a mid-plan move (and removes the non-atomic-build foot-gun). The File Structure block already reflects `internal/config/`.

7. **Tasks 11 / 5 — guard cross-rollout cache collisions via config, not key identity (P1; 2026-06-30 review Q1).** Keep the cache key as query+tags+window+site (fix #2) — do **not** add AnalysisRun/Rollout identity, which would defeat the §15.2/§15.3 cross-rollout coalescing the design depends on ("many rollouts cost one upstream call"). Treat under-scoping as config-correctness instead: document in `docs/configuration.md` (Task 12) that each metric must carry a service-identifying tag so its resolved query uniquely identifies the gated target, and add a `Validate()` warning (Task 2) when a `monitor`/`slo` search source has neither a service-identifying tag nor a per-source `query`. The examples already model the correct `tags: ["service:…","env:…"]` pattern; the hazard only bites a misconfigured, purely `env:`-scoped search.

---

## Global Constraints

These apply to **every** task; each task's requirements implicitly include this section.

- **Module path:** `github.com/mubarak-j/rollouts-plugin-metric-datadog` (verbatim).
- **Plugin name key** (the map key in `metric.Provider.Plugin[...]`, the ConfigMap `name`, and the AnalysisTemplate `provider.plugin.<name>`): `mubarak-j/rollouts-plugin-metric-datadog` (verbatim, used everywhere config JSON is read).
- **go-plugin handshake (must match the controller exactly):** `ProtocolVersion: 1`, `MagicCookieKey: "ARGO_ROLLOUTS_RPC_PLUGIN"`, `MagicCookieValue: "metricprovider"`, dispensed plugin map key `"RpcMetricProviderPlugin"`.
- **Go toolchain:** `go 1.24` in `go.mod` (argo-rollouts v1.9.0 requires `go 1.24.9`; Datadog SDK requires ≥ 1.22).
- **Pinned dependency versions:** `github.com/argoproj/argo-rollouts v1.9.0` (NO `replace` directive — do not copy the sample plugin's stale fork pin), `github.com/DataDog/datadog-api-client-go/v2 v2.62.0`, `github.com/hashicorp/go-plugin v1.6.3`, `github.com/sirupsen/logrus v1.9.3`.
- **`net/rpc` error contract:** `InitPlugin`/`GarbageCollect` return `types.RpcError` (zero value `types.RpcError{}` means success). `Run`/`Resume`/`Terminate` convey errors *inside the Measurement* via `metricutil.MarkMeasurementError(m, err)` (sets `Phase = AnalysisPhaseError`, `Message`). `MarkMeasurementError` returns the measurement **by value** — always capture it (`return metricutil.MarkMeasurementError(m, err)`).
- **No hard-coded pass/fail:** every source resolves to a value; `Run()` calls `evaluate.EvaluateResult(value, metric, logCtx)` so condition semantics are identical to all other Argo providers.
- **Structured sources** (monitor, slo) marshal their typed SDK response to JSON and re-unmarshal into `map[string]interface{}` before evaluation, so existing `web`-provider conditions (`result.counts.status`, …) keep matching.
- **TDD, DRY, YAGNI, frequent commits.** Each `DataSource` is unit-tested against an `httptest.Server` returning canned Datadog JSON (point the SDK at it via `cfg.Host`/`cfg.Scheme`).

---

## File Structure

```
go.mod  go.sum
main.go                                  # Task 1  — handshake + Serve
Makefile  Dockerfile  .github/workflows/ci.yaml   # Task 0 / Task 12
internal/config/
  config.go                              # Task 2  — Config structs + ParseConfig + Validate (package config)
  config_test.go                         # Task 2
internal/plugin/
  plugin.go                              # Task 1/6 — RpcPlugin: the 7 RPC methods + Run() wrapper
internal/datadog/
  credentials.go                         # Task 3  — Credentials struct + Resolver (kube client)
  credentials_test.go                    # Task 3
  client.go                              # Task 4  — SDK client construction, site mapping, auth ctx
  client_test.go                         # Task 4
  ratelimit.go                           # Task 10 — limiting http.RoundTripper (token bucket + headers + semaphore)
  ratelimit_test.go                      # Task 10
  cache.go                               # Task 11 — singleflight + TTL cache + last-known-good
  cache_test.go                          # Task 11
internal/datasource/
  datasource.go                          # Task 6  — DataSource interface + Select() dispatch + TimeWindow + result helpers
  tags.go                                # Task 5  — shared tag application per source
  tags_test.go                           # Task 5
  metrics.go  metrics_test.go            # Task 7
  monitor.go  monitor_test.go            # Task 8
  slo.go      slo_test.go                # Task 9
docs/  examples/                         # Task 12
```

**Responsibility split:** `internal/plugin` owns the RPC surface and config; `internal/datadog` owns everything that talks to Datadog (auth, client, rate-limit, cache, secrets); `internal/datasource` owns the per-source request-build/response-parse logic and tag application. Files that change together live together.

---

### Task 0: Repository scaffolding (module, Makefile, CI skeleton)

**Files:**
- Create: `go.mod`
- Create: `Makefile`
- Create: `.github/workflows/ci.yaml`
- Create: `internal/plugin/doc.go` (placeholder so the package compiles)

**Interfaces:**
- Consumes: nothing.
- Produces: a buildable, testable empty module on Go 1.24 with the pinned deps available.

- [ ] **Step 1: Create `go.mod` with pinned dependencies**

```
module github.com/mubarak-j/rollouts-plugin-metric-datadog

go 1.24

require (
	github.com/DataDog/datadog-api-client-go/v2 v2.62.0
	github.com/argoproj/argo-rollouts v1.9.0
	github.com/hashicorp/go-plugin v1.6.3
	github.com/sirupsen/logrus v1.9.3
	github.com/stretchr/testify v1.9.0
	golang.org/x/sync v0.8.0
	golang.org/x/time v0.6.0
	k8s.io/apimachinery v0.34.1
	k8s.io/client-go v0.34.1
)
```

- [ ] **Step 2: Add a placeholder package file so the module compiles**

Create `internal/plugin/doc.go`:

```go
// Package plugin implements the Argo Rollouts metric provider RPC plugin
// for Datadog.
package plugin
```

- [ ] **Step 3: Create `Makefile`**

```makefile
BINARY := rollouts-plugin-metric-datadog
PLUGIN_PATH := mubarak-j/rollouts-plugin-metric-datadog

.PHONY: build test vet tidy
tidy:
	go mod tidy

build:
	CGO_ENABLED=0 go build -o dist/$(BINARY) .

test:
	go test ./... -race -count=1

vet:
	go vet ./...
```

- [ ] **Step 4: Create `.github/workflows/ci.yaml`**

```yaml
name: ci
on: [push, pull_request]
jobs:
  build-test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-go@v5
        with:
          go-version: "1.24"
      - run: go mod download
      - run: go vet ./...
      - run: go test ./... -race -count=1
```

- [ ] **Step 5: Resolve the module graph and verify it builds**

Run: `go mod tidy && go build ./... && go test ./...`
Expected: `go mod tidy` populates `go.sum`; build succeeds; `go test ./...` prints `no test files` for `internal/plugin` and exits 0.

- [ ] **Step 6: Commit**

```bash
git add go.mod go.sum Makefile .github/workflows/ci.yaml internal/plugin/doc.go
git commit -m "chore: scaffold Go module, Makefile, and CI"
```

---

### Task 1: go-plugin handshake + RPC skeleton

Stands up the binary the controller can launch and the seven RPC methods. `Run` is a stub here (returns an Error measurement saying "not implemented"); Task 6 fills it in. This task's gate is "the binary builds and the handshake/no-op methods are correct."

**Files:**
- Create: `main.go`
- Modify: `internal/plugin/plugin.go` (replace the placeholder responsibilities of `doc.go`; keep `doc.go` for the package comment)
- Create: `internal/plugin/plugin_test.go`

**Interfaces:**
- Consumes: argo-rollouts `metricproviders/plugin/rpc`, `utils/plugin/types`, `pkg/apis/rollouts/v1alpha1`, `metricproviders/plugin` (`ProviderType`), `utils/time` (`MetaNow`).
- Produces:
  - `type RpcPlugin struct { LogCtx log.Entry }` implementing `rpc.MetricProviderPlugin`.
  - `func (g *RpcPlugin) Type() string` → `plugin.ProviderType` (`"RPCPlugin"`).
  - `func (g *RpcPlugin) InitPlugin() types.RpcError`, `Run`, `Resume`, `Terminate`, `GarbageCollect`, `GetMetadata` with the exact signatures below (later tasks add fields to `RpcPlugin` and flesh out `Run`/`InitPlugin`/`GetMetadata`).

- [ ] **Step 1: Write the failing test for `Type()` and the no-op lifecycle methods**

Create `internal/plugin/plugin_test.go`:

```go
package plugin

import (
	"testing"

	"github.com/argoproj/argo-rollouts/pkg/apis/rollouts/v1alpha1"
	"github.com/stretchr/testify/assert"
)

func TestType(t *testing.T) {
	g := &RpcPlugin{}
	assert.Equal(t, "RPCPlugin", g.Type())
}

func TestResumeTerminateReturnMeasurementUnchanged(t *testing.T) {
	g := &RpcPlugin{}
	m := v1alpha1.Measurement{Phase: v1alpha1.AnalysisPhaseRunning, Value: "x"}
	assert.Equal(t, m, g.Resume(nil, v1alpha1.Metric{}, m))
	assert.Equal(t, m, g.Terminate(nil, v1alpha1.Metric{}, m))
}

func TestGarbageCollectAndInitReturnNoError(t *testing.T) {
	g := &RpcPlugin{}
	assert.False(t, g.GarbageCollect(nil, v1alpha1.Metric{}, 0).HasError())
}
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `go test ./internal/plugin/ -run 'TestType|TestResume|TestGarbage' -v`
Expected: FAIL — `RpcPlugin` / its methods are undefined.

- [ ] **Step 3: Implement the `RpcPlugin` skeleton**

Create `internal/plugin/plugin.go`:

```go
package plugin

import (
	"github.com/argoproj/argo-rollouts/metricproviders/plugin"
	"github.com/argoproj/argo-rollouts/pkg/apis/rollouts/v1alpha1"
	"github.com/argoproj/argo-rollouts/utils/metric"
	timeutil "github.com/argoproj/argo-rollouts/utils/time"
	"github.com/argoproj/argo-rollouts/utils/plugin/types"
	log "github.com/sirupsen/logrus"
)

// RpcPlugin implements rpc.MetricProviderPlugin for Datadog.
type RpcPlugin struct {
	LogCtx log.Entry
}

// InitPlugin is called once per plugin process. Later tasks build the kube
// client and shared rate-limit/cache state here.
func (g *RpcPlugin) InitPlugin() types.RpcError {
	return types.RpcError{}
}

// Run is filled in by Task 6.
func (g *RpcPlugin) Run(analysisRun *v1alpha1.AnalysisRun, m v1alpha1.Metric) v1alpha1.Measurement {
	startTime := timeutil.MetaNow()
	measurement := v1alpha1.Measurement{StartedAt: &startTime}
	return metric.MarkMeasurementError(measurement, errNotImplemented)
}

func (g *RpcPlugin) Resume(_ *v1alpha1.AnalysisRun, _ v1alpha1.Metric, measurement v1alpha1.Measurement) v1alpha1.Measurement {
	return measurement
}

func (g *RpcPlugin) Terminate(_ *v1alpha1.AnalysisRun, _ v1alpha1.Metric, measurement v1alpha1.Measurement) v1alpha1.Measurement {
	return measurement
}

func (g *RpcPlugin) GarbageCollect(_ *v1alpha1.AnalysisRun, _ v1alpha1.Metric, _ int) types.RpcError {
	return types.RpcError{}
}

func (g *RpcPlugin) Type() string {
	return plugin.ProviderType
}

// GetMetadata is filled in by Task 6.
func (g *RpcPlugin) GetMetadata(_ v1alpha1.Metric) map[string]string {
	return map[string]string{}
}
```

Add to the same file (a package-level sentinel used by the stub):

```go
import "errors"

var errNotImplemented = errors.New("datadog plugin: Run not implemented yet")
```

(Merge the `errors` import into the import block.)

- [ ] **Step 4: Run the test to verify it passes**

Run: `go test ./internal/plugin/ -run 'TestType|TestResume|TestGarbage' -v`
Expected: PASS.

- [ ] **Step 5: Add a compile-time interface assertion test**

Append to `internal/plugin/plugin_test.go`:

```go
import rolloutsRpc "github.com/argoproj/argo-rollouts/metricproviders/plugin/rpc"

func TestImplementsInterface(t *testing.T) {
	var _ rolloutsRpc.MetricProviderPlugin = (*RpcPlugin)(nil)
}
```

Run: `go test ./internal/plugin/ -run TestImplementsInterface -v`
Expected: PASS (this fails to compile if any method signature drifts).

- [ ] **Step 6: Write `main.go` (handshake + Serve)**

Create `main.go`:

```go
package main

import (
	"github.com/mubarak-j/rollouts-plugin-metric-datadog/internal/plugin"

	rolloutsPlugin "github.com/argoproj/argo-rollouts/metricproviders/plugin/rpc"
	goPlugin "github.com/hashicorp/go-plugin"
	log "github.com/sirupsen/logrus"
)

var handshakeConfig = goPlugin.HandshakeConfig{
	ProtocolVersion:  1,
	MagicCookieKey:   "ARGO_ROLLOUTS_RPC_PLUGIN",
	MagicCookieValue: "metricprovider",
}

func main() {
	logCtx := *log.WithFields(log.Fields{"plugin": "datadog"})

	impl := &plugin.RpcPlugin{LogCtx: logCtx}
	pluginMap := map[string]goPlugin.Plugin{
		"RpcMetricProviderPlugin": &rolloutsPlugin.RpcMetricProviderPlugin{Impl: impl},
	}

	goPlugin.Serve(&goPlugin.ServeConfig{
		HandshakeConfig: handshakeConfig,
		Plugins:         pluginMap,
	})
}
```

- [ ] **Step 7: Build the binary**

Run: `make build`
Expected: `dist/rollouts-plugin-metric-datadog` is produced; no errors.

- [ ] **Step 8: Commit**

```bash
git add main.go internal/plugin/plugin.go internal/plugin/plugin_test.go
git commit -m "feat: go-plugin handshake and RPC method skeleton"
```

---

### Task 2: Config structs + parsing + validation

Defines the full Phase 1 config schema (§5) and all validation rules (§5.1). No Datadog calls. The metrics/monitor/slo sub-structs are defined now (the sources in Tasks 7–9 consume them); apm/logs are intentionally omitted (Phase 2).

**Files:**
- Create: `internal/plugin/config.go`
- Create: `internal/plugin/config_test.go`

**Interfaces:**
- Consumes: argo-rollouts `v1alpha1.Metric`.
- Produces:
  - `type Config struct { ... }` (fields below) with `MetricsConfig`, `MonitorConfig`, `SLOConfig`, `SecretRef`, `RateLimitConfig`, `CacheConfig`, `RetryConfig` sub-structs.
  - `func ParseConfig(metric v1alpha1.Metric) (*Config, error)` — unmarshals `metric.Provider.Plugin["mubarak-j/rollouts-plugin-metric-datadog"]` and calls `Validate`.
  - `func (c *Config) Validate() error`.
  - `func (c *Config) Source() string` → one of `"metrics"|"monitor"|"slo"` (the present block).
  - `func (c *Config) Timeout() time.Duration` → `TimeoutSeconds` or 30s default.

- [ ] **Step 1: Write failing tests for parsing + validation rules**

Create `internal/plugin/config_test.go`:

```go
package plugin

import (
	"encoding/json"
	"testing"

	"github.com/argoproj/argo-rollouts/pkg/apis/rollouts/v1alpha1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const pluginName = "mubarak-j/rollouts-plugin-metric-datadog"

func metricWith(t *testing.T, raw string) v1alpha1.Metric {
	t.Helper()
	return v1alpha1.Metric{
		Provider: v1alpha1.MetricProvider{
			Plugin: map[string]json.RawMessage{pluginName: json.RawMessage(raw)},
		},
	}
}

func TestParseConfig_Metrics(t *testing.T) {
	c, err := ParseConfig(metricWith(t, `{"metrics":{"query":"avg:cpu{*}"}}`))
	require.NoError(t, err)
	assert.Equal(t, "metrics", c.Source())
	assert.Equal(t, "v2", c.Metrics.APIVersion) // default
}

func TestValidate_ExactlyOneSource(t *testing.T) {
	_, err := ParseConfig(metricWith(t, `{}`))
	assert.ErrorContains(t, err, "exactly one")

	_, err = ParseConfig(metricWith(t, `{"monitor":{},"slo":{}}`))
	assert.ErrorContains(t, err, "exactly one")
}

func TestValidate_MetricsFieldRules(t *testing.T) {
	// query and queries are mutually exclusive
	_, err := ParseConfig(metricWith(t, `{"metrics":{"query":"a","queries":{"a":"x"}}}`))
	assert.ErrorContains(t, err, "query")

	// >1 query requires a formula
	_, err = ParseConfig(metricWith(t, `{"metrics":{"queries":{"a":"x","b":"y"}}}`))
	assert.ErrorContains(t, err, "formula")

	// formula requires queries
	_, err = ParseConfig(metricWith(t, `{"metrics":{"query":"x","formula":"a"}}`))
	assert.ErrorContains(t, err, "queries")

	// apiVersion must be v1 or v2
	_, err = ParseConfig(metricWith(t, `{"metrics":{"query":"x","apiVersion":"v3"}}`))
	assert.ErrorContains(t, err, "apiVersion")

	// aggregator is v2-only
	_, err = ParseConfig(metricWith(t, `{"metrics":{"query":"x","apiVersion":"v1","aggregator":"last"}}`))
	assert.ErrorContains(t, err, "aggregator")
}

func TestValidate_TagsWithByIDRejected(t *testing.T) {
	_, err := ParseConfig(metricWith(t, `{"tags":["service:x"],"monitor":{"id":123}}`))
	assert.ErrorContains(t, err, "tags")

	_, err = ParseConfig(metricWith(t, `{"tags":["service:x"],"slo":{"id":"abc"}}`))
	assert.ErrorContains(t, err, "tags")
}

func TestValidate_NamespacedSecretRequiresName(t *testing.T) {
	_, err := ParseConfig(metricWith(t, `{"monitor":{},"secretRef":{"namespaced":true}}`))
	assert.ErrorContains(t, err, "name")
}

func TestSourceAndTimeoutDefaults(t *testing.T) {
	c, err := ParseConfig(metricWith(t, `{"monitor":{}}`))
	require.NoError(t, err)
	assert.Equal(t, "monitor", c.Source())
	assert.Equal(t, float64(30), c.Timeout().Seconds())
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `go test ./internal/plugin/ -run 'Config|Validate|Source' -v`
Expected: FAIL — `ParseConfig`, `Config`, etc. undefined.

- [ ] **Step 3: Implement `config.go`**

Create `internal/plugin/config.go`:

```go
package plugin

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/argoproj/argo-rollouts/pkg/apis/rollouts/v1alpha1"
)

// ConfigKey is the plugin name used as the map key in metric.Provider.Plugin
// and as provider.plugin.<name> in AnalysisTemplates.
const ConfigKey = "mubarak-j/rollouts-plugin-metric-datadog"

type Config struct {
	Site           string           `json:"site,omitempty"`
	Address        string           `json:"address,omitempty"`
	SecretRef      *SecretRef       `json:"secretRef,omitempty"`
	TimeoutSeconds int              `json:"timeoutSeconds,omitempty"`
	Tags           []string         `json:"tags,omitempty"`
	RateLimit      *RateLimitConfig `json:"rateLimit,omitempty"`
	Cache          *CacheConfig     `json:"cache,omitempty"`
	Retry          *RetryConfig     `json:"retry,omitempty"`

	Metrics *MetricsConfig `json:"metrics,omitempty"`
	Monitor *MonitorConfig `json:"monitor,omitempty"`
	SLO     *SLOConfig     `json:"slo,omitempty"`
}

type SecretRef struct {
	Name       string `json:"name,omitempty"`
	Namespaced bool   `json:"namespaced,omitempty"`
}

type MetricsConfig struct {
	APIVersion string            `json:"apiVersion,omitempty"` // v1|v2, default v2
	Query      string            `json:"query,omitempty"`
	Queries    map[string]string `json:"queries,omitempty"`
	Formula    string            `json:"formula,omitempty"`
	Aggregator string            `json:"aggregator,omitempty"` // v2 only
	Interval   string            `json:"interval,omitempty"`   // default 5m
}

type MonitorConfig struct {
	Mode  string `json:"mode,omitempty"` // search (default) | (id set => by-id)
	Query string `json:"query,omitempty"`
	ID    *int64 `json:"id,omitempty"`
}

type SLOConfig struct {
	Mode     string  `json:"mode,omitempty"` // search (default) | (id set => by-id)
	Query    string  `json:"query,omitempty"`
	ID       *string `json:"id,omitempty"`
	Interval string  `json:"interval,omitempty"` // by-id history window, default 7d
}

type RateLimitConfig struct {
	Enabled        *bool                    `json:"enabled,omitempty"` // default true
	BudgetFraction float64                  `json:"budgetFraction,omitempty"`
	Buckets        map[string]BucketCeiling `json:"buckets,omitempty"`
	MaxConcurrent  int                      `json:"maxConcurrent,omitempty"`
}

type BucketCeiling struct {
	RPS float64 `json:"rps,omitempty"`
}

type CacheConfig struct {
	Enabled      *bool  `json:"enabled,omitempty"` // default true
	TTL          string `json:"ttl,omitempty"`
	MaxStaleness string `json:"maxStaleness,omitempty"`
}

type RetryConfig struct {
	MaxRetries int `json:"maxRetries,omitempty"` // default 3
}

func ParseConfig(metric v1alpha1.Metric) (*Config, error) {
	raw, ok := metric.Provider.Plugin[ConfigKey]
	if !ok || len(raw) == 0 {
		return nil, fmt.Errorf("no plugin config found under %q", ConfigKey)
	}
	c := &Config{}
	if err := json.Unmarshal(raw, c); err != nil {
		return nil, fmt.Errorf("invalid plugin config JSON: %w", err)
	}
	c.applyDefaults()
	if err := c.Validate(); err != nil {
		return nil, err
	}
	return c, nil
}

func (c *Config) applyDefaults() {
	if c.Metrics != nil && c.Metrics.APIVersion == "" {
		c.Metrics.APIVersion = "v2"
	}
}

func (c *Config) Source() string {
	switch {
	case c.Metrics != nil:
		return "metrics"
	case c.Monitor != nil:
		return "monitor"
	case c.SLO != nil:
		return "slo"
	default:
		return ""
	}
}

func (c *Config) Timeout() time.Duration {
	if c.TimeoutSeconds > 0 {
		return time.Duration(c.TimeoutSeconds) * time.Second
	}
	return 30 * time.Second
}

func (c *Config) Validate() error {
	n := 0
	for _, present := range []bool{c.Metrics != nil, c.Monitor != nil, c.SLO != nil} {
		if present {
			n++
		}
	}
	if n != 1 {
		return fmt.Errorf("exactly one of metrics/monitor/slo must be set (found %d)", n)
	}

	if c.Site != "" && c.Address != "" {
		// address wins; not an error, but note it. (No-op here: documented in §5.1.)
	}

	if c.SecretRef != nil && c.SecretRef.Namespaced && c.SecretRef.Name == "" {
		return fmt.Errorf("secretRef.namespaced=true requires a non-empty name")
	}

	byID := (c.Monitor != nil && c.Monitor.ID != nil) || (c.SLO != nil && c.SLO.ID != nil)
	if byID && len(c.Tags) > 0 {
		return fmt.Errorf("tags cannot be combined with a by-id mode (monitor.id/slo.id); use search mode for tag filtering")
	}

	if c.Metrics != nil {
		return c.validateMetrics()
	}
	return nil
}

func (c *Config) validateMetrics() error {
	m := c.Metrics
	if m.APIVersion != "v1" && m.APIVersion != "v2" {
		return fmt.Errorf("metrics.apiVersion must be v1 or v2, got %q", m.APIVersion)
	}
	hasQuery := m.Query != ""
	hasQueries := len(m.Queries) > 0
	if hasQuery == hasQueries {
		return fmt.Errorf("metrics requires exactly one of query or queries")
	}
	if m.Formula != "" && !hasQueries {
		return fmt.Errorf("metrics.formula requires queries")
	}
	if len(m.Queries) > 1 && m.Formula == "" {
		return fmt.Errorf("metrics with more than one query requires a formula")
	}
	if m.Aggregator != "" && m.APIVersion != "v2" {
		return fmt.Errorf("metrics.aggregator is valid only for apiVersion v2")
	}
	return nil
}
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `go test ./internal/plugin/ -run 'Config|Validate|Source' -v`
Expected: PASS (all cases).

- [ ] **Step 5: Commit**

```bash
git add internal/plugin/config.go internal/plugin/config_test.go
git commit -m "feat: plugin config schema, parsing, and validation"
```

---

### Task 3: Credential resolver + kube client

Implements the 3-step resolution order (§8.1): per-metric `secretRef` → env vars → a secret literally named `datadog` in the controller namespace. The kube client is an interface so tests use a fake.

**Files:**
- Create: `internal/datadog/credentials.go`
- Create: `internal/datadog/credentials_test.go`

**Interfaces:**
- Consumes: `internal/plugin.Config`/`SecretRef` (import as `pluginconfig`), `v1alpha1.AnalysisRun` (for namespace), `k8s.io/client-go`.
- Produces:
  - `type Credentials struct { APIKey, AppKey, Address string }`
  - `type SecretGetter interface { GetSecret(ctx context.Context, namespace, name string) (map[string][]byte, error) }`
  - `type Resolver struct { Secrets SecretGetter; ControllerNamespace string }`
  - `func (r *Resolver) Resolve(ctx context.Context, run *v1alpha1.AnalysisRun, secretRef *pluginconfig.SecretRef) (Credentials, error)` — keys read from secret data: `api-key`, `app-key`, optional `address`.
  - `func NewKubeSecretGetter() (SecretGetter, string, error)` — in-cluster client + the controller namespace (read from the service-account namespace file).

To avoid an import cycle (`internal/datadog` importing `internal/plugin`), `Resolve` takes the `*pluginconfig.SecretRef` directly. `internal/plugin` may import `internal/datadog`, not vice-versa beyond this leaf struct — so define `SecretRef` resolution inputs as primitive params instead. **Revised signature (no plugin import):**
`func (r *Resolver) Resolve(ctx context.Context, runNamespace string, ref *SecretRefInput) (Credentials, error)` where `SecretRefInput struct { Name string; Namespaced bool }` lives in `internal/datadog`. `internal/plugin` maps its `*SecretRef` to `*datadog.SecretRefInput` at the call site.

- [ ] **Step 1: Write failing tests with a fake SecretGetter**

Create `internal/datadog/credentials_test.go`:

```go
package datadog

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type fakeSecrets struct {
	data map[string]map[string][]byte // namespace/name -> data
}

func (f *fakeSecrets) GetSecret(_ context.Context, ns, name string) (map[string][]byte, error) {
	if d, ok := f.data[ns+"/"+name]; ok {
		return d, nil
	}
	return nil, assertNotFound{}
}

type assertNotFound struct{}

func (assertNotFound) Error() string { return "not found" }

func TestResolve_SecretRefControllerNamespace(t *testing.T) {
	r := &Resolver{
		ControllerNamespace: "argo-rollouts",
		Secrets: &fakeSecrets{data: map[string]map[string][]byte{
			"argo-rollouts/my-dd": {"api-key": []byte("AK"), "app-key": []byte("PK")},
		}},
	}
	creds, err := r.Resolve(context.Background(), "app-ns", &SecretRefInput{Name: "my-dd"})
	require.NoError(t, err)
	assert.Equal(t, "AK", creds.APIKey)
	assert.Equal(t, "PK", creds.AppKey)
}

func TestResolve_SecretRefNamespaced(t *testing.T) {
	r := &Resolver{
		ControllerNamespace: "argo-rollouts",
		Secrets: &fakeSecrets{data: map[string]map[string][]byte{
			"app-ns/my-dd": {"api-key": []byte("AK"), "app-key": []byte("PK")},
		}},
	}
	creds, err := r.Resolve(context.Background(), "app-ns", &SecretRefInput{Name: "my-dd", Namespaced: true})
	require.NoError(t, err)
	assert.Equal(t, "AK", creds.APIKey)
}

func TestResolve_EnvVars(t *testing.T) {
	t.Setenv("DD_API_KEY", "envAK")
	t.Setenv("DD_APP_KEY", "envPK")
	r := &Resolver{ControllerNamespace: "argo-rollouts", Secrets: &fakeSecrets{}}
	creds, err := r.Resolve(context.Background(), "app-ns", nil)
	require.NoError(t, err)
	assert.Equal(t, "envAK", creds.APIKey)
	assert.Equal(t, "envPK", creds.AppKey)
}

func TestResolve_FallbackDatadogSecret(t *testing.T) {
	r := &Resolver{
		ControllerNamespace: "argo-rollouts",
		Secrets: &fakeSecrets{data: map[string]map[string][]byte{
			"argo-rollouts/datadog": {"api-key": []byte("dAK"), "app-key": []byte("dPK")},
		}},
	}
	creds, err := r.Resolve(context.Background(), "app-ns", nil)
	require.NoError(t, err)
	assert.Equal(t, "dAK", creds.APIKey)
	assert.Equal(t, "dPK", creds.AppKey)
}

func TestResolve_MissingKeysError(t *testing.T) {
	r := &Resolver{ControllerNamespace: "argo-rollouts", Secrets: &fakeSecrets{}}
	_, err := r.Resolve(context.Background(), "app-ns", nil)
	assert.Error(t, err)
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `go test ./internal/datadog/ -run TestResolve -v`
Expected: FAIL — `Resolver`, `Credentials`, `SecretRefInput` undefined.

- [ ] **Step 3: Implement `credentials.go`**

Create `internal/datadog/credentials.go`:

```go
package datadog

import (
	"context"
	"fmt"
	"os"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

type Credentials struct {
	APIKey  string
	AppKey  string
	Address string
}

type SecretRefInput struct {
	Name       string
	Namespaced bool
}

// SecretGetter abstracts reading a Secret's data, so tests can fake it.
type SecretGetter interface {
	GetSecret(ctx context.Context, namespace, name string) (map[string][]byte, error)
}

type Resolver struct {
	Secrets             SecretGetter
	ControllerNamespace string
}

func (r *Resolver) Resolve(ctx context.Context, runNamespace string, ref *SecretRefInput) (Credentials, error) {
	// 1. explicit secretRef
	if ref != nil && ref.Name != "" {
		ns := r.ControllerNamespace
		if ref.Namespaced {
			ns = runNamespace
		}
		return r.fromSecret(ctx, ns, ref.Name)
	}

	// 2. environment variables
	if ak := os.Getenv("DD_API_KEY"); ak != "" {
		creds := Credentials{APIKey: ak, AppKey: os.Getenv("DD_APP_KEY"), Address: os.Getenv("DD_ADDRESS")}
		if creds.AppKey == "" {
			return Credentials{}, fmt.Errorf("DD_API_KEY set but DD_APP_KEY missing")
		}
		return creds, nil
	}

	// 3. secret literally named "datadog" in the controller namespace
	return r.fromSecret(ctx, r.ControllerNamespace, "datadog")
}

func (r *Resolver) fromSecret(ctx context.Context, ns, name string) (Credentials, error) {
	data, err := r.Secrets.GetSecret(ctx, ns, name)
	if err != nil {
		return Credentials{}, fmt.Errorf("reading secret %s/%s: %w", ns, name, err)
	}
	ak := string(data["api-key"])
	pk := string(data["app-key"])
	if ak == "" || pk == "" {
		return Credentials{}, fmt.Errorf("secret %s/%s missing api-key/app-key", ns, name)
	}
	return Credentials{APIKey: ak, AppKey: pk, Address: string(data["address"])}, nil
}

// --- real kube client ---

type kubeSecretGetter struct {
	client kubernetes.Interface
}

func (k *kubeSecretGetter) GetSecret(ctx context.Context, ns, name string) (map[string][]byte, error) {
	s, err := k.client.CoreV1().Secrets(ns).Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		return nil, err
	}
	return s.Data, nil
}

const saNamespaceFile = "/var/run/secrets/kubernetes.io/serviceaccount/namespace"

func NewKubeSecretGetter() (SecretGetter, string, error) {
	cfg, err := rest.InClusterConfig()
	if err != nil {
		return nil, "", fmt.Errorf("in-cluster config: %w", err)
	}
	cs, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		return nil, "", fmt.Errorf("kube client: %w", err)
	}
	ns := "argo-rollouts"
	if b, err := os.ReadFile(saNamespaceFile); err == nil && len(b) > 0 {
		ns = string(b)
	}
	return &kubeSecretGetter{client: cs}, ns, nil
}
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `go test ./internal/datadog/ -run TestResolve -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/datadog/credentials.go internal/datadog/credentials_test.go
git commit -m "feat: Datadog credential resolver with kube secret getter"
```

---

### Task 4: Datadog SDK client construction + site mapping

Builds an authenticated `*datadog.APIClient` and an auth/site `context.Context`, with the `cfg.Host`/`cfg.Scheme` test-server hook and the retry config. Accepts an optional `http.RoundTripper` so Task 10 can inject the rate-limiting transport without changing call sites.

**Files:**
- Create: `internal/datadog/client.go`
- Create: `internal/datadog/client_test.go`

**Interfaces:**
- Consumes: `Credentials` (Task 3); Datadog SDK `api/datadog`.
- Produces:
  - `type ClientOptions struct { Site, Address string; Timeout time.Duration; MaxRetries int; Transport http.RoundTripper }`
  - `func NewClient(creds Credentials, opts ClientOptions) (*datadog.APIClient, error)`
  - `func AuthContext(ctx context.Context, creds Credentials, site string) context.Context` — installs `ContextAPIKeys` + (when no full address) `ContextServerVariables{"site"}`.
  - A package-level `func hostScheme(address string) (host, scheme string, err error)` used both for `address` override and tests.

- [ ] **Step 1: Write failing tests (auth headers + site + test-server routing)**

Create `internal/datadog/client_test.go`:

```go
package datadog

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	datadogV1 "github.com/DataDog/datadog-api-client-go/v2/api/datadogV1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewClient_RoutesToTestServerWithAuthHeaders(t *testing.T) {
	var gotAPIKey, gotAppKey string
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotAPIKey = r.Header.Get("DD-API-KEY")
		gotAppKey = r.Header.Get("DD-APPLICATION-KEY")
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"groups":[],"counts":{}}`))
	}))
	defer ts.Close()

	creds := Credentials{APIKey: "AK", AppKey: "PK", Address: ts.URL}
	client, err := NewClient(creds, ClientOptions{Address: ts.URL})
	require.NoError(t, err)

	ctx := AuthContext(context.Background(), creds, "")
	api := datadogV1.NewMonitorsApi(client)
	_, _, err = api.SearchMonitorGroups(ctx)
	require.NoError(t, err)
	assert.Equal(t, "AK", gotAPIKey)
	assert.Equal(t, "PK", gotAppKey)
}

func TestHostScheme(t *testing.T) {
	h, s, err := hostScheme("https://api.datadoghq.eu")
	require.NoError(t, err)
	assert.Equal(t, "api.datadoghq.eu", h)
	assert.Equal(t, "https", s)
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `go test ./internal/datadog/ -run 'NewClient|HostScheme' -v`
Expected: FAIL — `NewClient`, `AuthContext`, `hostScheme` undefined.

- [ ] **Step 3: Implement `client.go`**

Create `internal/datadog/client.go`:

```go
package datadog

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"time"

	"github.com/DataDog/datadog-api-client-go/v2/api/datadog"
)

type ClientOptions struct {
	Site       string
	Address    string // full URL override; wins over Site when set
	Timeout    time.Duration
	MaxRetries int
	Transport  http.RoundTripper // optional; Task 10 injects the limiter
}

func NewClient(creds Credentials, opts ClientOptions) (*datadog.APIClient, error) {
	cfg := datadog.NewConfiguration()

	// retry: enable so 429/5xx honor X-Ratelimit-Reset.
	cfg.RetryConfiguration.EnableRetry = true
	if opts.MaxRetries > 0 {
		cfg.RetryConfiguration.MaxRetries = opts.MaxRetries
	}
	timeout := opts.Timeout
	if timeout <= 0 {
		timeout = 30 * time.Second
	}
	cfg.RetryConfiguration.HTTPRetryTimeout = timeout
	cfg.HTTPClient = &http.Client{Timeout: timeout, Transport: opts.Transport}

	// address (full URL) wins over site; creds.Address (from secret/env) also counts.
	addr := opts.Address
	if addr == "" {
		addr = creds.Address
	}
	if addr != "" {
		host, scheme, err := hostScheme(addr)
		if err != nil {
			return nil, err
		}
		cfg.Host = host
		cfg.Scheme = scheme
	}
	return datadog.NewAPIClient(cfg), nil
}

func AuthContext(ctx context.Context, creds Credentials, site string) context.Context {
	ctx = context.WithValue(ctx, datadog.ContextAPIKeys, map[string]datadog.APIKey{
		"apiKeyAuth": {Key: creds.APIKey},
		"appKeyAuth": {Key: creds.AppKey},
	})
	if site != "" {
		ctx = context.WithValue(ctx, datadog.ContextServerVariables, map[string]string{"site": site})
	}
	return ctx
}

func hostScheme(address string) (string, string, error) {
	u, err := url.Parse(address)
	if err != nil {
		return "", "", fmt.Errorf("invalid address %q: %w", address, err)
	}
	if u.Host == "" || u.Scheme == "" {
		return "", "", fmt.Errorf("address %q must be a full URL with scheme and host", address)
	}
	return u.Host, u.Scheme, nil
}
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `go test ./internal/datadog/ -run 'NewClient|HostScheme' -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/datadog/client.go internal/datadog/client_test.go
git commit -m "feat: Datadog SDK client construction, auth, and site mapping"
```

---

### Task 5: Shared tag application

Implements §7. Pure string functions, no Datadog calls. The metric-scope brace merge (§7.2) is the tricky one; it gets the most test coverage.

**Files:**
- Create: `internal/datasource/tags.go`
- Create: `internal/datasource/tags_test.go`

**Interfaces:**
- Consumes: nothing.
- Produces:
  - `func JoinSearchQuery(tags []string, extra string) string` — for monitor/slo search (§7.3): space-joined tags + extra.
  - `func AppendQueryFilter(query string, tags []string) string` — for apm/logs (§7.1; defined now, used in Phase 2): `<query> <tags...>`.
  - `func MergeScopeTags(query string, tags []string) string` — for metrics (§7.2): merge into the first `{…}` scope brace, preserving `by {…}` and function suffixes.

- [ ] **Step 1: Write failing tests, esp. for `MergeScopeTags`**

Create `internal/datasource/tags_test.go`:

```go
package datasource

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestJoinSearchQuery(t *testing.T) {
	assert.Equal(t, "service:x env:prod muted:false",
		JoinSearchQuery([]string{"service:x", "env:prod"}, "muted:false"))
	assert.Equal(t, "service:x", JoinSearchQuery([]string{"service:x"}, ""))
	assert.Equal(t, "muted:false", JoinSearchQuery(nil, "muted:false"))
}

func TestMergeScopeTags(t *testing.T) {
	tags := []string{"service:x", "env:prod"}
	cases := []struct{ in, want string }{
		{"avg:cpu{*}", "avg:cpu{service:x,env:prod}"},
		{"avg:cpu{}", "avg:cpu{service:x,env:prod}"},
		{"avg:cpu{team:a}", "avg:cpu{team:a,service:x,env:prod}"},
		{"avg:cpu{*} by {host}", "avg:cpu{service:x,env:prod} by {host}"},
		{"sum:hits{team:a}.as_count()", "sum:hits{team:a,service:x,env:prod}.as_count()"},
		{"avg:cpu", "avg:cpu{service:x,env:prod}"}, // no brace: appended
	}
	for _, c := range cases {
		assert.Equal(t, c.want, MergeScopeTags(c.in, tags), c.in)
	}
	// no tags: query unchanged
	assert.Equal(t, "avg:cpu{*}", MergeScopeTags("avg:cpu{*}", nil))
}

func TestAppendQueryFilter(t *testing.T) {
	assert.Equal(t, "status:error service:x", AppendQueryFilter("status:error", []string{"service:x"}))
	assert.Equal(t, "service:x", AppendQueryFilter("", []string{"service:x"}))
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `go test ./internal/datasource/ -run 'Tags|MergeScope|JoinSearch|AppendQuery' -v`
Expected: FAIL — functions undefined.

- [ ] **Step 3: Implement `tags.go`**

Create `internal/datasource/tags.go`:

```go
package datasource

import "strings"

// JoinSearchQuery space-joins tags with an optional extra query (monitor/slo).
func JoinSearchQuery(tags []string, extra string) string {
	parts := make([]string, 0, len(tags)+1)
	parts = append(parts, tags...)
	if strings.TrimSpace(extra) != "" {
		parts = append(parts, strings.TrimSpace(extra))
	}
	return strings.Join(parts, " ")
}

// AppendQueryFilter ANDs tags onto a query string (apm/logs).
func AppendQueryFilter(query string, tags []string) string {
	return JoinSearchQuery(tags, query)
}

// MergeScopeTags merges tags into the first {…} scope brace of a metric query,
// preserving any trailing `by {…}` grouping and function suffix. A query with no
// scope brace gets one appended after the metric name.
func MergeScopeTags(query string, tags []string) string {
	if len(tags) == 0 {
		return query
	}
	joined := strings.Join(tags, ",")

	open := strings.IndexByte(query, '{')
	if open < 0 {
		return query + "{" + joined + "}"
	}
	close := strings.IndexByte(query[open:], '}')
	if close < 0 {
		// malformed; leave as-is rather than corrupt the query.
		return query
	}
	close += open

	inner := strings.TrimSpace(query[open+1 : close])
	var merged string
	if inner == "" || inner == "*" {
		merged = joined
	} else {
		merged = inner + "," + joined
	}
	return query[:open+1] + merged + query[close:]
}
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `go test ./internal/datasource/ -run 'Tags|MergeScope|JoinSearch|AppendQuery' -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/datasource/tags.go internal/datasource/tags_test.go
git commit -m "feat: shared tag application (search join, scope-brace merge)"
```

---

### Task 6: DataSource interface, dispatch, TimeWindow, and the `Run()` wrapper

Wires everything into a working plugin (no rate-limit/cache yet — that is layered on in Tasks 10–11). After this task the plugin compiles end-to-end and `Run()` dispatches to a source; the sources themselves are stubbed here with a trivial `metrics`-only path proven by an integration-style test that uses a fake DataSource, then Tasks 7–9 replace the stubs.

**Files:**
- Create: `internal/datasource/datasource.go`
- Modify: `internal/plugin/plugin.go` (flesh out `InitPlugin`, `Run`, `GetMetadata`; add fields to `RpcPlugin`)
- Create: `internal/plugin/run_test.go`

**Interfaces:**
- Consumes: `pluginconfig.Config` (Task 2), `datadog.NewClient`/`AuthContext`/`Resolver` (Tasks 3–4), `v1alpha1`, `evaluate`, `metricutil`, Datadog `*datadog.APIClient`.
- Produces:
  - `type Result struct { Value interface{}; Metadata map[string]string }`
  - `type TimeWindow struct { From, To time.Time }` + `func WindowFrom(now time.Time, interval string, def time.Duration) (TimeWindow, error)` + methods `FromUnixMillis()/ToUnixMillis() int64`, `FromUnixSeconds()/ToUnixSeconds() int64`. **Note:** the window’s end is taken from the context/caller; for determinism the plugin passes `now` in. Signature: `func WindowFrom(now time.Time, interval string, def time.Duration) (TimeWindow, error)`.
  - `type DataSource interface { Query(ctx context.Context, client *datadog.APIClient, cfg *config.Config) (Result, error); Key(cfg *config.Config) string }`
  - `func Select(cfg *config.Config) (DataSource, error)` — returns the impl for `cfg.Source()`.
  - In `internal/plugin`: `RpcPlugin` gains `resolver *datadog.Resolver` and `controllerNamespace string`; `Run()` is fully implemented; `GetMetadata` returns the resolved source + tags.

To resolve the import direction: `internal/datasource` imports `internal/plugin` for the `Config` type. `internal/plugin` must therefore NOT import `internal/datasource` at the type level for `Run`. Break this by having `internal/datasource.Select` and `DataSource` consumed in `plugin.Run` through a tiny indirection: **move `Config` into its own leaf package** `internal/config` that both import. **Revised file plan:** create `internal/config/config.go` (the Task 2 content, package `config`) and have `internal/plugin` and `internal/datasource` both import `internal/config`. Update Task 2's package name to `config` and its import path accordingly when executing; the test file moves to `internal/config/config_test.go`. (This is the one cross-cutting refactor; do it as Step 1 here.)

- [ ] **Step 1: Move config to a leaf package to break the import cycle**

```bash
git mv internal/plugin/config.go internal/config/config.go
git mv internal/plugin/config_test.go internal/config/config_test.go
```

Then edit both moved files: change `package plugin` → `package config`. In `internal/config/config.go` rename the exported `ConfigKey` constant usage stays the same. Update `internal/plugin/config_test.go`'s former `pluginName` const — it now lives in the config package as `config.ConfigKey`. Update any references.

Run: `go build ./... && go test ./internal/config/ -v`
Expected: config tests pass under the new package.

- [ ] **Step 2: Write the failing `Run()` integration test with a fake DataSource**

Create `internal/plugin/run_test.go`:

```go
package plugin

import (
	"context"
	"testing"

	"github.com/DataDog/datadog-api-client-go/v2/api/datadog"
	"github.com/argoproj/argo-rollouts/pkg/apis/rollouts/v1alpha1"
	"github.com/mubarak-j/rollouts-plugin-metric-datadog/internal/config"
	"github.com/mubarak-j/rollouts-plugin-metric-datadog/internal/datasource"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type fakeSource struct{ value interface{} }

func (f fakeSource) Query(_ context.Context, _ *datadog.APIClient, _ *config.Config) (datasource.Result, error) {
	return datasource.Result{Value: f.value, Metadata: map[string]string{"resolvedQuery": "fake"}}, nil
}
func (f fakeSource) Key(_ *config.Config) string { return "fake" }

func TestRun_SuccessfulScalar(t *testing.T) {
	g := newTestPlugin(t, fakeSource{value: 0.99})
	m := metricWith(t, `{"site":"datadoghq.com","metrics":{"query":"avg:cpu{*}"}}`)
	m.SuccessCondition = "result >= 0.95"

	out := g.Run(&v1alpha1.AnalysisRun{}, m)
	require.Equal(t, v1alpha1.AnalysisPhaseSuccessful, out.Phase, out.Message)
	assert.Equal(t, "0.99", out.Value)
	assert.Equal(t, "fake", out.Metadata["resolvedQuery"])
}

func TestRun_FailingCondition(t *testing.T) {
	g := newTestPlugin(t, fakeSource{value: 0.5})
	m := metricWith(t, `{"metrics":{"query":"avg:cpu{*}"}}`)
	m.SuccessCondition = "result >= 0.95"
	out := g.Run(&v1alpha1.AnalysisRun{}, m)
	assert.Equal(t, v1alpha1.AnalysisPhaseFailed, out.Phase)
}

func TestRun_ConfigErrorMapsToErrorPhase(t *testing.T) {
	g := newTestPlugin(t, fakeSource{})
	m := metricWith(t, `{}`) // no source
	out := g.Run(&v1alpha1.AnalysisRun{}, m)
	assert.Equal(t, v1alpha1.AnalysisPhaseError, out.Phase)
	assert.Contains(t, out.Message, "exactly one")
}
```

Add a test helper file `internal/plugin/helpers_test.go`:

```go
package plugin

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/DataDog/datadog-api-client-go/v2/api/datadog"
	"github.com/argoproj/argo-rollouts/pkg/apis/rollouts/v1alpha1"
	ddinternal "github.com/mubarak-j/rollouts-plugin-metric-datadog/internal/datadog"
	"github.com/mubarak-j/rollouts-plugin-metric-datadog/internal/config"
	"github.com/mubarak-j/rollouts-plugin-metric-datadog/internal/datasource"
	log "github.com/sirupsen/logrus"
)

func metricWith(t *testing.T, raw string) v1alpha1.Metric {
	t.Helper()
	return v1alpha1.Metric{Provider: v1alpha1.MetricProvider{
		Plugin: map[string]json.RawMessage{config.ConfigKey: json.RawMessage(raw)},
	}}
}

type stubSecrets struct{}

func (stubSecrets) GetSecret(_ context.Context, _, _ string) (map[string][]byte, error) {
	return map[string][]byte{"api-key": []byte("AK"), "app-key": []byte("PK")}, nil
}

// newTestPlugin builds an RpcPlugin whose source selection is overridden to a fake.
func newTestPlugin(t *testing.T, ds datasource.DataSource) *RpcPlugin {
	t.Helper()
	return &RpcPlugin{
		LogCtx:              *log.WithField("test", t.Name()),
		resolver:            &ddinternal.Resolver{Secrets: stubSecrets{}, ControllerNamespace: "argo-rollouts"},
		controllerNamespace: "argo-rollouts",
		selectSource:        func(_ *config.Config) (datasource.DataSource, error) { return ds, nil },
		newClient: func(creds ddinternal.Credentials, opts ddinternal.ClientOptions) (*datadog.APIClient, error) {
			return ddinternal.NewClient(creds, opts)
		},
	}
}
```

- [ ] **Step 3: Run the tests to verify they fail**

Run: `go test ./internal/plugin/ -run TestRun -v`
Expected: FAIL — `RpcPlugin` lacks `resolver`/`selectSource`/`newClient`; `Run` not implemented.

- [ ] **Step 4: Implement `datasource.go`**

Create `internal/datasource/datasource.go`:

```go
package datasource

import (
	"context"
	"fmt"
	"time"

	"github.com/DataDog/datadog-api-client-go/v2/api/datadog"
	"github.com/mubarak-j/rollouts-plugin-metric-datadog/internal/config"
)

type Result struct {
	Value    interface{}
	Metadata map[string]string
}

type DataSource interface {
	Query(ctx context.Context, client *datadog.APIClient, cfg *config.Config) (Result, error)
	Key(cfg *config.Config) string
}

// Select returns the DataSource for the single configured source. The concrete
// constructors are assigned in their own files (Tasks 7–9).
func Select(cfg *config.Config) (DataSource, error) {
	switch cfg.Source() {
	case "metrics":
		return metricsSource{}, nil
	case "monitor":
		return monitorSource{}, nil
	case "slo":
		return sloSource{}, nil
	default:
		return nil, fmt.Errorf("no data source for %q", cfg.Source())
	}
}

type TimeWindow struct {
	From time.Time
	To   time.Time
}

func WindowFrom(now time.Time, interval string, def time.Duration) (TimeWindow, error) {
	d := def
	if interval != "" {
		parsed, err := parseDuration(interval)
		if err != nil {
			return TimeWindow{}, fmt.Errorf("invalid interval %q: %w", interval, err)
		}
		d = parsed
	}
	return TimeWindow{From: now.Add(-d), To: now}, nil
}

func (w TimeWindow) FromUnixMillis() int64 { return w.From.UnixMilli() }
func (w TimeWindow) ToUnixMillis() int64   { return w.To.UnixMilli() }
func (w TimeWindow) FromUnixSeconds() int64 { return w.From.Unix() }
func (w TimeWindow) ToUnixSeconds() int64   { return w.To.Unix() }

// parseDuration extends time.ParseDuration with day support (e.g. "7d").
func parseDuration(s string) (time.Duration, error) {
	if len(s) > 1 && s[len(s)-1] == 'd' {
		days, err := time.ParseDuration(s[:len(s)-1] + "h")
		if err != nil {
			return 0, err
		}
		return days * 24, nil
	}
	return time.ParseDuration(s)
}
```

**Note:** `metricsSource{}`, `monitorSource{}`, `sloSource{}` are referenced here but defined in Tasks 7–9. To keep this task self-contained and green, add temporary stub types now (they are replaced in Tasks 7–9):

Create `internal/datasource/stubs.go` (deleted incrementally as Tasks 7–9 land):

```go
package datasource

import (
	"context"
	"fmt"

	"github.com/DataDog/datadog-api-client-go/v2/api/datadog"
	"github.com/mubarak-j/rollouts-plugin-metric-datadog/internal/config"
)

type metricsSource struct{}

func (metricsSource) Query(context.Context, *datadog.APIClient, *config.Config) (Result, error) {
	return Result{}, fmt.Errorf("metrics source not implemented")
}
func (metricsSource) Key(*config.Config) string { return "metrics" }

type monitorSource struct{}

func (monitorSource) Query(context.Context, *datadog.APIClient, *config.Config) (Result, error) {
	return Result{}, fmt.Errorf("monitor source not implemented")
}
func (monitorSource) Key(*config.Config) string { return "monitor" }

type sloSource struct{}

func (sloSource) Query(context.Context, *datadog.APIClient, *config.Config) (Result, error) {
	return Result{}, fmt.Errorf("slo source not implemented")
}
func (sloSource) Key(*config.Config) string { return "slo" }
```

- [ ] **Step 5: Implement the `Run()` wrapper and wire fields in `plugin.go`**

**Replace the entire `internal/plugin/plugin.go`** from Task 1 with this complete file. It supersedes the Task 1 skeleton — the old `errNotImplemented` sentinel and the Task 1 imports are gone, and the no-op `Resume`/`Terminate`/`GarbageCollect` are included here so there is no separate "keep the old methods" merge step:

```go
package plugin

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"

	"github.com/DataDog/datadog-api-client-go/v2/api/datadog"
	rolloutsPlugin "github.com/argoproj/argo-rollouts/metricproviders/plugin"
	"github.com/argoproj/argo-rollouts/pkg/apis/rollouts/v1alpha1"
	"github.com/argoproj/argo-rollouts/utils/evaluate"
	metricutil "github.com/argoproj/argo-rollouts/utils/metric"
	timeutil "github.com/argoproj/argo-rollouts/utils/time"
	"github.com/argoproj/argo-rollouts/utils/plugin/types"
	log "github.com/sirupsen/logrus"

	"github.com/mubarak-j/rollouts-plugin-metric-datadog/internal/config"
	ddinternal "github.com/mubarak-j/rollouts-plugin-metric-datadog/internal/datadog"
	"github.com/mubarak-j/rollouts-plugin-metric-datadog/internal/datasource"
)

type RpcPlugin struct {
	LogCtx              log.Entry
	resolver            *ddinternal.Resolver
	controllerNamespace string

	// indirections (overridable in tests)
	selectSource func(*config.Config) (datasource.DataSource, error)
	newClient    func(ddinternal.Credentials, ddinternal.ClientOptions) (*datadog.APIClient, error)
}

func (g *RpcPlugin) InitPlugin() types.RpcError {
	getter, ns, err := ddinternal.NewKubeSecretGetter()
	if err != nil {
		return types.RpcError{ErrorString: fmt.Sprintf("init kube client: %v", err)}
	}
	g.resolver = &ddinternal.Resolver{Secrets: getter, ControllerNamespace: ns}
	g.controllerNamespace = ns
	g.selectSource = datasource.Select
	g.newClient = ddinternal.NewClient
	return types.RpcError{}
}

func (g *RpcPlugin) Run(analysisRun *v1alpha1.AnalysisRun, metric v1alpha1.Metric) v1alpha1.Measurement {
	startTime := timeutil.MetaNow()
	m := v1alpha1.Measurement{StartedAt: &startTime}

	cfg, err := config.ParseConfig(metric)
	if err != nil {
		return finish(metricutil.MarkMeasurementError(m, err))
	}

	ctx, cancel := context.WithTimeout(context.Background(), cfg.Timeout())
	defer cancel()

	var ref *ddinternal.SecretRefInput
	if cfg.SecretRef != nil {
		ref = &ddinternal.SecretRefInput{Name: cfg.SecretRef.Name, Namespaced: cfg.SecretRef.Namespaced}
	}
	creds, err := g.resolver.Resolve(ctx, analysisRun.Namespace, ref)
	if err != nil {
		return finish(metricutil.MarkMeasurementError(m, err))
	}

	client, err := g.newClient(creds, ddinternal.ClientOptions{
		Site: cfg.Site, Address: cfg.Address, Timeout: cfg.Timeout(),
	})
	if err != nil {
		return finish(metricutil.MarkMeasurementError(m, err))
	}
	ctx = ddinternal.AuthContext(ctx, creds, cfg.Site)

	ds, err := g.selectSource(cfg)
	if err != nil {
		return finish(metricutil.MarkMeasurementError(m, err))
	}

	res, err := ds.Query(ctx, client, cfg)
	if err != nil {
		return finish(metricutil.MarkMeasurementError(m, err))
	}

	phase, err := evaluate.EvaluateResult(res.Value, metric, g.LogCtx)
	if err != nil {
		return finish(metricutil.MarkMeasurementError(m, err))
	}
	m.Phase = phase
	m.Value = stringify(res.Value)
	m.Metadata = res.Metadata
	return finish(m)
}

func (g *RpcPlugin) GetMetadata(metric v1alpha1.Metric) map[string]string {
	md := map[string]string{}
	cfg, err := config.ParseConfig(metric)
	if err != nil {
		return md
	}
	md["source"] = cfg.Source()
	if len(cfg.Tags) > 0 {
		md["tags"] = fmt.Sprintf("%v", cfg.Tags)
	}
	return md
}

func finish(m v1alpha1.Measurement) v1alpha1.Measurement {
	if m.FinishedAt == nil {
		t := timeutil.MetaNow()
		m.FinishedAt = &t
	}
	return m
}

// stringify renders a resolved value for Measurement.Value (display only;
// EvaluateResult receives the raw typed value).
func stringify(v interface{}) string {
	switch x := v.(type) {
	case float64:
		return strconv.FormatFloat(x, 'f', -1, 64)
	case string:
		return x
	case nil:
		return ""
	default:
		b, err := json.Marshal(x)
		if err != nil {
			return fmt.Sprintf("%v", x)
		}
		return string(b)
	}
}

func (g *RpcPlugin) Type() string { return rolloutsPlugin.ProviderType }

func (g *RpcPlugin) Resume(_ *v1alpha1.AnalysisRun, _ v1alpha1.Metric, measurement v1alpha1.Measurement) v1alpha1.Measurement {
	return measurement
}

func (g *RpcPlugin) Terminate(_ *v1alpha1.AnalysisRun, _ v1alpha1.Metric, measurement v1alpha1.Measurement) v1alpha1.Measurement {
	return measurement
}

func (g *RpcPlugin) GarbageCollect(_ *v1alpha1.AnalysisRun, _ v1alpha1.Metric, _ int) types.RpcError {
	return types.RpcError{}
}
```

This is the complete file as of Task 6 (Tasks 10–11 later add the `limiter`/`cache` fields and the cache-wrapped call). After writing it, run `go build ./...` to confirm there are no leftover references to the Task 1 skeleton (`errNotImplemented`) or unused imports.

- [ ] **Step 6: Run the tests to verify they pass**

Run: `go test ./internal/plugin/ -run TestRun -v && go build ./...`
Expected: PASS; full module builds.

- [ ] **Step 7: Run the whole suite**

Run: `go test ./... -race`
Expected: all green (sources still stubbed, but nothing calls them yet except the fake in tests).

- [ ] **Step 8: Commit**

```bash
git add -A
git commit -m "feat: DataSource interface, dispatch, time windows, and Run() wrapper"
```

---

### Task 7: `metrics` source (built-in parity, v1 + v2)

Implements the `metrics` block: scalar float result, v2 scalar (single + multi-query+formula) and v1 query, with shared tags merged into each query scope.

**Files:**
- Modify: `internal/datasource/stubs.go` (remove `metricsSource`)
- Create: `internal/datasource/metrics.go`
- Create: `internal/datasource/metrics_test.go`

**Interfaces:**
- Consumes: `config.Config`/`MetricsConfig`, `MergeScopeTags` (Task 5), `WindowFrom`/`TimeWindow` (Task 6), Datadog `datadogV1`/`datadogV2`/`datadog`.
- Produces: `metricsSource` implementing `DataSource`; `Query` returns `Result{Value: float64}`.

- [ ] **Step 1: Write failing tests against an httptest server (v2 single, v2 formula, v1, empty)**

Create `internal/datasource/metrics_test.go`:

```go
package datasource

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/mubarak-j/rollouts-plugin-metric-datadog/internal/config"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMetrics_V2Scalar(t *testing.T) {
	var gotBody string
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		gotBody = string(b)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"data":{"type":"scalar_response","attributes":{"columns":[{"type":"number","values":[0.97]}]}}}`))
	}))
	defer ts.Close()

	cfg := &config.Config{
		Tags:    []string{"service:x"},
		Metrics: &config.MetricsConfig{APIVersion: "v2", Query: "avg:cpu{*}"},
	}
	res, err := runSource(t, ts, metricsSource{}, cfg)
	require.NoError(t, err)
	assert.InDelta(t, 0.97, res.Value.(float64), 1e-9)
	assert.Contains(t, gotBody, "service:x") // tags merged into scope
}

func TestMetrics_V2Formula(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"data":{"attributes":{"columns":[{"type":"number","values":[0.5]}]}}}`))
	}))
	defer ts.Close()
	cfg := &config.Config{Metrics: &config.MetricsConfig{
		APIVersion: "v2",
		Queries:    map[string]string{"a": "sum:hits{*}.as_count()", "b": "sum:errs{*}.as_count()"},
		Formula:    "(a-b)/a",
	}}
	res, err := runSource(t, ts, metricsSource{}, cfg)
	require.NoError(t, err)
	assert.InDelta(t, 0.5, res.Value.(float64), 1e-9)
}

func TestMetrics_V1(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.True(t, strings.Contains(r.URL.Path, "/api/v1/query"))
		_, _ = w.Write([]byte(`{"series":[{"pointlist":[[1000,1.0],[2000,2.5]]}]}`))
	}))
	defer ts.Close()
	cfg := &config.Config{Metrics: &config.MetricsConfig{APIVersion: "v1", Query: "avg:cpu{*}"}}
	res, err := runSource(t, ts, metricsSource{}, cfg)
	require.NoError(t, err)
	assert.InDelta(t, 2.5, res.Value.(float64), 1e-9) // latest point
}

func TestMetrics_EmptyIsNil(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"data":{"attributes":{"columns":[]}}}`))
	}))
	defer ts.Close()
	cfg := &config.Config{Metrics: &config.MetricsConfig{APIVersion: "v2", Query: "avg:cpu{*}"}}
	res, err := runSource(t, ts, metricsSource{}, cfg)
	require.NoError(t, err)
	assert.Nil(t, res.Value) // scalar empty => nil; Run()'s EvaluateResult surfaces "no value"
}

// A single named query with no formula is valid config (the formula rule only
// fires for >1 query). The source must synthesize the formula from the sole
// query name so it never sends an empty Formula to Datadog.
func TestMetrics_V2SingleNamedQueryNoFormula(t *testing.T) {
	var gotBody string
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		gotBody = string(b)
		_, _ = w.Write([]byte(`{"data":{"attributes":{"columns":[{"type":"number","values":[0.42]}]}}}`))
	}))
	defer ts.Close()
	cfg := &config.Config{Metrics: &config.MetricsConfig{
		APIVersion: "v2",
		Queries:    map[string]string{"a": "avg:cpu{*}"}, // 1 query, no formula
	}}
	res, err := runSource(t, ts, metricsSource{}, cfg)
	require.NoError(t, err)
	assert.InDelta(t, 0.42, res.Value.(float64), 1e-9)
	assert.Contains(t, gotBody, `"formula":"a"`) // synthesized from the query name, not empty
}
```

`runSource` is the shared helper below; the `io` import is used by the body-capturing tests.

Create the shared helper `internal/datasource/helpers_test.go`:

```go
package datasource

import (
	"context"
	"net/http/httptest"
	"testing"

	"github.com/mubarak-j/rollouts-plugin-metric-datadog/internal/config"
	ddinternal "github.com/mubarak-j/rollouts-plugin-metric-datadog/internal/datadog"
)

func runSource(t *testing.T, ts *httptest.Server, ds DataSource, cfg *config.Config) (Result, error) {
	t.Helper()
	creds := ddinternal.Credentials{APIKey: "AK", AppKey: "PK", Address: ts.URL}
	client, err := ddinternal.NewClient(creds, ddinternal.ClientOptions{Address: ts.URL})
	if err != nil {
		return Result{}, err
	}
	ctx := ddinternal.AuthContext(context.Background(), creds, "")
	return ds.Query(ctx, client, cfg)
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `go test ./internal/datasource/ -run TestMetrics -v`
Expected: FAIL — `metricsSource.Query` returns the stub error.

- [ ] **Step 3: Remove the metrics stub and implement `metrics.go`**

Delete the `metricsSource` type from `internal/datasource/stubs.go`. Create `internal/datasource/metrics.go`:

```go
package datasource

import (
	"context"
	"fmt"
	"time"

	"github.com/DataDog/datadog-api-client-go/v2/api/datadog"
	datadogV1 "github.com/DataDog/datadog-api-client-go/v2/api/datadogV1"
	datadogV2 "github.com/DataDog/datadog-api-client-go/v2/api/datadogV2"
	"github.com/mubarak-j/rollouts-plugin-metric-datadog/internal/config"
)

type metricsSource struct{}

func (metricsSource) Key(cfg *config.Config) string {
	m := cfg.Metrics
	return fmt.Sprintf("metrics|%s|%s|%v|%s|%v|%s", m.APIVersion, m.Query, m.Queries, m.Formula, cfg.Tags, m.Interval)
}

func (metricsSource) Query(ctx context.Context, client *datadog.APIClient, cfg *config.Config) (Result, error) {
	win, err := WindowFrom(time.Now(), cfg.Metrics.Interval, 5*time.Minute)
	if err != nil {
		return Result{}, err
	}
	if cfg.Metrics.APIVersion == "v1" {
		return queryV1(ctx, client, cfg, win)
	}
	return queryV2(ctx, client, cfg, win)
}

func queryV2(ctx context.Context, client *datadog.APIClient, cfg *config.Config, win TimeWindow) (Result, error) {
	m := cfg.Metrics
	agg := metricsAggregator(m.Aggregator)

	var queries []datadogV2.ScalarQuery
	var formulas []datadogV2.QueryFormula
	resolved := map[string]string{}

	if m.Query != "" {
		q := MergeScopeTags(m.Query, cfg.Tags)
		resolved["a"] = q
		queries = append(queries, scalarQuery("a", q, agg))
		formulas = append(formulas, datadogV2.QueryFormula{Formula: "a"})
	} else {
		// stable name order for determinism
		names := sortedKeys(m.Queries)
		for _, name := range names {
			q := MergeScopeTags(m.Queries[name], cfg.Tags)
			resolved[name] = q
			queries = append(queries, scalarQuery(name, q, agg))
		}
		formula := m.Formula
		if formula == "" {
			// A single query with no formula is valid (validation only requires a
			// formula for >1 query). Project the sole query's column by name so we
			// never send an empty Formula to Datadog.
			formula = names[0]
		}
		formulas = append(formulas, datadogV2.QueryFormula{Formula: formula})
	}

	body := datadogV2.ScalarFormulaQueryRequest{
		Data: datadogV2.ScalarFormulaRequest{
			Type: datadogV2.SCALARFORMULAREQUESTTYPE_SCALAR_REQUEST,
			Attributes: datadogV2.ScalarFormulaRequestAttributes{
				From:     win.FromUnixMillis(),
				To:       win.ToUnixMillis(),
				Queries:  queries,
				Formulas: formulas,
			},
		},
	}

	api := datadogV2.NewMetricsApi(client)
	resp, _, err := api.QueryScalarData(ctx, body)
	if err != nil {
		return Result{}, fmt.Errorf("datadog v2 scalar query: %w", err)
	}
	value := extractV2Scalar(resp)
	return Result{Value: value, Metadata: map[string]string{
		"source": "metrics", "apiVersion": "v2", "resolvedQuery": fmt.Sprintf("%v", resolved),
	}}, nil
}

func scalarQuery(name, query string, agg datadogV2.MetricsAggregator) datadogV2.ScalarQuery {
	n := name
	return datadogV2.ScalarQuery{MetricsScalarQuery: &datadogV2.MetricsScalarQuery{
		DataSource: datadogV2.METRICSDATASOURCE_METRICS,
		Query:      query,
		Aggregator: agg,
		Name:       &n,
	}}
}

func extractV2Scalar(resp datadogV2.ScalarFormulaQueryResponse) interface{} {
	if resp.Data == nil || resp.Data.Attributes == nil {
		return nil
	}
	cols := resp.Data.Attributes.Columns
	if len(cols) == 0 || cols[0].DataScalarColumn == nil {
		return nil
	}
	vals := cols[0].DataScalarColumn.Values
	if len(vals) == 0 || vals[0] == nil {
		return nil
	}
	return *vals[0]
}

func queryV1(ctx context.Context, client *datadog.APIClient, cfg *config.Config, win TimeWindow) (Result, error) {
	q := MergeScopeTags(cfg.Metrics.Query, cfg.Tags)
	api := datadogV1.NewMetricsApi(client)
	resp, _, err := api.QueryMetrics(ctx, win.FromUnixSeconds(), win.ToUnixSeconds(), q)
	if err != nil {
		return Result{}, fmt.Errorf("datadog v1 query: %w", err)
	}
	value := extractV1Latest(resp)
	return Result{Value: value, Metadata: map[string]string{
		"source": "metrics", "apiVersion": "v1", "resolvedQuery": q,
	}}, nil
}

func extractV1Latest(resp datadogV1.MetricsQueryResponse) interface{} {
	if len(resp.Series) == 0 {
		return nil
	}
	pl := resp.Series[0].Pointlist
	if len(pl) == 0 {
		return nil
	}
	last := pl[len(pl)-1]
	if len(last) != 2 || last[1] == nil {
		return nil
	}
	return *last[1]
}

func metricsAggregator(s string) datadogV2.MetricsAggregator {
	if s == "" {
		return datadogV2.METRICSAGGREGATOR_LAST
	}
	// MetricsAggregator is a string enum; the value is the lowercase token.
	return datadogV2.MetricsAggregator(s)
}
```

Add a small helper `sortedKeys` to `internal/datasource/datasource.go`:

```go
import "sort"

func sortedKeys(m map[string]string) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `go test ./internal/datasource/ -run TestMetrics -v`
Expected: PASS (v2 single, v2 formula, v1, empty).

- [ ] **Step 5: Add a `Run()` integration test proving condition semantics for metrics**

Append to `internal/plugin/run_test.go`:

```go
func TestRun_MetricsConditionAgainstRealServer(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"data":{"attributes":{"columns":[{"type":"number","values":[0.99]}]}}}`))
	}))
	defer ts.Close()

	g := &RpcPlugin{
		LogCtx:   *log.WithField("test", t.Name()),
		resolver: &ddinternal.Resolver{Secrets: stubSecrets{}, ControllerNamespace: "argo-rollouts"},
		selectSource: datasource.Select,
		newClient: func(creds ddinternal.Credentials, opts ddinternal.ClientOptions) (*datadog.APIClient, error) {
			opts.Address = ts.URL
			return ddinternal.NewClient(creds, opts)
		},
	}
	m := metricWith(t, `{"metrics":{"query":"avg:cpu{*}"}}`)
	m.SuccessCondition = "result >= 0.95"
	out := g.Run(&v1alpha1.AnalysisRun{}, m)
	require.Equal(t, v1alpha1.AnalysisPhaseSuccessful, out.Phase, out.Message)
}
```

(Add `net/http`, `net/http/httptest` imports to the test file.)

Run: `go test ./internal/plugin/ -run TestRun_Metrics -v`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add internal/datasource/metrics.go internal/datasource/metrics_test.go internal/datasource/helpers_test.go internal/datasource/datasource.go internal/datasource/stubs.go internal/plugin/run_test.go
git commit -m "feat: metrics data source (v1 + v2 scalar/formula) with scope tag merge"
```

---

### Task 8: `monitor` source (search + by-id) with migration-parity test

Implements the `monitor` block. Search mode returns the group-search response as `map[string]interface{}` (§6 struct→map conversion) so existing `web`-provider conditions match; by-id returns the monitor object as a map. Includes the §12 migration-parity test.

**Files:**
- Modify: `internal/datasource/stubs.go` (remove `monitorSource`)
- Create: `internal/datasource/monitor.go`
- Create: `internal/datasource/monitor_test.go`

**Interfaces:**
- Consumes: `config.MonitorConfig`, `JoinSearchQuery` (Task 5), Datadog `datadogV1`.
- Produces: `monitorSource` implementing `DataSource`; `Query` returns `Result{Value: map[string]interface{}}`.
- Also produces a reusable `func structToMap(v interface{}) (map[string]interface{}, error)` (used by slo too) — place it in `datasource.go`.

- [ ] **Step 1: Write failing tests (search → map, by-id → map, migration parity)**

Create `internal/datasource/monitor_test.go`:

```go
package datasource

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/argoproj/argo-rollouts/utils/evaluate"
	"github.com/argoproj/argo-rollouts/pkg/apis/rollouts/v1alpha1"
	"github.com/mubarak-j/rollouts-plugin-metric-datadog/internal/config"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const monitorSearchJSON = `{
  "counts": {"status": [{"name": "Alert", "count": 0}, {"name": "OK", "count": 3}]},
  "groups": [],
  "metadata": {"total_count": 3}
}`

func TestMonitor_SearchReturnsMap(t *testing.T) {
	var gotQuery string
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotQuery = r.URL.Query().Get("query")
		_, _ = w.Write([]byte(monitorSearchJSON))
	}))
	defer ts.Close()

	cfg := &config.Config{Tags: []string{"service:x", "env:prod"}, Monitor: &config.MonitorConfig{Query: "muted:false"}}
	res, err := runSource(t, ts, monitorSource{}, cfg)
	require.NoError(t, err)
	m, ok := res.Value.(map[string]interface{})
	require.True(t, ok)
	assert.Contains(t, m, "counts")
	assert.True(t, strings.Contains(gotQuery, "service:x") && strings.Contains(gotQuery, "muted:false"))
}

func TestMonitor_MigrationParity(t *testing.T) {
	// The existing web-provider conditions must still match after struct->map.
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(monitorSearchJSON))
	}))
	defer ts.Close()
	cfg := &config.Config{Tags: []string{"service:x"}, Monitor: &config.MonitorConfig{}}
	res, err := runSource(t, ts, monitorSource{}, cfg)
	require.NoError(t, err)

	metric := v1alpha1.Metric{
		FailureCondition: "any(result.counts.status, {.name == 'Alert' && .count > 0})",
		SuccessCondition: "result.counts.status == nil || any(result.counts.status, {.name != 'Alert'})",
	}
	phase, err := evaluate.EvaluateResult(res.Value, metric, *log.WithField("t", t.Name()))
	require.NoError(t, err)
	assert.Equal(t, v1alpha1.AnalysisPhaseSuccessful, phase) // 0 Alerts => success
}

func TestMonitor_ByID(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.True(t, strings.Contains(r.URL.Path, "/api/v1/monitor/123"))
		_, _ = w.Write([]byte(`{"id":123,"name":"m","overall_state":"OK","query":"x","type":"metric alert"}`))
	}))
	defer ts.Close()
	id := int64(123)
	cfg := &config.Config{Monitor: &config.MonitorConfig{ID: &id}}
	res, err := runSource(t, ts, monitorSource{}, cfg)
	require.NoError(t, err)
	m := res.Value.(map[string]interface{})
	assert.Equal(t, "OK", m["overall_state"])
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `go test ./internal/datasource/ -run TestMonitor -v`
Expected: FAIL — stub error.

- [ ] **Step 3: Remove the monitor stub and implement `monitor.go`**

Delete `monitorSource` from `stubs.go`. Add `structToMap` to `datasource.go`:

```go
import "encoding/json"

// structToMap marshals a typed SDK response to JSON then re-unmarshals into a
// generic map, so expr conditions traverse the same field names as the raw
// Datadog API JSON (the web-provider compatibility contract, §6).
func structToMap(v interface{}) (map[string]interface{}, error) {
	b, err := json.Marshal(v)
	if err != nil {
		return nil, err
	}
	var m map[string]interface{}
	if err := json.Unmarshal(b, &m); err != nil {
		return nil, err
	}
	return m, nil
}
```

Create `internal/datasource/monitor.go`:

```go
package datasource

import (
	"context"
	"fmt"

	"github.com/DataDog/datadog-api-client-go/v2/api/datadog"
	datadogV1 "github.com/DataDog/datadog-api-client-go/v2/api/datadogV1"
	"github.com/mubarak-j/rollouts-plugin-metric-datadog/internal/config"
)

type monitorSource struct{}

func (monitorSource) Key(cfg *config.Config) string {
	mn := cfg.Monitor
	if mn.ID != nil {
		return fmt.Sprintf("monitor|id|%d", *mn.ID)
	}
	return fmt.Sprintf("monitor|search|%s", JoinSearchQuery(cfg.Tags, mn.Query))
}

func (monitorSource) Query(ctx context.Context, client *datadog.APIClient, cfg *config.Config) (Result, error) {
	api := datadogV1.NewMonitorsApi(client)
	if cfg.Monitor.ID != nil {
		mon, _, err := api.GetMonitor(ctx, *cfg.Monitor.ID)
		if err != nil {
			return Result{}, fmt.Errorf("datadog GetMonitor: %w", err)
		}
		m, err := structToMap(mon)
		if err != nil {
			return Result{}, err
		}
		return Result{Value: m, Metadata: map[string]string{"source": "monitor", "mode": "id"}}, nil
	}

	query := JoinSearchQuery(cfg.Tags, cfg.Monitor.Query)
	opts := *datadogV1.NewSearchMonitorGroupsOptionalParameters().WithQuery(query)
	resp, _, err := api.SearchMonitorGroups(ctx, opts)
	if err != nil {
		return Result{}, fmt.Errorf("datadog SearchMonitorGroups: %w", err)
	}
	m, err := structToMap(resp)
	if err != nil {
		return Result{}, err
	}
	return Result{Value: m, Metadata: map[string]string{
		"source": "monitor", "mode": "search", "resolvedQuery": query,
	}}, nil
}
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `go test ./internal/datasource/ -run TestMonitor -v`
Expected: PASS (search, migration-parity, by-id).

- [ ] **Step 5: Commit**

```bash
git add internal/datasource/monitor.go internal/datasource/monitor_test.go internal/datasource/datasource.go internal/datasource/stubs.go
git commit -m "feat: monitor data source (search + by-id) with web-provider parity"
```

---

### Task 9: `slo` source (search + by-id)

Implements the `slo` block. Search mode flattens `resp.Data.Attributes.Slos[].Data.Attributes` into `{ slos: [...], facets: ... }` (§5.2) so conditions read `result.slos[].overall_status[].state`; by-id mode returns the SLI attainment % as a scalar float via `GetSLOHistory` (epoch **seconds**).

**Files:**
- Modify: `internal/datasource/stubs.go` (remove `sloSource` — file may now be empty; delete it if so)
- Create: `internal/datasource/slo.go`
- Create: `internal/datasource/slo_test.go`

**Interfaces:**
- Consumes: `config.SLOConfig`, `JoinSearchQuery`, `WindowFrom`, `structToMap`, Datadog `datadogV1`.
- Produces: `sloSource` implementing `DataSource`; search → `Result{Value: map[string]interface{}}`, by-id → `Result{Value: float64}`.

- [ ] **Step 1: Write failing tests (search flatten + breach condition, by-id scalar)**

Create `internal/datasource/slo_test.go`:

```go
package datasource

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/argoproj/argo-rollouts/pkg/apis/rollouts/v1alpha1"
	"github.com/argoproj/argo-rollouts/utils/evaluate"
	"github.com/mubarak-j/rollouts-plugin-metric-datadog/internal/config"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const sloSearchJSON = `{
  "data": {"attributes": {"slos": [
    {"data": {"id": "abc", "type": "slo", "attributes": {
      "name": "checkout availability",
      "all_tags": ["service:x"],
      "overall_status": [{"state": "ok", "status": 99.95, "target": 99.9, "error_budget_remaining": 50.0}]
    }}}
  ], "facets": {}}}
}`

func TestSLO_SearchFlattenAndCondition(t *testing.T) {
	var gotQuery string
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotQuery = r.URL.Query().Get("query")
		_, _ = w.Write([]byte(sloSearchJSON))
	}))
	defer ts.Close()

	cfg := &config.Config{Tags: []string{"service:x"}, SLO: &config.SLOConfig{Query: "slo_type:metric"}}
	res, err := runSource(t, ts, sloSource{}, cfg)
	require.NoError(t, err)
	m := res.Value.(map[string]interface{})
	slos := m["slos"].([]interface{})
	require.Len(t, slos, 1)
	assert.True(t, strings.Contains(gotQuery, "service:x") && strings.Contains(gotQuery, "slo_type:metric"))

	metric := v1alpha1.Metric{
		FailureCondition: "any(result.slos, {any(.overall_status, {.state == 'breached'})})",
		SuccessCondition: "all(result.slos, {all(.overall_status, {.state != 'breached'})})",
	}
	phase, err := evaluate.EvaluateResult(res.Value, metric, *log.WithField("t", t.Name()))
	require.NoError(t, err)
	assert.Equal(t, v1alpha1.AnalysisPhaseSuccessful, phase)
}

func TestSLO_ByIDScalar(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.True(t, strings.Contains(r.URL.Path, "/api/v1/slo/abc/history"))
		_, _ = w.Write([]byte(`{"data":{"overall":{"sli_value":99.92}}}`))
	}))
	defer ts.Close()
	id := "abc"
	cfg := &config.Config{SLO: &config.SLOConfig{ID: &id, Interval: "7d"}}
	res, err := runSource(t, ts, sloSource{}, cfg)
	require.NoError(t, err)
	assert.InDelta(t, 99.92, res.Value.(float64), 1e-9)
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `go test ./internal/datasource/ -run TestSLO -v`
Expected: FAIL — stub error.

- [ ] **Step 3: Remove the slo stub and implement `slo.go`**

Delete `sloSource` from `stubs.go` (delete the file if it is now empty, and remove its reference — `Select` still refers to `sloSource{}`, which now lives in `slo.go`). Create `internal/datasource/slo.go`:

```go
package datasource

import (
	"context"
	"fmt"
	"time"

	"github.com/DataDog/datadog-api-client-go/v2/api/datadog"
	datadogV1 "github.com/DataDog/datadog-api-client-go/v2/api/datadogV1"
	"github.com/mubarak-j/rollouts-plugin-metric-datadog/internal/config"
)

type sloSource struct{}

func (sloSource) Key(cfg *config.Config) string {
	s := cfg.SLO
	if s.ID != nil {
		return fmt.Sprintf("slo|id|%s|%s", *s.ID, s.Interval)
	}
	return fmt.Sprintf("slo|search|%s", JoinSearchQuery(cfg.Tags, s.Query))
}

func (sloSource) Query(ctx context.Context, client *datadog.APIClient, cfg *config.Config) (Result, error) {
	api := datadogV1.NewServiceLevelObjectivesApi(client)

	if cfg.SLO.ID != nil {
		win, err := WindowFrom(time.Now(), cfg.SLO.Interval, 7*24*time.Hour)
		if err != nil {
			return Result{}, err
		}
		resp, _, err := api.GetSLOHistory(ctx, *cfg.SLO.ID, win.FromUnixSeconds(), win.ToUnixSeconds())
		if err != nil {
			return Result{}, fmt.Errorf("datadog GetSLOHistory: %w", err)
		}
		var value interface{}
		if resp.Data != nil && resp.Data.Overall != nil {
			if sli := resp.Data.Overall.SliValue.Get(); sli != nil {
				value = *sli
			}
		}
		return Result{Value: value, Metadata: map[string]string{"source": "slo", "mode": "id"}}, nil
	}

	query := JoinSearchQuery(cfg.Tags, cfg.SLO.Query)
	opts := *datadogV1.NewSearchSLOOptionalParameters().WithQuery(query)
	resp, _, err := api.SearchSLO(ctx, opts)
	if err != nil {
		return Result{}, fmt.Errorf("datadog SearchSLO: %w", err)
	}
	flat, err := flattenSLOSearch(resp)
	if err != nil {
		return Result{}, err
	}
	return Result{Value: flat, Metadata: map[string]string{
		"source": "slo", "mode": "search", "resolvedQuery": query,
	}}, nil
}

// flattenSLOSearch turns the nested SearchSLOResponse into { slos: [...], facets }
// where each element is the per-SLO attributes object (§5.2), so conditions read
// result.slos[].overall_status[].state etc.
func flattenSLOSearch(resp datadogV1.SearchSLOResponse) (map[string]interface{}, error) {
	out := map[string]interface{}{"slos": []interface{}{}, "facets": map[string]interface{}{}}
	if resp.Data == nil || resp.Data.Attributes == nil {
		return out, nil
	}
	slos := make([]interface{}, 0, len(resp.Data.Attributes.Slos))
	for _, s := range resp.Data.Attributes.Slos {
		if s.Data == nil || s.Data.Attributes == nil {
			continue
		}
		attrs, err := structToMap(s.Data.Attributes)
		if err != nil {
			return nil, err
		}
		slos = append(slos, attrs)
	}
	out["slos"] = slos
	if resp.Data.Attributes.Facets != nil {
		facets, err := structToMap(resp.Data.Attributes.Facets)
		if err == nil {
			out["facets"] = facets
		}
	}
	return out, nil
}
```

**Verification note for the implementer:** `SearchSLOResponseDataAttributes` may expose `Facets` under a different field name/type; if `resp.Data.Attributes.Facets` does not compile, drop the facets block (it is optional ergonomics, not required by any condition) and leave `out["facets"]` as the empty map. The `slos` flattening is the required behavior.

- [ ] **Step 4: Run the tests to verify they pass**

Run: `go test ./internal/datasource/ -run TestSLO -v`
Expected: PASS (search flatten + condition, by-id scalar).

- [ ] **Step 5: Run the full datasource + plugin suites**

Run: `go test ./... -race`
Expected: all green; no remaining stub references.

- [ ] **Step 6: Commit**

```bash
git add internal/datasource/slo.go internal/datasource/slo_test.go internal/datasource/stubs.go internal/datasource/datasource.go
git commit -m "feat: slo data source (search flatten + by-id SLI history)"
```

---

### Task 10: Shared rate-limit transport (token bucket + header adaptation + concurrency)

Implements §15.3 layers 1, 2, 4 as a single shared `http.RoundTripper` installed into every Datadog client. It is transparent to data sources. SDK retry (layer 5) is already enabled in Task 4.

**Files:**
- Create: `internal/datadog/ratelimit.go`
- Create: `internal/datadog/ratelimit_test.go`
- Modify: `internal/plugin/plugin.go` (`InitPlugin` builds one shared `*Limiter`; `Run` passes `g.limiter.Transport()` into `ClientOptions.Transport`)

**Interfaces:**
- Consumes: `golang.org/x/time/rate`, `net/http`.
- Produces:
  - `type Limiter struct { ... }` with `func NewLimiter(opts LimiterOptions) *Limiter` and `func (l *Limiter) Transport() http.RoundTripper`.
  - `type LimiterOptions struct { Enabled bool; DefaultRPS float64; MaxConcurrent int; BudgetFraction float64; StaticBuckets map[string]float64 }`.
  - Internally: per-bucket `*rate.Limiter` keyed by `X-RateLimit-Name` (falling back to a default bucket), a `chan struct{}` semaphore for `MaxConcurrent`, and last-seen header state to adapt the ceiling.
  - `func (l *Limiter) LastInfo(bucket string) RateInfo` — `type RateInfo struct { Limit, Remaining, ResetSeconds float64; Name string }` for observability (Task 11 surfaces it).

- [ ] **Step 1: Write failing tests (QPS ceiling under concurrency; 429 respects Reset)**

Create `internal/datadog/ratelimit_test.go`:

```go
package datadog

import (
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLimiter_CapsQPS(t *testing.T) {
	var count int64
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt64(&count, 1)
		w.Header().Set("X-RateLimit-Name", "test")
		w.WriteHeader(200)
	}))
	defer ts.Close()

	l := NewLimiter(LimiterOptions{Enabled: true, DefaultRPS: 10, MaxConcurrent: 4})
	client := &http.Client{Transport: l.Transport()}

	var wg sync.WaitGroup
	start := time.Now()
	for i := 0; i < 30; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			resp, err := client.Get(ts.URL)
			require.NoError(t, err)
			_ = resp.Body.Close()
		}()
	}
	wg.Wait()
	elapsed := time.Since(start)
	// 30 requests at 10 rps with a small burst should take well over ~2s.
	assert.GreaterOrEqual(t, elapsed, 2*time.Second)
	assert.Equal(t, int64(30), atomic.LoadInt64(&count))
}

func TestLimiter_AdaptsCeilingFromHeaders(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-RateLimit-Name", "metrics_query")
		w.Header().Set("X-RateLimit-Limit", "1600")
		w.Header().Set("X-RateLimit-Period", "60")
		w.Header().Set("X-RateLimit-Remaining", "1599")
		w.Header().Set("X-RateLimit-Reset", "60")
		w.WriteHeader(200)
	}))
	defer ts.Close()
	l := NewLimiter(LimiterOptions{Enabled: true, DefaultRPS: 5, MaxConcurrent: 2, BudgetFraction: 0.5})
	client := &http.Client{Transport: l.Transport()}
	resp, err := client.Get(ts.URL)
	require.NoError(t, err)
	_ = resp.Body.Close()
	info := l.LastInfo("metrics_query")
	assert.Equal(t, float64(1600), info.Limit)
	assert.Equal(t, "metrics_query", info.Name)
}

func TestLimiter_LearnsBucketPerPathAndGatesOnIt(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-RateLimit-Name", "metrics_query")
		w.WriteHeader(200)
	}))
	defer ts.Close()
	l := NewLimiter(LimiterOptions{Enabled: true, DefaultRPS: 100, MaxConcurrent: 4})
	client := &http.Client{Transport: l.Transport()}

	// Before any call, the endpoint has no learned bucket (gates on _default).
	assert.Equal(t, "", l.LearnedBucket(http.MethodGet, "/api/v2/query/scalar"))

	resp, err := client.Get(ts.URL + "/api/v2/query/scalar")
	require.NoError(t, err)
	_ = resp.Body.Close()

	// After one response, the path is mapped to the header bucket, so subsequent
	// RoundTrips gate on (and adapt) that named bucket rather than _default.
	assert.Equal(t, "metrics_query", l.LearnedBucket(http.MethodGet, "/api/v2/query/scalar"))
}

func TestLimiter_DisabledIsPassthrough(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(200) }))
	defer ts.Close()
	l := NewLimiter(LimiterOptions{Enabled: false})
	client := &http.Client{Transport: l.Transport()}
	resp, err := client.Get(ts.URL)
	require.NoError(t, err)
	_ = resp.Body.Close()
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `go test ./internal/datadog/ -run TestLimiter -v`
Expected: FAIL — `NewLimiter`, `LimiterOptions`, etc. undefined.

- [ ] **Step 3: Implement `ratelimit.go`**

Create `internal/datadog/ratelimit.go`:

```go
package datadog

import (
	"net/http"
	"strconv"
	"sync"

	"golang.org/x/time/rate"
)

type LimiterOptions struct {
	Enabled        bool
	DefaultRPS     float64
	MaxConcurrent  int
	BudgetFraction float64
	StaticBuckets  map[string]float64 // bucket name -> rps ceiling
}

type RateInfo struct {
	Name         string
	Limit        float64
	Remaining    float64
	ResetSeconds float64
}

type Limiter struct {
	opts LimiterOptions
	sem  chan struct{}

	mu         sync.Mutex
	buckets    map[string]*rate.Limiter
	lastInfo   map[string]RateInfo
	pathBucket map[string]string // "METHOD path" -> X-RateLimit-Name learned from responses
}

func NewLimiter(opts LimiterOptions) *Limiter {
	if opts.DefaultRPS <= 0 {
		opts.DefaultRPS = 10
	}
	if opts.BudgetFraction <= 0 {
		opts.BudgetFraction = 0.5
	}
	var sem chan struct{}
	if opts.MaxConcurrent > 0 {
		sem = make(chan struct{}, opts.MaxConcurrent)
	}
	return &Limiter{
		opts:       opts,
		sem:        sem,
		buckets:    map[string]*rate.Limiter{},
		lastInfo:   map[string]RateInfo{},
		pathBucket: map[string]string{},
	}
}

func (l *Limiter) Transport() http.RoundTripper { return &limitRT{l: l, next: http.DefaultTransport} }

func (l *Limiter) LastInfo(bucket string) RateInfo {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.lastInfo[bucket]
}

func pathKey(req *http.Request) string { return req.Method + " " + req.URL.Path }

// LearnedBucket reports the X-RateLimit-Name observed for a method+path, or ""
// if none has been seen yet. Exposed for observability and tests.
func (l *Limiter) LearnedBucket(method, path string) string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.pathBucket[method+" "+path]
}

func (l *Limiter) bucketFor(name string) *rate.Limiter {
	l.mu.Lock()
	defer l.mu.Unlock()
	if name == "" {
		name = "_default"
	}
	if lim, ok := l.buckets[name]; ok {
		return lim
	}
	rps := l.opts.DefaultRPS
	if v, ok := l.opts.StaticBuckets[name]; ok && v > 0 {
		rps = v
	}
	lim := rate.NewLimiter(rate.Limit(rps), max(1, int(rps)))
	l.buckets[name] = lim
	return lim
}

func (l *Limiter) observe(resp *http.Response) {
	name := resp.Header.Get("X-RateLimit-Name")
	if name == "" {
		return
	}
	info := RateInfo{
		Name:         name,
		Limit:        parseFloat(resp.Header.Get("X-RateLimit-Limit")),
		Remaining:    parseFloat(resp.Header.Get("X-RateLimit-Remaining")),
		ResetSeconds: parseFloat(resp.Header.Get("X-RateLimit-Reset")),
	}
	period := parseFloat(resp.Header.Get("X-RateLimit-Period"))

	l.mu.Lock()
	defer l.mu.Unlock()
	l.lastInfo[name] = info
	if resp.Request != nil {
		l.pathBucket[pathKey(resp.Request)] = name // learn which bucket gates this endpoint
	}
	// adapt the ceiling: limit/period * budgetFraction.
	if info.Limit > 0 && period > 0 {
		rps := (info.Limit / period) * l.opts.BudgetFraction
		if rps <= 0 {
			rps = l.opts.DefaultRPS
		}
		if lim, ok := l.buckets[name]; ok {
			lim.SetLimit(rate.Limit(rps))
		} else {
			l.buckets[name] = rate.NewLimiter(rate.Limit(rps), max(1, int(rps)))
		}
	}
}

type limitRT struct {
	l    *Limiter
	next http.RoundTripper
}

func (t *limitRT) RoundTrip(req *http.Request) (*http.Response, error) {
	if !t.l.opts.Enabled {
		return t.next.RoundTrip(req)
	}
	// concurrency cap
	if t.l.sem != nil {
		select {
		case t.l.sem <- struct{}{}:
			defer func() { <-t.l.sem }()
		case <-req.Context().Done():
			return nil, req.Context().Err()
		}
	}
	// Gate on the bucket learned for this endpoint. The first call to a given
	// path has no learned name yet and waits on "_default"; once a response's
	// X-RateLimit-Name is observed, subsequent calls to the same path gate on
	// (and benefit from the adapted ceiling of) that named bucket.
	bucket := t.l.bucketFor(t.l.LearnedBucket(req.Method, req.URL.Path))
	if err := bucket.Wait(req.Context()); err != nil {
		return nil, err
	}
	resp, err := t.next.RoundTrip(req)
	if err != nil {
		return resp, err
	}
	t.l.observe(resp)
	return resp, nil
}

func parseFloat(s string) float64 {
	f, _ := strconv.ParseFloat(s, 64)
	return f
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}
```

**Note on layer 2 (429 + Reset wait):** the Datadog SDK's own `RetryConfiguration.EnableRetry` (Task 4) already retries `429`/5xx honoring `X-Ratelimit-Reset`. The transport here intentionally does **not** duplicate 429 retry — it shapes the *outbound* rate so 429s are rare, and the SDK handles the residual. The `TestLimiter` 429 scenario from §15.6 is covered by the SDK retry path and asserted at the `Run()` level in Task 11 (last-known-good).

- [ ] **Step 4: Run the tests to verify they pass**

Run: `go test ./internal/datadog/ -run TestLimiter -race -v`
Expected: PASS. (The QPS test takes ~2–3s by design.)

- [ ] **Step 5: Wire one shared limiter into the plugin**

Edit `internal/plugin/plugin.go`:
- Add field `limiter *ddinternal.Limiter` to `RpcPlugin`.
- In `InitPlugin`, after building the resolver:

```go
g.limiter = ddinternal.NewLimiter(ddinternal.LimiterOptions{
	Enabled:       true,
	DefaultRPS:    10,
	MaxConcurrent: 16,
})
```

- In `Run`, pass the transport into client options:

```go
client, err := g.newClient(creds, ddinternal.ClientOptions{
	Site: cfg.Site, Address: cfg.Address, Timeout: cfg.Timeout(),
	Transport: g.transport(),
})
```

Add a helper that honors per-metric `rateLimit.enabled` and merges config:

```go
func (g *RpcPlugin) transport() http.RoundTripper {
	if g.limiter == nil {
		return nil
	}
	return g.limiter.Transport()
}
```

(For test plugins built without a limiter, `transport()` returns nil → SDK default transport. Add `net/http` import.)

- [ ] **Step 6: Run the full suite**

Run: `go test ./... -race`
Expected: all green.

- [ ] **Step 7: Commit**

```bash
git add internal/datadog/ratelimit.go internal/datadog/ratelimit_test.go internal/plugin/plugin.go
git commit -m "feat: shared rate-limit transport (token bucket, header adaptation, concurrency cap)"
```

---

### Task 11: Coalescing + short-TTL cache + last-known-good degradation + observability

Implements §15.3 layers 3, 6, 7 at the `Run()` level: identical concurrent queries collapse via singleflight; results cache for a short TTL keyed by `ds.Key(cfg)+site`; on error within the deadline, serve the last-known-good value if within `maxStaleness`; record cache/limiter state in `Measurement.Metadata`. A throttle never yields `Failed`.

**Files:**
- Create: `internal/datadog/cache.go`
- Create: `internal/datadog/cache_test.go`
- Modify: `internal/plugin/plugin.go` (`InitPlugin` builds the shared cache; `Run` routes the source call through it)

**Interfaces:**
- Consumes: `golang.org/x/sync/singleflight`, `time`.
- Produces:
  - `type Cache struct { ... }`, `func NewCache(opts CacheOptions) *Cache` with `CacheOptions{ Enabled bool; TTL, MaxStaleness time.Duration }`.
  - `type Entry struct { Value interface{}; Meta map[string]string; StoredAt time.Time }`.
  - `func (c *Cache) Do(ctx, key string, fresh bool, fn func() (interface{}, map[string]string, error)) (value interface{}, meta map[string]string, source string, err error)` — `source` ∈ `fresh|coalesced|cached|last-good`. On `fn` error, falls back to last-good within `MaxStaleness`.
  - `now func() time.Time` field for deterministic tests.

- [ ] **Step 1: Write failing tests (coalescing, TTL hit, last-known-good)**

Create `internal/datadog/cache_test.go`:

```go
package datadog

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCache_CoalescesConcurrent(t *testing.T) {
	c := NewCache(CacheOptions{Enabled: true, TTL: time.Minute, MaxStaleness: time.Minute})
	var calls int64
	fn := func() (interface{}, map[string]string, error) {
		atomic.AddInt64(&calls, 1)
		time.Sleep(50 * time.Millisecond)
		return 1.0, nil, nil
	}
	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, _, _, err := c.Do(context.Background(), "k", false, fn)
			require.NoError(t, err)
		}()
	}
	wg.Wait()
	assert.Equal(t, int64(1), atomic.LoadInt64(&calls)) // collapsed
}

func TestCache_TTLHit(t *testing.T) {
	c := NewCache(CacheOptions{Enabled: true, TTL: time.Minute, MaxStaleness: time.Minute})
	var calls int64
	fn := func() (interface{}, map[string]string, error) { atomic.AddInt64(&calls, 1); return 2.0, nil, nil }
	_, _, s1, _ := c.Do(context.Background(), "k", false, fn)
	_, _, s2, _ := c.Do(context.Background(), "k", false, fn)
	assert.Equal(t, "fresh", s1)
	assert.Equal(t, "cached", s2)
	assert.Equal(t, int64(1), atomic.LoadInt64(&calls))
}

func TestCache_LastKnownGoodOnError(t *testing.T) {
	base := time.Now()
	c := NewCache(CacheOptions{Enabled: true, TTL: time.Millisecond, MaxStaleness: time.Hour})
	c.now = func() time.Time { return base }
	// seed a good value
	_, _, _, err := c.Do(context.Background(), "k", false, func() (interface{}, map[string]string, error) { return 3.0, nil, nil })
	require.NoError(t, err)
	// advance past TTL, now fn errors -> serve last-good
	c.now = func() time.Time { return base.Add(time.Minute) }
	v, _, src, err := c.Do(context.Background(), "k", false, func() (interface{}, map[string]string, error) {
		return nil, nil, errors.New("429 throttled")
	})
	require.NoError(t, err)
	assert.Equal(t, 3.0, v)
	assert.Equal(t, "last-good", src)
}

func TestCache_ErrorWithoutLastGoodPropagates(t *testing.T) {
	c := NewCache(CacheOptions{Enabled: true, TTL: time.Minute, MaxStaleness: time.Hour})
	_, _, _, err := c.Do(context.Background(), "k", false, func() (interface{}, map[string]string, error) {
		return nil, nil, errors.New("boom")
	})
	assert.Error(t, err)
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `go test ./internal/datadog/ -run TestCache -v`
Expected: FAIL — `NewCache` undefined.

- [ ] **Step 3: Implement `cache.go`**

Create `internal/datadog/cache.go`:

```go
package datadog

import (
	"context"
	"sync"
	"time"

	"golang.org/x/sync/singleflight"
)

type CacheOptions struct {
	Enabled      bool
	TTL          time.Duration
	MaxStaleness time.Duration
}

type Entry struct {
	Value    interface{}
	Meta     map[string]string
	StoredAt time.Time
}

type Cache struct {
	opts CacheOptions
	now  func() time.Time

	mu      sync.Mutex
	entries map[string]Entry
	group   singleflight.Group
}

func NewCache(opts CacheOptions) *Cache {
	return &Cache{opts: opts, now: time.Now, entries: map[string]Entry{}}
}

type doResult struct {
	value interface{}
	meta  map[string]string
}

func (c *Cache) get(key string) (Entry, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	e, ok := c.entries[key]
	return e, ok
}

func (c *Cache) put(key string, e Entry) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.entries[key] = e
}

// Do returns a value for key, coalescing concurrent calls and caching results.
// source is one of fresh|coalesced|cached|last-good.
func (c *Cache) Do(ctx context.Context, key string, fresh bool, fn func() (interface{}, map[string]string, error)) (interface{}, map[string]string, string, error) {
	if !c.opts.Enabled {
		v, meta, err := fn()
		return v, meta, "fresh", err
	}

	// fresh TTL hit
	if !fresh {
		if e, ok := c.get(key); ok && c.now().Sub(e.StoredAt) <= c.opts.TTL {
			return e.Value, e.Meta, "cached", nil
		}
	}

	res, err, shared := c.group.Do(key, func() (interface{}, error) {
		v, meta, e := fn()
		if e != nil {
			return nil, e
		}
		c.put(key, Entry{Value: v, Meta: meta, StoredAt: c.now()})
		return doResult{value: v, meta: meta}, nil
	})

	if err != nil {
		// last-known-good degradation
		if e, ok := c.get(key); ok && c.now().Sub(e.StoredAt) <= c.opts.MaxStaleness {
			return e.Value, e.Meta, "last-good", nil
		}
		return nil, nil, "", err
	}
	dr := res.(doResult)
	src := "fresh"
	if shared {
		src = "coalesced"
	}
	return dr.value, dr.meta, src, nil
}
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `go test ./internal/datadog/ -run TestCache -race -v`
Expected: PASS (coalescing collapses to 1; TTL hit; last-good on error; error propagates without last-good).

- [ ] **Step 5: Route the source call through the cache in `Run()` + add observability metadata**

Edit `internal/plugin/plugin.go`:
- Add field `cache *ddinternal.Cache` to `RpcPlugin`.
- In `InitPlugin`:

```go
g.cache = ddinternal.NewCache(ddinternal.CacheOptions{
	Enabled: true, TTL: 30 * time.Second, MaxStaleness: 5 * time.Minute,
})
```

- Replace the direct `ds.Query(...)` call in `Run` with a cache-wrapped call:

```go
key := ds.Key(cfg) + "|site=" + cfg.Site
fresh := cfg.Cache != nil && cfg.Cache.Enabled != nil && !*cfg.Cache.Enabled
value, meta, src, err := g.cache.Do(ctx, key, fresh, func() (interface{}, map[string]string, error) {
	r, e := ds.Query(ctx, client, cfg)
	return r.Value, r.Metadata, e
})
if err != nil {
	return finish(metricutil.MarkMeasurementError(m, err))
}

phase, err := evaluate.EvaluateResult(value, metric, g.LogCtx)
if err != nil {
	return finish(metricutil.MarkMeasurementError(m, err))
}
m.Phase = phase
m.Value = stringify(value)
m.Metadata = mergeMeta(meta, map[string]string{"cache": src})
return finish(m)
```

Add `mergeMeta` helper:

```go
func mergeMeta(base, extra map[string]string) map[string]string {
	out := map[string]string{}
	for k, v := range base {
		out[k] = v
	}
	for k, v := range extra {
		out[k] = v
	}
	return out
}
```

(For test plugins built without a cache, guard: if `g.cache == nil`, call `ds.Query` directly. Update `newTestPlugin` in `helpers_test.go` to set `cache: ddinternal.NewCache(ddinternal.CacheOptions{Enabled: false})` so the wrapper is exercised but pass-through.)

- [ ] **Step 6: Add a `Run()`-level last-known-good test (throttle never Failed)**

Append to `internal/plugin/run_test.go`:

```go
func TestRun_ThrottleServesLastGoodNeverFailed(t *testing.T) {
	// First call succeeds and seeds the cache; second call's source errors,
	// but the cached value is served => phase is Successful, never Failed.
	var calls int64
	src := &flakySource{firstValue: 0.99, calls: &calls}
	g := newTestPlugin(t, src)
	g.cache = ddinternal.NewCache(ddinternal.CacheOptions{Enabled: true, TTL: time.Nanosecond, MaxStaleness: time.Hour})

	m := metricWith(t, `{"metrics":{"query":"avg:cpu{*}"}}`)
	m.SuccessCondition = "result >= 0.95"
	m.FailureCondition = "result < 0.95"

	out1 := g.Run(&v1alpha1.AnalysisRun{}, m)
	require.Equal(t, v1alpha1.AnalysisPhaseSuccessful, out1.Phase)

	time.Sleep(time.Millisecond) // exceed TTL
	out2 := g.Run(&v1alpha1.AnalysisRun{}, m)
	require.NotEqual(t, v1alpha1.AnalysisPhaseFailed, out2.Phase)
	assert.Equal(t, v1alpha1.AnalysisPhaseSuccessful, out2.Phase) // last-good
	assert.Equal(t, "last-good", out2.Metadata["cache"])
}
```

Add `flakySource` to `helpers_test.go`:

```go
import "sync/atomic"

type flakySource struct {
	firstValue interface{}
	calls      *int64
}

func (f *flakySource) Query(_ context.Context, _ *datadog.APIClient, _ *config.Config) (datasource.Result, error) {
	n := atomic.AddInt64(f.calls, 1)
	if n == 1 {
		return datasource.Result{Value: f.firstValue}, nil
	}
	return datasource.Result{}, fmt.Errorf("429 throttled")
}
func (f *flakySource) Key(_ *config.Config) string { return "flaky" }
```

(Add `fmt`, `time`, `sync/atomic` imports as needed.)

Run: `go test ./internal/plugin/ -run TestRun_Throttle -race -v`
Expected: PASS — second call returns Successful via last-good, never Failed.

- [ ] **Step 7: Run the entire suite**

Run: `go test ./... -race -count=1`
Expected: all green.

- [ ] **Step 8: Commit**

```bash
git add internal/datadog/cache.go internal/datadog/cache_test.go internal/plugin/plugin.go internal/plugin/helpers_test.go internal/plugin/run_test.go
git commit -m "feat: coalescing, TTL cache, last-known-good degradation, and observability metadata"
```

---

### Task 12: Docs, examples, Dockerfile, and migration guide

Ships the operator-facing artifacts (§11, §14, §10): a Dockerfile for the initContainer install method, install/config docs, runnable AnalysisTemplate examples for all three sources, and the `web`-provider → plugin migration guide.

**Files:**
- Create: `Dockerfile`
- Create: `docs/install.md`
- Create: `docs/configuration.md`
- Create: `docs/migration-web-provider.md`
- Create: `examples/metrics-analysistemplate.yaml`
- Create: `examples/monitor-analysistemplate.yaml`
- Create: `examples/slo-analysistemplate.yaml`
- Create: `examples/multi-source-analysistemplate.yaml`
- Modify: `README.md`

**Interfaces:**
- Consumes: the finished binary and config schema.
- Produces: documentation only; no Go code.

- [ ] **Step 1: Create the `Dockerfile` (build + initContainer copy artifact)**

```dockerfile
FROM golang:1.24 AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -o /out/rollouts-plugin-metric-datadog .

# Minimal image whose only job is to expose the binary for an initContainer copy.
FROM alpine:3.20
COPY --from=build /out/rollouts-plugin-metric-datadog /plugin/rollouts-plugin-metric-datadog
```

- [ ] **Step 2: Write `examples/monitor-analysistemplate.yaml`** (the migration target)

```yaml
apiVersion: argoproj.io/v1alpha1
kind: AnalysisTemplate
metadata:
  name: datadog-monitors
spec:
  metrics:
    - name: monitors
      interval: 1m
      failureLimit: 2
      provider:
        plugin:
          mubarak-j/rollouts-plugin-metric-datadog:
            tags: ["service:my-cool-service", "env:production"]
            monitor:
              query: "muted:false"
      failureCondition: "any(result.counts.status, {.name == 'Alert' && .count > 0})"
      successCondition: "result.counts.status == nil || any(result.counts.status, {.name != 'Alert'})"
```

- [ ] **Step 3: Write `examples/slo-analysistemplate.yaml`**

```yaml
apiVersion: argoproj.io/v1alpha1
kind: AnalysisTemplate
metadata:
  name: datadog-slos
spec:
  metrics:
    - name: slos
      interval: 1m
      failureLimit: 2
      provider:
        plugin:
          mubarak-j/rollouts-plugin-metric-datadog:
            tags: ["service:my-cool-service", "env:production"]
            slo: {}
      failureCondition: "any(result.slos, {any(.overall_status, {.state == 'breached'})})"
      successCondition: "all(result.slos, {all(.overall_status, {.state != 'breached'})})"
```

- [ ] **Step 4: Write `examples/metrics-analysistemplate.yaml`**

```yaml
apiVersion: argoproj.io/v1alpha1
kind: AnalysisTemplate
metadata:
  name: datadog-error-rate
spec:
  metrics:
    - name: error-rate
      interval: 1m
      provider:
        plugin:
          mubarak-j/rollouts-plugin-metric-datadog:
            tags: ["service:my-cool-service", "env:production"]
            metrics:
              apiVersion: v2
              queries:
                a: "sum:trace.http.request.hits{*}.as_count()"
                b: "sum:trace.http.request.errors{*}.as_count()"
              formula: "(a - b) / a"
      successCondition: "result >= 0.95"
```

- [ ] **Step 5: Write `examples/multi-source-analysistemplate.yaml`** (§10 OR-of-failures)

```yaml
apiVersion: argoproj.io/v1alpha1
kind: AnalysisTemplate
metadata:
  name: datadog-monitors-and-slos
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

- [ ] **Step 6: Write `docs/install.md`**

Document both install methods from §4.3/§14: (1) initContainer copying the binary from the image built in Step 1 into a shared `plugin-bin` volume at `plugin-bin/mubarak-j/rollouts-plugin-metric-datadog`, with the `argo-rollouts-config` ConfigMap `metricProviderPlugins` `file://` entry; (2) `https://` release-download location with `sha256`. Include the RBAC note from §8.2 (default install needs no new RBAC; namespace-narrowed installs must grant cross-namespace `secrets get` for `namespaced: true` refs). Include the ConfigMap snippet:

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: argo-rollouts-config
data:
  metricProviderPlugins: |-
    - name: "mubarak-j/rollouts-plugin-metric-datadog"
      location: "file://./plugin-bin/mubarak-j/rollouts-plugin-metric-datadog"
```

- [ ] **Step 7: Write `docs/configuration.md`**

Document the full config schema from §5 (shared connection fields, `tags`, the three source blocks, `rateLimit`/`cache`/`retry` with defaults from §15.4), the credential resolution order (§8.1), site values (§8.3), and time-window semantics (§5.3). Include the result-shape table from §6.

- [ ] **Step 8: Write `docs/migration-web-provider.md`**

Show the before (`web` provider with manual URI-encoded tags and `api-key`/`app-key` in `args`/headers) → after (this plugin with structured `tags` and in-process credential resolution), reusing the same `failureCondition`/`successCondition`. Point at the `datadog` secret reuse (§8.1 step 3) so the existing secret drops in unchanged.

- [ ] **Step 9: Update `README.md`** with a one-paragraph overview, the supported sources (Phase 1: metrics/monitor/slo), the supported argo-rollouts version range (built against v1.9.0, §4.4), and links to the docs/ pages.

- [ ] **Step 10: Verify examples parse and the image builds**

Run: `docker build -t rollouts-plugin-metric-datadog:dev . && for f in examples/*.yaml; do python3 -c "import yaml,sys; yaml.safe_load(open('$f'))"; done`
Expected: image builds; every example is valid YAML.

- [ ] **Step 11: Commit**

```bash
git add Dockerfile docs/ examples/ README.md
git commit -m "docs: install, configuration, web-provider migration, and examples"
```

---

## Self-Review

**1. Spec coverage** (each design section → task):

| Spec § | Covered by |
|---|---|
| §4.1 DataSource strategy | Task 6 |
| §4.2 SDK calls (metrics/monitor/slo) | Tasks 7, 8, 9 |
| §4.3 plugin mechanism / handshake / install | Task 1 (handshake), Task 12 (install/ConfigMap) |
| §4.4 version compatibility | Task 0 (`go.mod` pin), Task 12 (README version range) |
| §5.1 top-level config + validation rules | Task 2 |
| §5.2 per-source blocks (metrics/monitor/slo) | Tasks 2 (schema), 7, 8, 9 |
| §5.3 time windows (`interval`, ms vs s) | Task 6 (`WindowFrom`/`TimeWindow`), consumed in 7/9 |
| §6 result/verdict model + struct→map | Task 6 (`EvaluateResult`), Tasks 8/9 (`structToMap`), Task 8 (migration-parity) |
| §7 tag filtering (all sub-cases) | Task 5 (used by 7/8/9) |
| §8.1 credential resolution order | Task 3 |
| §8.2 RBAC | Task 12 (docs) |
| §8.3 region/site/address | Task 4 |
| §9 error handling + empty results | Task 6 (`MarkMeasurementError`, nil scalar), Task 11 (degradation) |
| §10 multi-source composition | Task 12 (`multi-source` example) |
| §11 repository layout | Tasks 0–12 (file structure) |
| §12 testing strategy | every task (TDD), Task 8 (migration-parity) |
| §13 phasing | this plan = Phase 1; apm/logs deferred to a Phase 2 plan |
| §14 distribution + migration | Task 12 (Dockerfile, install.md, migration doc) |
| §15.3 layers 1/2/4/5 | Task 10 (limiter transport) + Task 4 (SDK retry) |
| §15.3 layers 3/6/7 | Task 11 (cache, last-good, observability) |
| §15.4 config surface | Task 2 (schema), Tasks 10/11 (consumption) |
| §15.6 rate-limit testing | Tasks 10, 11 |

Phase 2 sources (apm, logs — §5.2 apm/logs, §13 Phase 2) are intentionally **out of scope** for this plan and become a separate plan; the `DataSource` interface (Task 6) and `AppendQueryFilter` (Task 5) are built so they slot in with no core churn.

**2. Placeholder scan:** No `TODO`/`TBD`/"add error handling"/"similar to Task N" left. Two implementer-verification notes are deliberate (not placeholders): the SLO `Facets` field name (Task 9 Step 3) and the `MetricsAggregator` enum token form (Task 7) — both have a stated fallback. The cross-package config move (Task 6 Step 1) is called out explicitly so it isn't a surprise.

**3. Type consistency:** `Config`/`ConfigKey` live in `internal/config` (Task 6 Step 1) and are referenced consistently in Tasks 7–11. `DataSource.Query` returns `Result{Value, Metadata}` everywhere; `Result.Value` is `float64` for scalar sources (metrics, slo by-id) and `map[string]interface{}` for structured sources (monitor, slo search). `Credentials`/`ClientOptions`/`SecretRefInput`/`Resolver` signatures match between Tasks 3, 4, and 6. `Limiter.Transport()` (Task 10) and `Cache.Do(...)` (Task 11) signatures match their `Run()` call sites. `WindowFrom(now, interval, def)` and `FromUnix{Millis,Seconds}` are consistent between Task 6 and Tasks 7/9.

**Known follow-ups for the implementer** (do not block Phase 1, but track):
- The static per-bucket ceilings from `rateLimit.buckets` (§15.4) and per-metric `rateLimit.enabled`/`cache.ttl` overrides are parsed in Task 2 but wired with conservative defaults in Tasks 10/11; full per-metric override plumbing can be a fast follow if needed.
- `GetMetadata` (Task 6) returns source+tags; enriching it with the resolved query is straightforward once sources expose it via `Key`/metadata.

**4. Second-pass correctness review** (independent fresh-eyes pass, applied to this plan):
- **Metrics empty-formula bug (Task 7):** a single named query with no `formula` is valid config but previously produced an empty Datadog `Formula`. Fixed: the source now synthesizes the formula from the sole query name, with regression test `TestMetrics_V2SingleNamedQueryNoFormula`.
- **Limiter adaptation was inert (Task 10):** the transport observed `X-RateLimit-Name` buckets but always gated on `_default`, so header-adaptive throttling (§15.3 layer 2) never took effect. Fixed: `RoundTrip` now gates on the bucket learned per `method+path` (`LearnedBucket`), with test `TestLimiter_LearnsBucketPerPathAndGatesOnIt`. (Also removed a dead `next` field on `Limiter`.)
- **Monitor migration-parity premise verified (Task 8):** confirmed against the SDK at v2.62.0 that `MonitorGroupSearchResponseCounts.Status` is `[]MonitorSearchCountItem` (json `status`) and each item is `{Count *int64 \`json:"count"\`, Name interface{} \`json:"name"\`}` — so `result.counts.status` with `.name`/`.count` survives the struct→map conversion. No change; the parity test guards against future drift.
- **Test/file hygiene (Tasks 6–7):** Task 6 Step 5 is now a complete `plugin.go` (no fragile "keep the old methods" merge); `metrics_test.go`/`helpers_test.go` imports are correct (`io` added, unused `encoding/json`/`time`/`io` and throwaway `var _` lines removed).

> **2026-06-30 review — both open questions resolved** (see Applied Review Fixes #1 and #7): cross-rollout cache collisions are handled by a config-correctness guard rather than key identity (key stays query+tags+window+site); last-known-good degradation (§15.3 layer 6) is deferred to Phase 2, with limiter + coalescing/cache + SDK retry (layers 1–5) staying in Phase 1.
