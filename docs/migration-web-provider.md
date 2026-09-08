# Migrating from the `web` Provider to This Plugin

The Argo Rollouts built-in `web` metric provider can query Datadog by constructing the API URL manually, passing credentials as HTTP headers, and URI-encoding tag filters into the query string. This plugin replaces that pattern with structured config, in-process credential resolution, and automatic tag merging — while preserving the same `failureCondition`/`successCondition` expressions.

---

## Before: `web` provider

```yaml
apiVersion: argoproj.io/v1alpha1
kind: AnalysisTemplate
metadata:
  name: datadog-error-rate-web
spec:
  args:
    - name: api-key
      valueFrom:
        secretKeyRef:
          name: datadog
          key: api-key
    - name: app-key
      valueFrom:
        secretKeyRef:
          name: datadog
          key: app-key
  metrics:
    - name: error-rate
      interval: 1m
      provider:
        web:
          url: >-
            https://api.datadoghq.com/api/v1/query?from={{- (now | unixEpoch | subtract 300) -}}&to={{- now | unixEpoch -}}&query=sum%3Atrace.http.request.errors%7Bservice%3Amy-cool-service%2Cenv%3Aproduction%7D.as_count()
          headers:
            - key: DD-API-KEY
              value: "{{args.api-key}}"
            - key: DD-APPLICATION-KEY
              value: "{{args.app-key}}"
          jsonPath: "{$.series[0].pointlist[-1][1]}"
      successCondition: "result >= 0.95"
```

Pain points:
- Tags must be URI-encoded into the URL by hand.
- Credentials are passed via `args` referencing a SecretKeyRef — they appear in the AnalysisRun spec.
- The time window (`from`/`to`) is computed with template functions and is fragile.
- No rate limiting or caching across concurrent rollout waves.

---

## After: this plugin

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
              apiVersion: v1
              query: "sum:trace.http.request.errors{*}.as_count()"
      successCondition: "result >= 0.95"
```

What changed:
- **No credential args.** The plugin reads credentials from the `datadog` Secret in the controller namespace automatically (resolution step 3). The existing `datadog` Secret (`api-key` / `app-key` keys) drops in unchanged — no new Secret, no new `args`.
- **Structured tags.** `tags` is a YAML list; the plugin merges them into the query's `{...}` scope brace. No URI encoding.
- **Time window computed in-process.** The plugin computes `[now-5m, now]` automatically from `metrics.interval` (default 5m). No template arithmetic.
- **Rate limiting and caching** are on by default across all concurrent measurements.
- **Same `successCondition`.** The scalar result shape is identical to what `jsonPath` extracts from the v1 API.

---

## Monitor migration

Before (web provider calling the search endpoint):

```yaml
provider:
  web:
    url: "https://api.datadoghq.com/api/v1/monitor/groups/search?query=service%3Amy-cool-service+env%3Aproduction+muted%3Afalse"
    headers:
      - key: DD-API-KEY
        value: "{{args.api-key}}"
      - key: DD-APPLICATION-KEY
        value: "{{args.app-key}}"
    jsonPath: "{$.counts.status}"
failureCondition: "any(result, {.name == 'Alert' && .count > 0})"
```

After:

```yaml
provider:
  plugin:
    mubarak-j/rollouts-plugin-metric-datadog:
      tags: ["service:my-cool-service", "env:production"]
      monitor:
        query: "muted:false"
failureCondition: "any(result.counts.status, {.name == 'Alert' && .count > 0})"
```

Note: the plugin returns the full `SearchMonitorGroupsResponse` as `result`, so the path becomes `result.counts.status` (not bare `result`). Update the condition accordingly.

---

## Credential reuse

The plugin's resolution step 3 reads a Secret literally named `datadog` from the controller namespace, expecting `api-key` and `app-key` keys. This matches the standard Datadog Secret that the `web` provider previously referenced via `secretKeyRef`. No Secret changes are needed; remove the `args` block from the AnalysisTemplate and let the plugin resolve credentials automatically.

If your cluster uses a different Secret name, set `secretRef.name` explicitly:

```yaml
mubarak-j/rollouts-plugin-metric-datadog:
  secretRef:
    name: my-datadog-secret
  tags: ["service:my-cool-service", "env:production"]
  metrics:
    query: "..."
```

See [configuration.md](configuration.md#credential-resolution-order) for the full resolution order and the secretRef escalation note.
