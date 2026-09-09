# Installing the Datadog Metric Plugin

The plugin binary must be present on every node where the `argo-rollouts` controller pod runs. Two methods are supported: initContainer copy (recommended for self-hosted clusters) and direct URL download.

---

## Method 1: initContainer copy

This method uses an initContainer to copy the binary from the plugin image into a shared `emptyDir` volume before the controller starts. It requires no internet access at controller startup.

### 1. Patch the controller Deployment

Add an initContainer and a shared volume to the `argo-rollouts` Deployment:

```yaml
spec:
  template:
    spec:
      volumes:
        - name: plugin-bin
          emptyDir: {}
      initContainers:
        - name: copy-datadog-plugin
          image: ghcr.io/mubarak-j/rollouts-plugin-metric-datadog:latest
          securityContext:
            runAsNonRoot: true
            runAsUser: 999
          command:
            - cp
            - /plugin/rollouts-plugin-metric-datadog
            - /home/argo-rollouts/plugin-bin/datadog-plugin-src
          volumeMounts:
            - name: plugin-bin
              mountPath: /home/argo-rollouts/plugin-bin
      containers:
        - name: argo-rollouts
          # ... existing container spec ...
          volumeMounts:
            - name: plugin-bin
              mountPath: /home/argo-rollouts/plugin-bin
```

Three details matter here:

- **`mountPath` must be `/home/argo-rollouts/plugin-bin`.** The controller's working directory is `/home/argo-rollouts`, so a `file://./plugin-bin/...` location resolves relative to that, not to `/`.
- **The copy destination must not be the location the controller installs to.** Argo Rollouts installs every plugin to `<workdir>/plugin-bin/<plugin-name>` — here `/home/argo-rollouts/plugin-bin/mubarak-j/rollouts-plugin-metric-datadog`. If the initContainer writes the binary to that same path and the ConfigMap points `location` at it, the controller copies the file onto itself, truncating it to 0 bytes. The controller then fails with `fork/exec ...: exec format error`. Copy to a distinct name (`datadog-plugin-src`) and let the controller install from it.
- **`runAsUser: 999`** matches the uid of the `argo-rollouts` container, so the controller can `chmod` the binary it installs. A different uid produces `failed to set file permissions of plugin: ... operation not permitted`.

The initContainer writes `/home/argo-rollouts/plugin-bin/datadog-plugin-src`; the controller installs from there to `/home/argo-rollouts/plugin-bin/mubarak-j/rollouts-plugin-metric-datadog`.

### 2. Configure the plugin in the ConfigMap

Update (or create) the `argo-rollouts-config` ConfigMap in the same namespace as the controller:

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: argo-rollouts-config
data:
  metricProviderPlugins: |-
    - name: "mubarak-j/rollouts-plugin-metric-datadog"
      location: "file://./plugin-bin/datadog-plugin-src"
```

Restart the controller after applying the ConfigMap.

Verify the handshake in the controller log before running an AnalysisRun:

```
Copied plugin from /home/argo-rollouts/plugin-bin/datadog-plugin-src to /home/argo-rollouts/plugin-bin/mubarak-j/rollouts-plugin-metric-datadog
plugin: plugin started: path=/home/argo-rollouts/plugin-bin/mubarak-j/rollouts-plugin-metric-datadog pid=15
plugin: using plugin: version=1
```

If the two paths in `Copied plugin from ... to ...` are identical, the `location` collides with the install path — see the notes above.

---

## Method 2: URL download

Argo Rollouts can download the plugin binary directly from a GitHub release at controller startup. Add the `sha256` checksum so the controller verifies the binary before running it.

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: argo-rollouts-config
data:
  metricProviderPlugins: |-
    - name: "mubarak-j/rollouts-plugin-metric-datadog"
      location: "https://github.com/mubarak-j/rollouts-plugin-metric-datadog/releases/download/v0.1.0-alpha.1/rollouts-plugin-metric-datadog-linux-amd64"
      sha256: "524637e552329d0e161157b5eb206a8d29cc98b0a4ba97171bb05400a02b46ec"
```

**SHA256 checksums for v0.1.0-alpha.1:**

| Binary | SHA256 |
|---|---|
| `rollouts-plugin-metric-datadog-linux-amd64` | `524637e552329d0e161157b5eb206a8d29cc98b0a4ba97171bb05400a02b46ec` |
| `rollouts-plugin-metric-datadog-linux-arm64` | `9b71b763e15fcaea052a38576f6bb914945d3edb40cb8d8a61566ca625ad0105` |
| `rollouts-plugin-metric-datadog-darwin-amd64` | `b8a8fe2d4d8a34660f9b56baf5f35f59b115891c8131f407b5400ad5c060f039` |
| `rollouts-plugin-metric-datadog-darwin-arm64` | `48fd5d1155397cea36236e1ff867cd02a4ca9e1bc42e0f9877539ee975cdd9bd` |

Update the version tag and `sha256` when upgrading. The [releases page](https://github.com/mubarak-j/rollouts-plugin-metric-datadog/releases) lists all available versions.

---

## RBAC

### Default install (controller-namespace secret)

The default credential resolution (step 3 in [configuration.md](configuration.md#credential-resolution-order)) reads a Secret named `datadog` from the controller's own namespace. The standard `argo-rollouts` ClusterRole already grants `secrets/get` within its namespace, so **no additional RBAC is required** for the default install.

### Cross-namespace secretRef (`namespaced: true`)

When a metric uses `secretRef.namespaced: true`, the plugin reads the named Secret from the AnalysisRun's namespace via the controller ServiceAccount. The controller must be granted `get` on Secrets in those namespaces.

**Security note (Fix #3 — secretRef escalation):** Because `secretRef.name` is unrestricted and `namespaced: true` causes the plugin to read the named Secret from the AnalysisRun's namespace via the controller ServiceAccount, **anyone who can author AnalysisTemplates or AnalysisRuns in a namespace can instruct the controller to read any Secret in that namespace and forward its `api-key`/`app-key` to Datadog.** Operators must treat "can author AnalysisTemplates in namespace N" as equivalent to "can read all Secrets in N". To restrict this, enforce eligible Secret names by naming convention, label selector, or an admission policy (e.g. OPA/Kyverno) that rejects unknown `secretRef.name` values.

Example RBAC for cross-namespace reads:

```yaml
apiVersion: rbac.authorization.k8s.io/v1
kind: Role
metadata:
  name: argo-rollouts-secret-reader
  namespace: my-app-namespace
rules:
  - apiGroups: [""]
    resources: ["secrets"]
    verbs: ["get"]
---
apiVersion: rbac.authorization.k8s.io/v1
kind: RoleBinding
metadata:
  name: argo-rollouts-secret-reader
  namespace: my-app-namespace
subjects:
  - kind: ServiceAccount
    name: argo-rollouts
    namespace: argo-rollouts
roleRef:
  kind: Role
  apiGroup: rbac.authorization.k8s.io
  name: argo-rollouts-secret-reader
```
