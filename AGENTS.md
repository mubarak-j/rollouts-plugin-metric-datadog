# AGENTS.md

Rules and instructions for AI coding agents (Claude Code, Cursor, Copilot, etc.).
Symlinked from `CLAUDE.md` for tools that read that name.

**Tradeoff:** These guidelines bias toward caution over speed. For trivial tasks, use judgment.

---

## 1. Think Before Coding

**Don't assume. Don't hide confusion. Surface tradeoffs.**

Before implementing:
- State your assumptions explicitly. If uncertain, ask.
- If multiple interpretations exist, present them — don't pick silently.
- If a simpler approach exists, say so. Push back when warranted.
- If something is unclear, stop. Name what's confusing. Ask.

## 2. Simplicity First

**Minimum code that solves the problem. Nothing speculative.**

- No features beyond what was asked.
- No abstractions for single-use code.
- No "flexibility" or "configurability" that wasn't requested.
- No error handling for impossible scenarios.
- If you write 200 lines and it could be 50, rewrite it.

Ask yourself: "Would a senior engineer say this is overcomplicated?" If yes, simplify.

## 3. Surgical Changes

**Touch only what you must. Clean up only your own mess.**

When editing existing code:
- Don't "improve" adjacent code, comments, or formatting.
- Don't refactor things that aren't broken.
- Match existing style, even if you'd do it differently.
- If you notice unrelated dead code, mention it — don't delete it.

When your changes create orphans:
- Remove imports/variables/functions that YOUR changes made unused.
- Don't remove pre-existing dead code unless asked.

The test: every changed line should trace directly to the user's request.

## 4. Goal-Driven Execution

**Define success criteria. Loop until verified.**

Transform tasks into verifiable goals:
- "Add validation" → "Write tests for invalid inputs, then make them pass"
- "Fix the bug" → "Write a test that reproduces it, then make it pass"
- "Refactor X" → "Ensure tests pass before and after"

For multi-step tasks, state a brief plan:
```
1. [Step] → verify: [check]
2. [Step] → verify: [check]
3. [Step] → verify: [check]
```

Strong success criteria let you loop independently. Weak criteria ("make it work") require constant clarification.

---

## Project-specific rules

### Commands

```bash
make build      # CGO_ENABLED=0 build for host platform → dist/rollouts-plugin-metric-datadog
make release    # cross-compile: linux/darwin × amd64/arm64 → dist/
make test       # go test ./... -race -count=1
make vet        # go vet ./...
test -z "$(gofmt -l .)"   # formatting gate (also enforced by CI)

# single package / single test
go test ./internal/datasource -run TestMetrics -race -count=1

docker build -t rollouts-plugin-metric-datadog .
```

### Never run `go mod tidy` casually

`go mod tidy` strips `github.com/DataDog/zstd` (a cgo-only transitive dep), which breaks the `-race` build. If you must tidy, restore it immediately:

```bash
go get github.com/DataDog/zstd@v1.5.2
```

Dependabot ignores this dep for the same reason (`.github/dependabot.yml`).

### Errors must be `MarkMeasurementError`, never `Failed`

Any error in `Run()` must produce an `Error` measurement phase, not `Failed`. `failureLimit` defaults to 0 — mapping a Datadog throttle to `Failed` aborts a rollout on infra flakiness instead of a real regression.

### Test guard: never remove the `DD_*` env check

`Run`-level tests skip or fail-fast when `DD_API_KEY` / `DD_APP_KEY` are set in the environment. Keep that guard when adding cases.

### Testing conventions

- Table-test data sources against `net/http/httptest` stubs. Assert request shape and response parsing — not live Datadog.
- `RpcPlugin` exposes `selectSource` and `newClient` indirections; use them to inject fakes without a real kube client or Datadog endpoint.

---

## Reference docs

- [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) — full design spec (architecture, result model, rate-limiting, phasing)
- [docs/configuration.md](docs/configuration.md) — config schema, credential resolution, site values
- [docs/install.md](docs/install.md) — installation via initContainer or binary download
- [CONTRIBUTING.md](CONTRIBUTING.md) — how to add a data source, run tests, submit a PR

---

**These guidelines are working if:** fewer unnecessary changes in diffs, fewer rewrites due to overcomplication, and clarifying questions come before implementation rather than after mistakes.
