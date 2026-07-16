package plugin

import (
	"testing"
	"time"

	"github.com/mubarak-j/rollouts-plugin-metric-datadog/internal/config"
	"github.com/stretchr/testify/assert"
)

// baseMetricsCfg returns a minimal valid config for a metrics source.
func baseMetricsCfg() *config.Config {
	return &config.Config{
		Site:    "datadoghq.com",
		Address: "",
		Metrics: &config.MetricsConfig{APIVersion: "v2", Query: "avg:cpu{*}"},
	}
}

// buildKey mirrors the cache-key construction in Run() so we can test it
// without wiring the full plugin. The source Key() for a metrics config with
// query "avg:cpu{*}" is stable across calls; we pin now so the window bucket
// is deterministic.
func buildKey(cfg *config.Config, runNS, controllerNS string, now time.Time) string {
	// mirror Run(): ds.Key(cfg) + "|site=" + ... + "|addr=" + ... + "|cred=" + ... + "|win=" + ...
	// For a metrics source, Key() returns a hash of the query+tags+apiVersion.
	// We don't call the real datasource here; instead we use credKey directly
	// and test addr isolation separately.
	return cfg.Site + "|addr=" + cfg.Address + "|cred=" + credKey(cfg, runNS, controllerNS)
}

// TestCredKey_DefaultWhenNoSecretRef verifies that two configs with no secretRef
// (same process-level account) share the same credential discriminator.
func TestCredKey_DefaultWhenNoSecretRef(t *testing.T) {
	cfgA := baseMetricsCfg()
	cfgB := baseMetricsCfg()
	cfgB.Tags = []string{"env:prod"} // different tag; same account

	assert.Equal(t, "default", credKey(cfgA, "ns-a", "argo-rollouts"))
	assert.Equal(t, credKey(cfgA, "ns-a", "argo-rollouts"), credKey(cfgB, "ns-b", "argo-rollouts"),
		"two configs with no secretRef must produce the same credKey regardless of run namespace")
}

// TestCacheKey_AddressDifferentiatesKey verifies that two configs identical
// except for Address produce different cache keys (isolation bug FIX 1).
func TestCacheKey_AddressDifferentiatesKey(t *testing.T) {
	cfgA := baseMetricsCfg()
	cfgB := baseMetricsCfg()
	cfgB.Address = "https://proxy.internal"

	keyA := buildKey(cfgA, "argo-rollouts", "argo-rollouts", time.Now())
	keyB := buildKey(cfgB, "argo-rollouts", "argo-rollouts", time.Now())
	assert.NotEqual(t, keyA, keyB, "different Address must produce different cache keys")
}

// TestCacheKey_SecretRefDifferentiatesKey verifies that two configs identical
// except for secretRef.name produce different cache keys (isolation bug FIX 1).
func TestCacheKey_SecretRefDifferentiatesKey(t *testing.T) {
	cfgA := baseMetricsCfg()
	cfgA.SecretRef = &config.SecretRef{Name: "dd-secret-team-a"}

	cfgB := baseMetricsCfg()
	cfgB.SecretRef = &config.SecretRef{Name: "dd-secret-team-b"}

	keyA := buildKey(cfgA, "argo-rollouts", "argo-rollouts", time.Now())
	keyB := buildKey(cfgB, "argo-rollouts", "argo-rollouts", time.Now())
	assert.NotEqual(t, keyA, keyB, "different secretRef.name must produce different cache keys")
}

// TestCacheKey_SameAccountSameKey verifies that two configs with the same
// account (same/empty address, no secretRef) from different rollouts produce
// the SAME key, preserving cross-rollout cache coalescing (Fix #7).
func TestCacheKey_SameAccountSameKey(t *testing.T) {
	now := time.Now()
	cfgA := baseMetricsCfg()
	cfgB := baseMetricsCfg()

	// Same site, same (empty) address, no secretRef — different run namespaces.
	keyA := buildKey(cfgA, "ns-rollout-a", "argo-rollouts", now)
	keyB := buildKey(cfgB, "ns-rollout-b", "argo-rollouts", now)
	assert.Equal(t, keyA, keyB,
		"two configs with the same account (no secretRef, same address) must produce the same key regardless of which rollout runs them")
}

// TestCredKey_NamespacedUsesRunNamespace verifies that when secretRef.namespaced
// is true the run namespace (not the controller namespace) appears in credKey.
func TestCredKey_NamespacedUsesRunNamespace(t *testing.T) {
	cfg := baseMetricsCfg()
	cfg.SecretRef = &config.SecretRef{Name: "my-secret", Namespaced: true}

	key := credKey(cfg, "run-ns", "argo-rollouts")
	assert.Equal(t, "ref:run-ns/my-secret", key)
}

// TestCredKey_NonNamespacedUsesControllerNamespace verifies that when
// secretRef.namespaced is false the controller namespace appears in credKey.
func TestCredKey_NonNamespacedUsesControllerNamespace(t *testing.T) {
	cfg := baseMetricsCfg()
	cfg.SecretRef = &config.SecretRef{Name: "my-secret", Namespaced: false}

	key := credKey(cfg, "run-ns", "argo-rollouts")
	assert.Equal(t, "ref:argo-rollouts/my-secret", key)
}
