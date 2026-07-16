package config

import (
	"encoding/json"
	"testing"

	"github.com/argoproj/argo-rollouts/pkg/apis/rollouts/v1alpha1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func metricWith(t *testing.T, raw string) v1alpha1.Metric {
	t.Helper()
	return v1alpha1.Metric{
		Provider: v1alpha1.MetricProvider{
			Plugin: map[string]json.RawMessage{ConfigKey: json.RawMessage(raw)},
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

func TestWarnings(t *testing.T) {
	tests := []struct {
		name     string
		raw      string
		wantWarn int
	}{
		{
			name:     "monitor search with env tag only",
			raw:      `{"tags":["env:prod"],"monitor":{}}`,
			wantWarn: 1,
		},
		{
			name:     "monitor search with service tag",
			raw:      `{"tags":["service:my-app","env:prod"],"monitor":{}}`,
			wantWarn: 0,
		},
		{
			name:     "monitor search with query no service tag",
			raw:      `{"tags":["env:prod"],"monitor":{"query":"name:my-monitor"}}`,
			wantWarn: 0,
		},
		{
			name:     "monitor by-id",
			raw:      `{"monitor":{"id":123}}`,
			wantWarn: 0,
		},
		{
			name:     "slo search with env tag only",
			raw:      `{"tags":["env:prod"],"slo":{}}`,
			wantWarn: 1,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			c, err := ParseConfig(metricWith(t, tc.raw))
			require.NoError(t, err)
			assert.Len(t, c.Warnings(), tc.wantWarn)
		})
	}
}
