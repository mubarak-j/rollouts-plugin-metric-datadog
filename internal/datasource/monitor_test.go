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

const monitorSearchJSON = `{
  "counts": {
    "status": [{"name": "Alert", "count": 0}, {"name": "OK", "count": 3}],
    "muted": [{"name": false, "count": 3}],
    "type": [{"name": "log alert", "count": 3}],
    "tag": [{"name": "env:prod", "count": 3}, {"name": "team:x", "count": 2}]
  },
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

func TestMonitor_SearchDropsTagFacet(t *testing.T) {
	// counts.tag is a facet over the whole matched set (capped at 1000 entries by
	// Datadog). On an under-scoped query it dominates the measurement value — 54KB
	// of an 80KB payload against a real org — and no condition can usefully read it.
	// The other facets are small and stay.
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(monitorSearchJSON))
	}))
	defer ts.Close()

	cfg := &config.Config{Monitor: &config.MonitorConfig{Query: "muted:false"}}
	res, err := runSource(t, ts, monitorSource{}, cfg)
	require.NoError(t, err)

	m := res.Value.(map[string]interface{})
	counts, ok := m["counts"].(map[string]interface{})
	require.True(t, ok)
	assert.NotContains(t, counts, "tag")
	assert.Contains(t, counts, "status")
	assert.Contains(t, counts, "muted")
	assert.Contains(t, counts, "type")
	assert.Contains(t, m, "groups")
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
