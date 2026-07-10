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
