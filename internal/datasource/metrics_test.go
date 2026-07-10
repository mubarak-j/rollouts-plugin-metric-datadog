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
	var gotBody string
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		gotBody = string(b)
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
	assert.Contains(t, gotBody, `(a-b)/a`)     // formula is sent
	assert.Contains(t, gotBody, `"name":"a"`)  // query a included
	assert.Contains(t, gotBody, `"name":"b"`)  // query b included
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

func TestMetrics_V2EmptyQueriesReturnsError(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Error("server should not be called for empty queries")
	}))
	defer ts.Close()
	cfg := &config.Config{Metrics: &config.MetricsConfig{APIVersion: "v2", Queries: nil}}
	_, err := runSource(t, ts, metricsSource{}, cfg)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "no queries")
}

func TestMetrics_KeyIsDeterministic(t *testing.T) {
	cfg := &config.Config{
		Metrics: &config.MetricsConfig{
			APIVersion: "v2",
			Queries:    map[string]string{"a": "sum:hits{*}.as_count()", "b": "sum:errs{*}.as_count()"},
			Formula:    "(a-b)/a",
		},
	}
	src := metricsSource{}
	k1 := src.Key(cfg)
	k2 := src.Key(cfg)
	k3 := src.Key(cfg)
	assert.Equal(t, k1, k2)
	assert.Equal(t, k2, k3)
}
