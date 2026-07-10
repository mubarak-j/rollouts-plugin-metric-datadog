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
