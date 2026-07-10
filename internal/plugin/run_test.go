package plugin

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/DataDog/datadog-api-client-go/v2/api/datadog"
	"github.com/argoproj/argo-rollouts/pkg/apis/rollouts/v1alpha1"
	"github.com/mubarak-j/rollouts-plugin-metric-datadog/internal/config"
	ddinternal "github.com/mubarak-j/rollouts-plugin-metric-datadog/internal/datadog"
	"github.com/mubarak-j/rollouts-plugin-metric-datadog/internal/datasource"
	log "github.com/sirupsen/logrus"
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

func TestRun_MetricsConditionAgainstRealServer(t *testing.T) {
	// Guard against ambient DD_* env vars (note B): this plugin builds directly
	// without newTestPlugin, so we must set them ourselves.
	t.Setenv("DD_API_KEY", "")
	t.Setenv("DD_APP_KEY", "")

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
			opts.AllowInsecure = true // httptest uses http:// (note B)
			return ddinternal.NewClient(creds, opts)
		},
	}
	m := metricWith(t, `{"metrics":{"query":"avg:cpu{*}"}}`)
	m.SuccessCondition = "result >= 0.95"
	out := g.Run(&v1alpha1.AnalysisRun{}, m)
	require.Equal(t, v1alpha1.AnalysisPhaseSuccessful, out.Phase, out.Message)
}
