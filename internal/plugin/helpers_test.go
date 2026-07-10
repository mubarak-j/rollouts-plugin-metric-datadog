package plugin

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/DataDog/datadog-api-client-go/v2/api/datadog"
	"github.com/argoproj/argo-rollouts/pkg/apis/rollouts/v1alpha1"
	"github.com/mubarak-j/rollouts-plugin-metric-datadog/internal/config"
	ddinternal "github.com/mubarak-j/rollouts-plugin-metric-datadog/internal/datadog"
	"github.com/mubarak-j/rollouts-plugin-metric-datadog/internal/datasource"
	log "github.com/sirupsen/logrus"
)

// countingSource is a DataSource that records how many times Query is called.
type countingSource struct {
	value interface{}
	calls int
}

func (c *countingSource) Query(_ context.Context, _ *datadog.APIClient, _ *config.Config) (datasource.Result, error) {
	c.calls++
	return datasource.Result{Value: c.value}, nil
}
func (c *countingSource) Key(_ *config.Config) string { return "counting" }

func metricWith(t *testing.T, raw string) v1alpha1.Metric {
	t.Helper()
	return v1alpha1.Metric{Provider: v1alpha1.MetricProvider{
		Plugin: map[string]json.RawMessage{config.ConfigKey: json.RawMessage(raw)},
	}}
}

type stubSecrets struct{}

func (stubSecrets) GetSecret(_ context.Context, _, _ string) (map[string][]byte, error) {
	return map[string][]byte{"api-key": []byte("AK"), "app-key": []byte("PK")}, nil
}

// newTestPlugin builds an RpcPlugin whose source selection is overridden to a fake.
func newTestPlugin(t *testing.T, ds datasource.DataSource) *RpcPlugin {
	t.Helper()
	// Clear any ambient DD_* env vars so credential resolution always falls
	// through to stubSecrets (step 3) rather than stopping at env-var lookup
	// (step 2). t.Setenv auto-restores the original value after the test.
	t.Setenv("DD_API_KEY", "")
	t.Setenv("DD_APP_KEY", "")
	return &RpcPlugin{
		LogCtx:              *log.WithField("test", t.Name()),
		resolver:            &ddinternal.Resolver{Secrets: stubSecrets{}, ControllerNamespace: "argo-rollouts"},
		controllerNamespace: "argo-rollouts",
		cache:               ddinternal.NewCache(ddinternal.CacheOptions{Enabled: false}),
		selectSource:        func(_ *config.Config) (datasource.DataSource, error) { return ds, nil },
		newClient: func(creds ddinternal.Credentials, opts ddinternal.ClientOptions) (*datadog.APIClient, error) {
			return ddinternal.NewClient(creds, opts)
		},
	}
}
