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
	return &RpcPlugin{
		LogCtx:              *log.WithField("test", t.Name()),
		resolver:            &ddinternal.Resolver{Secrets: stubSecrets{}, ControllerNamespace: "argo-rollouts"},
		controllerNamespace: "argo-rollouts",
		selectSource:        func(_ *config.Config) (datasource.DataSource, error) { return ds, nil },
		newClient: func(creds ddinternal.Credentials, opts ddinternal.ClientOptions) (*datadog.APIClient, error) {
			return ddinternal.NewClient(creds, opts)
		},
	}
}
