package plugin

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"

	"github.com/DataDog/datadog-api-client-go/v2/api/datadog"
	rolloutsPlugin "github.com/argoproj/argo-rollouts/metricproviders/plugin"
	"github.com/argoproj/argo-rollouts/pkg/apis/rollouts/v1alpha1"
	"github.com/argoproj/argo-rollouts/utils/evaluate"
	metricutil "github.com/argoproj/argo-rollouts/utils/metric"
	timeutil "github.com/argoproj/argo-rollouts/utils/time"
	"github.com/argoproj/argo-rollouts/utils/plugin/types"
	log "github.com/sirupsen/logrus"

	"github.com/mubarak-j/rollouts-plugin-metric-datadog/internal/config"
	ddinternal "github.com/mubarak-j/rollouts-plugin-metric-datadog/internal/datadog"
	"github.com/mubarak-j/rollouts-plugin-metric-datadog/internal/datasource"
)

// RpcPlugin implements rpc.MetricProviderPlugin for Datadog.
type RpcPlugin struct {
	LogCtx              log.Entry
	resolver            *ddinternal.Resolver
	controllerNamespace string
	limiter             *ddinternal.Limiter

	// indirections (overridable in tests)
	selectSource func(*config.Config) (datasource.DataSource, error)
	newClient    func(ddinternal.Credentials, ddinternal.ClientOptions) (*datadog.APIClient, error)
}

// InitPlugin is called once per plugin process. It wires the Kubernetes secret
// getter, credential resolver, and source dispatcher.
func (g *RpcPlugin) InitPlugin() types.RpcError {
	getter, ns, err := ddinternal.NewKubeSecretGetter()
	if err != nil {
		return types.RpcError{ErrorString: fmt.Sprintf("init kube client: %v", err)}
	}
	g.resolver = &ddinternal.Resolver{Secrets: getter, ControllerNamespace: ns}
	g.controllerNamespace = ns
	g.limiter = ddinternal.NewLimiter(ddinternal.LimiterOptions{
		Enabled:       true,
		DefaultRPS:    10,
		MaxConcurrent: 16,
	})
	g.selectSource = datasource.Select
	g.newClient = ddinternal.NewClient
	return types.RpcError{}
}

// Run executes a Datadog metric measurement: parse config, resolve credentials,
// build the API client, dispatch to the appropriate DataSource, evaluate the
// result, and return the populated Measurement.
func (g *RpcPlugin) Run(analysisRun *v1alpha1.AnalysisRun, metric v1alpha1.Metric) v1alpha1.Measurement {
	startTime := timeutil.MetaNow()
	m := v1alpha1.Measurement{StartedAt: &startTime}

	cfg, err := config.ParseConfig(metric)
	if err != nil {
		return finish(metricutil.MarkMeasurementError(m, err))
	}

	// Override 2: surface non-fatal warnings (e.g. under-scoped search) to logs.
	for _, w := range cfg.Warnings() {
		g.LogCtx.Warn(w)
	}

	ctx, cancel := context.WithTimeout(context.Background(), cfg.Timeout())
	defer cancel()

	var ref *ddinternal.SecretRefInput
	if cfg.SecretRef != nil {
		ref = &ddinternal.SecretRefInput{Name: cfg.SecretRef.Name, Namespaced: cfg.SecretRef.Namespaced}
	}
	creds, err := g.resolver.Resolve(ctx, analysisRun.Namespace, ref)
	if err != nil {
		return finish(metricutil.MarkMeasurementError(m, err))
	}

	client, err := g.newClient(creds, ddinternal.ClientOptions{
		Site: cfg.Site, Address: cfg.Address, Timeout: cfg.Timeout(),
		Transport: g.transport(),
	})
	if err != nil {
		return finish(metricutil.MarkMeasurementError(m, err))
	}
	ctx = ddinternal.AuthContext(ctx, creds, cfg.Site)

	ds, err := g.selectSource(cfg)
	if err != nil {
		return finish(metricutil.MarkMeasurementError(m, err))
	}

	res, err := ds.Query(ctx, client, cfg)
	if err != nil {
		return finish(metricutil.MarkMeasurementError(m, err))
	}

	phase, err := evaluate.EvaluateResult(res.Value, metric, g.LogCtx)
	if err != nil {
		return finish(metricutil.MarkMeasurementError(m, err))
	}
	m.Phase = phase
	m.Value = stringify(res.Value)
	m.Metadata = res.Metadata
	return finish(m)
}

// GetMetadata returns the resolved source name and any configured tags.
func (g *RpcPlugin) GetMetadata(metric v1alpha1.Metric) map[string]string {
	md := map[string]string{}
	cfg, err := config.ParseConfig(metric)
	if err != nil {
		return md
	}
	md["source"] = cfg.Source()
	if len(cfg.Tags) > 0 {
		md["tags"] = fmt.Sprintf("%v", cfg.Tags)
	}
	return md
}

func (g *RpcPlugin) Resume(_ *v1alpha1.AnalysisRun, _ v1alpha1.Metric, measurement v1alpha1.Measurement) v1alpha1.Measurement {
	return measurement
}

func (g *RpcPlugin) Terminate(_ *v1alpha1.AnalysisRun, _ v1alpha1.Metric, measurement v1alpha1.Measurement) v1alpha1.Measurement {
	return measurement
}

func (g *RpcPlugin) GarbageCollect(_ *v1alpha1.AnalysisRun, _ v1alpha1.Metric, _ int) types.RpcError {
	return types.RpcError{}
}

func (g *RpcPlugin) Type() string { return rolloutsPlugin.ProviderType }

// transport returns the shared rate-limit RoundTripper, or nil when the limiter
// is not initialised (e.g. test plugins built without InitPlugin).
func (g *RpcPlugin) transport() http.RoundTripper {
	if g.limiter == nil {
		return nil
	}
	return g.limiter.Transport()
}

// finish stamps FinishedAt on a Measurement if not already set.
func finish(m v1alpha1.Measurement) v1alpha1.Measurement {
	if m.FinishedAt == nil {
		t := timeutil.MetaNow()
		m.FinishedAt = &t
	}
	return m
}

// stringify renders a resolved value for Measurement.Value (display only;
// EvaluateResult receives the raw typed value).
func stringify(v interface{}) string {
	switch x := v.(type) {
	case float64:
		return strconv.FormatFloat(x, 'f', -1, 64)
	case string:
		return x
	case nil:
		return ""
	default:
		b, err := json.Marshal(x)
		if err != nil {
			return fmt.Sprintf("%v", x)
		}
		return string(b)
	}
}
