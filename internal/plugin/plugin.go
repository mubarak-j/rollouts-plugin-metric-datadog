package plugin

import (
	"errors"

	"github.com/argoproj/argo-rollouts/metricproviders/plugin"
	"github.com/argoproj/argo-rollouts/pkg/apis/rollouts/v1alpha1"
	"github.com/argoproj/argo-rollouts/utils/metric"
	"github.com/argoproj/argo-rollouts/utils/plugin/types"
	timeutil "github.com/argoproj/argo-rollouts/utils/time"
	log "github.com/sirupsen/logrus"
)

var errNotImplemented = errors.New("datadog plugin: Run not implemented yet")

// RpcPlugin implements rpc.MetricProviderPlugin for Datadog.
type RpcPlugin struct {
	LogCtx log.Entry
}

// InitPlugin is called once per plugin process. Later tasks build the kube
// client and shared rate-limit/cache state here.
func (g *RpcPlugin) InitPlugin() types.RpcError {
	return types.RpcError{}
}

// Run is filled in by Task 6.
func (g *RpcPlugin) Run(analysisRun *v1alpha1.AnalysisRun, m v1alpha1.Metric) v1alpha1.Measurement {
	startTime := timeutil.MetaNow()
	measurement := v1alpha1.Measurement{StartedAt: &startTime}
	return metric.MarkMeasurementError(measurement, errNotImplemented)
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

func (g *RpcPlugin) Type() string {
	return plugin.ProviderType
}

// GetMetadata is filled in by Task 6.
func (g *RpcPlugin) GetMetadata(_ v1alpha1.Metric) map[string]string {
	return map[string]string{}
}
