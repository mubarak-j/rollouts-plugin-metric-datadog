package plugin

import (
	"testing"

	rolloutsRpc "github.com/argoproj/argo-rollouts/metricproviders/plugin/rpc"
	"github.com/argoproj/argo-rollouts/pkg/apis/rollouts/v1alpha1"
	"github.com/stretchr/testify/assert"
)

func TestType(t *testing.T) {
	g := &RpcPlugin{}
	assert.Equal(t, "RPCPlugin", g.Type())
}

func TestResumeTerminateReturnMeasurementUnchanged(t *testing.T) {
	g := &RpcPlugin{}
	m := v1alpha1.Measurement{Phase: v1alpha1.AnalysisPhaseRunning, Value: "x"}
	assert.Equal(t, m, g.Resume(nil, v1alpha1.Metric{}, m))
	assert.Equal(t, m, g.Terminate(nil, v1alpha1.Metric{}, m))
}

func TestGarbageCollectAndInitReturnNoError(t *testing.T) {
	g := &RpcPlugin{}
	assert.False(t, g.GarbageCollect(nil, v1alpha1.Metric{}, 0).HasError())
}

func TestImplementsInterface(t *testing.T) {
	var _ rolloutsRpc.MetricProviderPlugin = (*RpcPlugin)(nil)
}
