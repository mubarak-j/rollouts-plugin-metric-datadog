package datasource

import (
	"context"
	"fmt"

	"github.com/DataDog/datadog-api-client-go/v2/api/datadog"
	"github.com/mubarak-j/rollouts-plugin-metric-datadog/internal/config"
)

// metricsSource, monitorSource, and sloSource are temporary stubs.
// Tasks 7–9 replace each stub with a real implementation.

type metricsSource struct{}

func (metricsSource) Query(_ context.Context, _ *datadog.APIClient, _ *config.Config) (Result, error) {
	return Result{}, fmt.Errorf("metrics source not implemented")
}
func (metricsSource) Key(_ *config.Config) string { return "metrics" }

type monitorSource struct{}

func (monitorSource) Query(_ context.Context, _ *datadog.APIClient, _ *config.Config) (Result, error) {
	return Result{}, fmt.Errorf("monitor source not implemented")
}
func (monitorSource) Key(_ *config.Config) string { return "monitor" }

type sloSource struct{}

func (sloSource) Query(_ context.Context, _ *datadog.APIClient, _ *config.Config) (Result, error) {
	return Result{}, fmt.Errorf("slo source not implemented")
}
func (sloSource) Key(_ *config.Config) string { return "slo" }
