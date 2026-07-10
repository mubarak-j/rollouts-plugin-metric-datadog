package datasource

import (
	"context"
	"fmt"

	"github.com/DataDog/datadog-api-client-go/v2/api/datadog"
	"github.com/mubarak-j/rollouts-plugin-metric-datadog/internal/config"
)

// monitorSource and sloSource are temporary stubs.
// Tasks 8–9 replace each stub with a real implementation.

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
