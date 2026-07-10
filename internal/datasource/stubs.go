package datasource

import (
	"context"
	"fmt"

	"github.com/DataDog/datadog-api-client-go/v2/api/datadog"
	"github.com/mubarak-j/rollouts-plugin-metric-datadog/internal/config"
)

// sloSource is a temporary stub. Task 9 replaces it with a real implementation.

type sloSource struct{}

func (sloSource) Query(_ context.Context, _ *datadog.APIClient, _ *config.Config) (Result, error) {
	return Result{}, fmt.Errorf("slo source not implemented")
}
func (sloSource) Key(_ *config.Config) string { return "slo" }
