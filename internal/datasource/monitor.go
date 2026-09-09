package datasource

import (
	"context"
	"fmt"

	"github.com/DataDog/datadog-api-client-go/v2/api/datadog"
	datadogV1 "github.com/DataDog/datadog-api-client-go/v2/api/datadogV1"
	"github.com/mubarak-j/rollouts-plugin-metric-datadog/internal/config"
)

type monitorSource struct{}

func (monitorSource) Key(cfg *config.Config) string {
	mn := cfg.Monitor
	if mn.ID != nil {
		return fmt.Sprintf("monitor|id|%d", *mn.ID)
	}
	return fmt.Sprintf("monitor|search|%s", JoinSearchQuery(cfg.Tags, mn.Query))
}

func (monitorSource) Query(ctx context.Context, client *datadog.APIClient, cfg *config.Config) (Result, error) {
	api := datadogV1.NewMonitorsApi(client)
	if cfg.Monitor.ID != nil {
		mon, _, err := api.GetMonitor(ctx, *cfg.Monitor.ID)
		if err != nil {
			return Result{}, fmt.Errorf("datadog GetMonitor: %w", err)
		}
		m, err := structToMap(mon)
		if err != nil {
			return Result{}, err
		}
		return Result{Value: m, Metadata: map[string]string{"source": "monitor", "mode": "id"}}, nil
	}

	query := JoinSearchQuery(cfg.Tags, cfg.Monitor.Query)
	opts := *datadogV1.NewSearchMonitorGroupsOptionalParameters().WithQuery(query)
	resp, _, err := api.SearchMonitorGroups(ctx, opts)
	if err != nil {
		return Result{}, fmt.Errorf("datadog SearchMonitorGroups: %w", err)
	}
	m, err := structToMap(resp)
	if err != nil {
		return Result{}, err
	}
	dropTagFacet(m)
	return Result{Value: m, Metadata: map[string]string{
		"source": "monitor", "mode": "search", "resolvedQuery": query,
	}}, nil
}

// dropTagFacet removes counts.tag from a group-search response. Datadog computes
// that facet over the entire matched set and caps it at 1000 entries, so an
// under-scoped query drags in every tag in the org: 54KB of an 80KB measurement
// value against a real org, versus 116 bytes for the counts.status the documented
// conditions actually read. Argo Rollouts retains 10 measurements per metric, so
// keeping it costs ~800KB of AnalysisRun status and can push a multi-metric
// template past etcd's 1.5MB request limit. The remaining facets are small.
func dropTagFacet(m map[string]interface{}) {
	if counts, ok := m["counts"].(map[string]interface{}); ok {
		delete(counts, "tag")
	}
}
