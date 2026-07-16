package datasource

import (
	"context"
	"fmt"
	"time"

	"github.com/DataDog/datadog-api-client-go/v2/api/datadog"
	datadogV1 "github.com/DataDog/datadog-api-client-go/v2/api/datadogV1"
	"github.com/mubarak-j/rollouts-plugin-metric-datadog/internal/config"
)

type sloSource struct{}

func (sloSource) Key(cfg *config.Config) string {
	s := cfg.SLO
	if s.ID != nil {
		return fmt.Sprintf("slo|id|%s|%s", *s.ID, s.Interval)
	}
	return fmt.Sprintf("slo|search|%s", JoinSearchQuery(cfg.Tags, s.Query))
}

func (sloSource) Query(ctx context.Context, client *datadog.APIClient, cfg *config.Config) (Result, error) {
	api := datadogV1.NewServiceLevelObjectivesApi(client)

	if cfg.SLO.ID != nil {
		win, err := WindowFrom(time.Now(), cfg.SLO.Interval, 7*24*time.Hour)
		if err != nil {
			return Result{}, err
		}
		resp, _, err := api.GetSLOHistory(ctx, *cfg.SLO.ID, win.FromUnixSeconds(), win.ToUnixSeconds())
		if err != nil {
			return Result{}, fmt.Errorf("datadog GetSLOHistory: %w", err)
		}
		var value interface{}
		if resp.Data != nil && resp.Data.Overall != nil {
			if sli := resp.Data.Overall.SliValue.Get(); sli != nil {
				value = *sli
			}
		}
		return Result{Value: value, Metadata: map[string]string{"source": "slo", "mode": "id"}}, nil
	}

	query := JoinSearchQuery(cfg.Tags, cfg.SLO.Query)
	opts := *datadogV1.NewSearchSLOOptionalParameters().WithQuery(query)
	resp, _, err := api.SearchSLO(ctx, opts)
	if err != nil {
		return Result{}, fmt.Errorf("datadog SearchSLO: %w", err)
	}
	flat, err := flattenSLOSearch(resp)
	if err != nil {
		return Result{}, err
	}
	return Result{Value: flat, Metadata: map[string]string{
		"source": "slo", "mode": "search", "resolvedQuery": query,
	}}, nil
}

// flattenSLOSearch turns the nested SearchSLOResponse into { slos: [...], facets: {} }
// where each element is the per-SLO attributes object (§5.2), so conditions read
// result.slos[].overall_status[].state etc.
//
// SDK verification: SearchSLOResponseDataAttributes.Facets is *SearchSLOResponseDataAttributesFacets
// (confirmed in v2.62.0) — the facets block is kept. SliValue is datadog.NullableFloat64 on
// SLOHistorySLIData (resp.Data.Overall); .Get() returns *float64.
func flattenSLOSearch(resp datadogV1.SearchSLOResponse) (map[string]interface{}, error) {
	out := map[string]interface{}{"slos": []interface{}{}, "facets": map[string]interface{}{}}
	if resp.Data == nil || resp.Data.Attributes == nil {
		return out, nil
	}
	slos := make([]interface{}, 0, len(resp.Data.Attributes.Slos))
	for _, s := range resp.Data.Attributes.Slos {
		if s.Data == nil || s.Data.Attributes == nil {
			continue
		}
		attrs, err := structToMap(s.Data.Attributes)
		if err != nil {
			return nil, err
		}
		slos = append(slos, attrs)
	}
	out["slos"] = slos
	if resp.Data.Attributes.Facets != nil {
		facets, err := structToMap(resp.Data.Attributes.Facets)
		if err == nil {
			out["facets"] = facets
		}
	}
	return out, nil
}
