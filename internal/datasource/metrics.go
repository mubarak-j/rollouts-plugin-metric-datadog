package datasource

import (
	"context"
	"fmt"
	"time"

	"github.com/DataDog/datadog-api-client-go/v2/api/datadog"
	datadogV1 "github.com/DataDog/datadog-api-client-go/v2/api/datadogV1"
	datadogV2 "github.com/DataDog/datadog-api-client-go/v2/api/datadogV2"
	"github.com/mubarak-j/rollouts-plugin-metric-datadog/internal/config"
)

type metricsSource struct{}

func (metricsSource) Key(cfg *config.Config) string {
	m := cfg.Metrics
	return fmt.Sprintf("metrics|%s|%s|%v|%s|%v|%s", m.APIVersion, m.Query, m.Queries, m.Formula, cfg.Tags, m.Interval)
}

func (metricsSource) Query(ctx context.Context, client *datadog.APIClient, cfg *config.Config) (Result, error) {
	win, err := WindowFrom(time.Now(), cfg.Metrics.Interval, 5*time.Minute)
	if err != nil {
		return Result{}, err
	}
	if cfg.Metrics.APIVersion == "v1" {
		return queryV1(ctx, client, cfg, win)
	}
	return queryV2(ctx, client, cfg, win)
}

func queryV2(ctx context.Context, client *datadog.APIClient, cfg *config.Config, win TimeWindow) (Result, error) {
	m := cfg.Metrics
	agg := metricsAggregator(m.Aggregator)

	var queries []datadogV2.ScalarQuery
	var formulas []datadogV2.QueryFormula
	resolved := map[string]string{}

	if m.Query != "" {
		q := MergeScopeTags(m.Query, cfg.Tags)
		resolved["a"] = q
		queries = append(queries, scalarQuery("a", q, agg))
		formulas = append(formulas, datadogV2.QueryFormula{Formula: "a"})
	} else {
		// stable name order for determinism
		names := sortedKeys(m.Queries)
		for _, name := range names {
			q := MergeScopeTags(m.Queries[name], cfg.Tags)
			resolved[name] = q
			queries = append(queries, scalarQuery(name, q, agg))
		}
		formula := m.Formula
		if formula == "" {
			// A single query with no formula is valid (validation only requires a
			// formula for >1 query). Project the sole query's column by name so we
			// never send an empty Formula to Datadog.
			formula = names[0]
		}
		formulas = append(formulas, datadogV2.QueryFormula{Formula: formula})
	}

	body := datadogV2.ScalarFormulaQueryRequest{
		Data: datadogV2.ScalarFormulaRequest{
			Type: datadogV2.SCALARFORMULAREQUESTTYPE_SCALAR_REQUEST,
			Attributes: datadogV2.ScalarFormulaRequestAttributes{
				From:     win.FromUnixMillis(),
				To:       win.ToUnixMillis(),
				Queries:  queries,
				Formulas: formulas,
			},
		},
	}

	api := datadogV2.NewMetricsApi(client)
	resp, _, err := api.QueryScalarData(ctx, body)
	if err != nil {
		return Result{}, fmt.Errorf("datadog v2 scalar query: %w", err)
	}
	value := extractV2Scalar(resp)
	return Result{Value: value, Metadata: map[string]string{
		"source": "metrics", "apiVersion": "v2", "resolvedQuery": fmt.Sprintf("%v", resolved),
	}}, nil
}

func scalarQuery(name, query string, agg datadogV2.MetricsAggregator) datadogV2.ScalarQuery {
	n := name
	return datadogV2.ScalarQuery{MetricsScalarQuery: &datadogV2.MetricsScalarQuery{
		DataSource: datadogV2.METRICSDATASOURCE_METRICS,
		Query:      query,
		Aggregator: agg,
		Name:       &n,
	}}
}

func extractV2Scalar(resp datadogV2.ScalarFormulaQueryResponse) interface{} {
	if resp.Data == nil || resp.Data.Attributes == nil {
		return nil
	}
	cols := resp.Data.Attributes.Columns
	if len(cols) == 0 || cols[0].DataScalarColumn == nil {
		return nil
	}
	vals := cols[0].DataScalarColumn.Values
	if len(vals) == 0 || vals[0] == nil {
		return nil
	}
	return *vals[0]
}

func queryV1(ctx context.Context, client *datadog.APIClient, cfg *config.Config, win TimeWindow) (Result, error) {
	q := MergeScopeTags(cfg.Metrics.Query, cfg.Tags)
	api := datadogV1.NewMetricsApi(client)
	resp, _, err := api.QueryMetrics(ctx, win.FromUnixSeconds(), win.ToUnixSeconds(), q)
	if err != nil {
		return Result{}, fmt.Errorf("datadog v1 query: %w", err)
	}
	value := extractV1Latest(resp)
	return Result{Value: value, Metadata: map[string]string{
		"source": "metrics", "apiVersion": "v1", "resolvedQuery": q,
	}}, nil
}

func extractV1Latest(resp datadogV1.MetricsQueryResponse) interface{} {
	if len(resp.Series) == 0 {
		return nil
	}
	pl := resp.Series[0].Pointlist
	if len(pl) == 0 {
		return nil
	}
	last := pl[len(pl)-1]
	if len(last) != 2 || last[1] == nil {
		return nil
	}
	return *last[1]
}

func metricsAggregator(s string) datadogV2.MetricsAggregator {
	if s == "" {
		return datadogV2.METRICSAGGREGATOR_LAST
	}
	// MetricsAggregator is a string enum; the value is the lowercase token.
	return datadogV2.MetricsAggregator(s)
}
