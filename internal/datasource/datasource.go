package datasource

import (
	"context"
	"fmt"
	"time"

	"github.com/DataDog/datadog-api-client-go/v2/api/datadog"
	"github.com/mubarak-j/rollouts-plugin-metric-datadog/internal/config"
)

// Result holds the value and metadata produced by a DataSource query.
type Result struct {
	Value    interface{}
	Metadata map[string]string
}

// DataSource executes a single query against the Datadog API and returns a Result.
type DataSource interface {
	Query(ctx context.Context, client *datadog.APIClient, cfg *config.Config) (Result, error)
	Key(cfg *config.Config) string
}

// Select returns the DataSource implementation for the single configured source.
// Concrete implementations are provided in their own files (Tasks 7–9); stubs.go
// provides temporary stand-ins that compile and return an "not implemented" error.
func Select(cfg *config.Config) (DataSource, error) {
	switch cfg.Source() {
	case "metrics":
		return metricsSource{}, nil
	case "monitor":
		return monitorSource{}, nil
	case "slo":
		return sloSource{}, nil
	default:
		return nil, fmt.Errorf("no data source for %q", cfg.Source())
	}
}

// TimeWindow is a closed [From, To] interval used by sources to bound queries.
type TimeWindow struct {
	From time.Time
	To   time.Time
}

// WindowFrom builds a TimeWindow ending at now and spanning the given interval.
// If interval is empty, def is used. Supports Go duration syntax plus "Nd" for days.
func WindowFrom(now time.Time, interval string, def time.Duration) (TimeWindow, error) {
	d := def
	if interval != "" {
		parsed, err := parseDuration(interval)
		if err != nil {
			return TimeWindow{}, fmt.Errorf("invalid interval %q: %w", interval, err)
		}
		d = parsed
	}
	return TimeWindow{From: now.Add(-d), To: now}, nil
}

func (w TimeWindow) FromUnixMillis() int64  { return w.From.UnixMilli() }
func (w TimeWindow) ToUnixMillis() int64    { return w.To.UnixMilli() }
func (w TimeWindow) FromUnixSeconds() int64 { return w.From.Unix() }
func (w TimeWindow) ToUnixSeconds() int64   { return w.To.Unix() }

// parseDuration extends time.ParseDuration with day support (e.g. "7d").
func parseDuration(s string) (time.Duration, error) {
	if len(s) > 1 && s[len(s)-1] == 'd' {
		// Convert "Nd" → parse "Nh" → multiply by 24 to get N days.
		hours, err := time.ParseDuration(s[:len(s)-1] + "h")
		if err != nil {
			return 0, err
		}
		return hours * 24, nil
	}
	return time.ParseDuration(s)
}
