package config

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/argoproj/argo-rollouts/pkg/apis/rollouts/v1alpha1"
)

// ConfigKey is the plugin name used as the map key in metric.Provider.Plugin
// and as provider.plugin.<name> in AnalysisTemplates.
const ConfigKey = "mubarak-j/rollouts-plugin-metric-datadog"

type Config struct {
	Site           string           `json:"site,omitempty"`
	Address        string           `json:"address,omitempty"`
	SecretRef      *SecretRef       `json:"secretRef,omitempty"`
	TimeoutSeconds int              `json:"timeoutSeconds,omitempty"`
	Tags           []string         `json:"tags,omitempty"`
	RateLimit      *RateLimitConfig `json:"rateLimit,omitempty"`
	Cache          *CacheConfig     `json:"cache,omitempty"`
	Retry          *RetryConfig     `json:"retry,omitempty"`

	Metrics *MetricsConfig `json:"metrics,omitempty"`
	Monitor *MonitorConfig `json:"monitor,omitempty"`
	SLO     *SLOConfig     `json:"slo,omitempty"`
}

type SecretRef struct {
	Name       string `json:"name,omitempty"`
	Namespaced bool   `json:"namespaced,omitempty"`
}

type MetricsConfig struct {
	APIVersion string            `json:"apiVersion,omitempty"` // v1|v2, default v2
	Query      string            `json:"query,omitempty"`
	Queries    map[string]string `json:"queries,omitempty"`
	Formula    string            `json:"formula,omitempty"`
	Aggregator string            `json:"aggregator,omitempty"` // v2 only
	Interval   string            `json:"interval,omitempty"`   // default 5m
}

type MonitorConfig struct {
	Mode  string `json:"mode,omitempty"` // search (default) | (id set => by-id)
	Query string `json:"query,omitempty"`
	ID    *int64 `json:"id,omitempty"`
}

type SLOConfig struct {
	Mode     string  `json:"mode,omitempty"` // search (default) | (id set => by-id)
	Query    string  `json:"query,omitempty"`
	ID       *string `json:"id,omitempty"`
	Interval string  `json:"interval,omitempty"` // by-id history window, default 7d
}

type RateLimitConfig struct {
	Enabled        *bool                    `json:"enabled,omitempty"` // default true
	BudgetFraction float64                  `json:"budgetFraction,omitempty"`
	Buckets        map[string]BucketCeiling `json:"buckets,omitempty"`
	MaxConcurrent  int                      `json:"maxConcurrent,omitempty"`
}

type BucketCeiling struct {
	RPS float64 `json:"rps,omitempty"`
}

type CacheConfig struct {
	Enabled      *bool  `json:"enabled,omitempty"` // default true
	TTL          string `json:"ttl,omitempty"`
	MaxStaleness string `json:"maxStaleness,omitempty"`
}

type RetryConfig struct {
	MaxRetries int `json:"maxRetries,omitempty"` // default 3
}

func ParseConfig(metric v1alpha1.Metric) (*Config, error) {
	raw, ok := metric.Provider.Plugin[ConfigKey]
	if !ok || len(raw) == 0 {
		return nil, fmt.Errorf("no plugin config found under %q", ConfigKey)
	}
	c := &Config{}
	if err := json.Unmarshal(raw, c); err != nil {
		return nil, fmt.Errorf("invalid plugin config JSON: %w", err)
	}
	c.applyDefaults()
	if err := c.Validate(); err != nil {
		return nil, err
	}
	return c, nil
}

func (c *Config) applyDefaults() {
	if c.Metrics != nil && c.Metrics.APIVersion == "" {
		c.Metrics.APIVersion = "v2"
	}
}

func (c *Config) Source() string {
	switch {
	case c.Metrics != nil:
		return "metrics"
	case c.Monitor != nil:
		return "monitor"
	case c.SLO != nil:
		return "slo"
	default:
		return ""
	}
}

func (c *Config) Timeout() time.Duration {
	if c.TimeoutSeconds > 0 {
		return time.Duration(c.TimeoutSeconds) * time.Second
	}
	return 30 * time.Second
}

func (c *Config) Validate() error {
	n := 0
	for _, present := range []bool{c.Metrics != nil, c.Monitor != nil, c.SLO != nil} {
		if present {
			n++
		}
	}
	if n != 1 {
		return fmt.Errorf("exactly one of metrics/monitor/slo must be set (found %d)", n)
	}

	if c.SecretRef != nil && c.SecretRef.Namespaced && c.SecretRef.Name == "" {
		return fmt.Errorf("secretRef.namespaced=true requires a non-empty name")
	}

	byID := (c.Monitor != nil && c.Monitor.ID != nil) || (c.SLO != nil && c.SLO.ID != nil)
	if byID && len(c.Tags) > 0 {
		return fmt.Errorf("tags cannot be combined with a by-id mode (monitor.id/slo.id); use search mode for tag filtering")
	}

	if c.Metrics != nil {
		return c.validateMetrics()
	}
	return nil
}

func (c *Config) validateMetrics() error {
	m := c.Metrics
	if m.APIVersion != "v1" && m.APIVersion != "v2" {
		return fmt.Errorf("metrics.apiVersion must be v1 or v2, got %q", m.APIVersion)
	}
	hasQuery := m.Query != ""
	hasQueries := len(m.Queries) > 0
	if hasQuery == hasQueries {
		return fmt.Errorf("metrics requires exactly one of query or queries")
	}
	if m.Formula != "" && !hasQueries {
		return fmt.Errorf("metrics.formula requires queries")
	}
	if len(m.Queries) > 1 && m.Formula == "" {
		return fmt.Errorf("metrics with more than one query requires a formula")
	}
	if m.Aggregator != "" && m.APIVersion != "v2" {
		return fmt.Errorf("metrics.aggregator is valid only for apiVersion v2")
	}
	return nil
}

// Warnings returns non-fatal configuration warnings surfaced by Run() to logs.
// It warns when a monitor or slo source is in search mode (ID is nil) and the
// config has neither a service-identifying tag nor a per-source query, since the
// resolved query may match resources across rollouts and coalesce/collide in the
// shared cache.
func (c *Config) Warnings() []string {
	var warns []string
	svc := hasServiceTag(c.Tags)

	if c.Monitor != nil && c.Monitor.ID == nil {
		if !svc && c.Monitor.Query == "" {
			warns = append(warns, "monitor search has no service-identifying tag (service:...) and no query; its resolved query may not uniquely identify the gated target")
		}
	}
	if c.SLO != nil && c.SLO.ID == nil {
		if !svc && c.SLO.Query == "" {
			warns = append(warns, "slo search has no service-identifying tag (service:...) and no query; its resolved query may not uniquely identify the gated target")
		}
	}
	return warns
}

func hasServiceTag(tags []string) bool {
	for _, t := range tags {
		if strings.HasPrefix(t, "service:") {
			return true
		}
	}
	return false
}
