package datadog

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"time"

	"github.com/DataDog/datadog-api-client-go/v2/api/datadog"
)

// ClientOptions configures how NewClient builds the Datadog API client.
type ClientOptions struct {
	Site          string
	Address       string // full URL override; wins over Site when set
	Timeout       time.Duration
	MaxRetries    int
	Transport     http.RoundTripper // optional; Task 10 injects the rate limiter
	AllowInsecure bool              // allow http:// addresses (test use only)
}

// NewClient builds an authenticated *datadog.APIClient from the given credentials
// and options. An optional http.RoundTripper can be provided via opts.Transport.
func NewClient(creds Credentials, opts ClientOptions) (*datadog.APIClient, error) {
	cfg := datadog.NewConfiguration()

	// Retry: enable so 429/5xx honor X-Ratelimit-Reset.
	cfg.RetryConfiguration.EnableRetry = true
	if opts.MaxRetries > 0 {
		cfg.RetryConfiguration.MaxRetries = opts.MaxRetries
	}

	timeout := opts.Timeout
	if timeout <= 0 {
		timeout = 30 * time.Second
	}
	// Bound SDK retry below the plugin timeout so the outer call can still
	// observe a clean deadline (Fix #5).
	cfg.RetryConfiguration.HTTPRetryTimeout = timeout - timeout/5
	cfg.HTTPClient = &http.Client{Timeout: timeout, Transport: opts.Transport}

	// Address override: opts.Address wins, then creds.Address, then site default.
	addr := opts.Address
	if addr == "" {
		addr = creds.Address
	}
	if addr != "" {
		host, scheme, err := hostScheme(addr, opts.AllowInsecure)
		if err != nil {
			return nil, err
		}
		cfg.Host = host
		cfg.Scheme = scheme
	}

	return datadog.NewAPIClient(cfg), nil
}

// AuthContext installs API key authentication into ctx. When site is non-empty,
// it also sets ContextServerVariables so the SDK routes to the correct regional
// endpoint. Use only when no full address override is provided.
func AuthContext(ctx context.Context, creds Credentials, site string) context.Context {
	ctx = context.WithValue(ctx, datadog.ContextAPIKeys, map[string]datadog.APIKey{
		"apiKeyAuth": {Key: creds.APIKey},
		"appKeyAuth": {Key: creds.AppKey},
	})
	if site != "" {
		ctx = context.WithValue(ctx, datadog.ContextServerVariables, map[string]string{"site": site})
	}
	return ctx
}

// hostScheme parses address into its host and scheme components.
// Unless allowInsecure is true, a non-https scheme is rejected to prevent
// live API keys being transmitted in cleartext.
func hostScheme(address string, allowInsecure bool) (host, scheme string, err error) {
	u, err := url.Parse(address)
	if err != nil {
		return "", "", fmt.Errorf("invalid address %q: %w", address, err)
	}
	if u.Host == "" || u.Scheme == "" {
		return "", "", fmt.Errorf("address %q must be a full URL with scheme and host", address)
	}
	if u.Scheme != "https" && !allowInsecure {
		return "", "", fmt.Errorf("address %q uses scheme %q; only https is allowed (set AllowInsecure to override)", address, u.Scheme)
	}
	return u.Host, u.Scheme, nil
}
