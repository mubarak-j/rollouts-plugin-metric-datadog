package datasource

import (
	"context"
	"net/http/httptest"
	"testing"

	"github.com/mubarak-j/rollouts-plugin-metric-datadog/internal/config"
	ddinternal "github.com/mubarak-j/rollouts-plugin-metric-datadog/internal/datadog"
)

func runSource(t *testing.T, ts *httptest.Server, ds DataSource, cfg *config.Config) (Result, error) {
	t.Helper()
	creds := ddinternal.Credentials{APIKey: "AK", AppKey: "PK", Address: ts.URL}
	// AllowInsecure is required because httptest.Server uses http:// (not https://).
	client, err := ddinternal.NewClient(creds, ddinternal.ClientOptions{Address: ts.URL, AllowInsecure: true})
	if err != nil {
		return Result{}, err
	}
	ctx := ddinternal.AuthContext(context.Background(), creds, "")
	return ds.Query(ctx, client, cfg)
}
