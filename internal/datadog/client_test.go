package datadog

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	datadogV1 "github.com/DataDog/datadog-api-client-go/v2/api/datadogV1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewClient_RoutesToTestServerWithAuthHeaders(t *testing.T) {
	var gotAPIKey, gotAppKey string
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotAPIKey = r.Header.Get("DD-API-KEY")
		gotAppKey = r.Header.Get("DD-APPLICATION-KEY")
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"groups":[],"counts":{}}`))
	}))
	defer ts.Close()

	creds := Credentials{APIKey: "AK", AppKey: "PK", Address: ts.URL}
	// httptest server uses http://, so AllowInsecure must be true
	client, err := NewClient(creds, ClientOptions{Address: ts.URL, AllowInsecure: true})
	require.NoError(t, err)

	ctx := AuthContext(context.Background(), creds, "")
	api := datadogV1.NewMonitorsApi(client)
	_, _, err = api.SearchMonitorGroups(ctx)
	require.NoError(t, err)
	assert.Equal(t, "AK", gotAPIKey)
	assert.Equal(t, "PK", gotAppKey)
}

func TestHostScheme(t *testing.T) {
	// https address with AllowInsecure=false succeeds
	h, s, err := hostScheme("https://api.datadoghq.eu", false)
	require.NoError(t, err)
	assert.Equal(t, "api.datadoghq.eu", h)
	assert.Equal(t, "https", s)
}

func TestHostScheme_RejectHTTPWhenNotAllowInsecure(t *testing.T) {
	// http address with AllowInsecure=false must be rejected
	_, _, err := hostScheme("http://api.datadoghq.com", false)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "https")
}

func TestHostScheme_AllowHTTPWhenAllowInsecure(t *testing.T) {
	// http address with AllowInsecure=true succeeds
	h, s, err := hostScheme("http://api.datadoghq.com", true)
	require.NoError(t, err)
	assert.Equal(t, "api.datadoghq.com", h)
	assert.Equal(t, "http", s)
}

func TestNewClient_RejectsHTTPAddressWithoutAllowInsecure(t *testing.T) {
	creds := Credentials{APIKey: "AK", AppKey: "PK"}
	_, err := NewClient(creds, ClientOptions{Address: "http://api.datadoghq.com", AllowInsecure: false})
	assert.Error(t, err)
}
