package datadog

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type fakeSecrets struct {
	data map[string]map[string][]byte // namespace/name -> data
}

func (f *fakeSecrets) GetSecret(_ context.Context, ns, name string) (map[string][]byte, error) {
	if d, ok := f.data[ns+"/"+name]; ok {
		return d, nil
	}
	return nil, assertNotFound{}
}

type assertNotFound struct{}

func (assertNotFound) Error() string { return "not found" }

func TestResolve_SecretRefControllerNamespace(t *testing.T) {
	t.Setenv("DD_API_KEY", "")
	r := &Resolver{
		ControllerNamespace: "argo-rollouts",
		Secrets: &fakeSecrets{data: map[string]map[string][]byte{
			"argo-rollouts/my-dd": {"api-key": []byte("AK"), "app-key": []byte("PK")},
		}},
	}
	creds, err := r.Resolve(context.Background(), "app-ns", &SecretRefInput{Name: "my-dd"})
	require.NoError(t, err)
	assert.Equal(t, "AK", creds.APIKey)
	assert.Equal(t, "PK", creds.AppKey)
}

func TestResolve_SecretRefNamespaced(t *testing.T) {
	t.Setenv("DD_API_KEY", "")
	r := &Resolver{
		ControllerNamespace: "argo-rollouts",
		Secrets: &fakeSecrets{data: map[string]map[string][]byte{
			"app-ns/my-dd": {"api-key": []byte("AK"), "app-key": []byte("PK")},
		}},
	}
	creds, err := r.Resolve(context.Background(), "app-ns", &SecretRefInput{Name: "my-dd", Namespaced: true})
	require.NoError(t, err)
	assert.Equal(t, "AK", creds.APIKey)
	assert.Equal(t, "PK", creds.AppKey)
}

func TestResolve_EnvVars(t *testing.T) {
	t.Setenv("DD_API_KEY", "envAK")
	t.Setenv("DD_APP_KEY", "envPK")
	r := &Resolver{ControllerNamespace: "argo-rollouts", Secrets: &fakeSecrets{}}
	creds, err := r.Resolve(context.Background(), "app-ns", nil)
	require.NoError(t, err)
	assert.Equal(t, "envAK", creds.APIKey)
	assert.Equal(t, "envPK", creds.AppKey)
}

// TestResolve_EnvVarAPIKeyWithoutAppKey covers the branch where DD_API_KEY is
// set but DD_APP_KEY is absent — credentials.go returns an explicit error.
func TestResolve_EnvVarAPIKeyWithoutAppKey(t *testing.T) {
	t.Setenv("DD_API_KEY", "envAK")
	t.Setenv("DD_APP_KEY", "")
	r := &Resolver{ControllerNamespace: "argo-rollouts", Secrets: &fakeSecrets{}}
	_, err := r.Resolve(context.Background(), "app-ns", nil)
	assert.ErrorContains(t, err, "DD_APP_KEY")
}

func TestResolve_FallbackDatadogSecret(t *testing.T) {
	t.Setenv("DD_API_KEY", "")
	r := &Resolver{
		ControllerNamespace: "argo-rollouts",
		Secrets: &fakeSecrets{data: map[string]map[string][]byte{
			"argo-rollouts/datadog": {"api-key": []byte("dAK"), "app-key": []byte("dPK")},
		}},
	}
	creds, err := r.Resolve(context.Background(), "app-ns", nil)
	require.NoError(t, err)
	assert.Equal(t, "dAK", creds.APIKey)
	assert.Equal(t, "dPK", creds.AppKey)
}

// TestResolve_MissingKeysError exercises the branch where the fallback secret
// exists but is missing app-key — fromSecret returns an error.
func TestResolve_MissingKeysError(t *testing.T) {
	t.Setenv("DD_API_KEY", "")
	r := &Resolver{
		ControllerNamespace: "argo-rollouts",
		Secrets: &fakeSecrets{data: map[string]map[string][]byte{
			"argo-rollouts/datadog": {"api-key": []byte("AK")}, // app-key absent
		}},
	}
	_, err := r.Resolve(context.Background(), "app-ns", nil)
	assert.ErrorContains(t, err, "missing api-key/app-key")
}

// TestResolve_SecretNotFound exercises the path where the fallback secret does
// not exist at all (distinct from it being present but incomplete).
func TestResolve_SecretNotFound(t *testing.T) {
	t.Setenv("DD_API_KEY", "")
	r := &Resolver{ControllerNamespace: "argo-rollouts", Secrets: &fakeSecrets{}}
	_, err := r.Resolve(context.Background(), "app-ns", nil)
	assert.Error(t, err)
}
