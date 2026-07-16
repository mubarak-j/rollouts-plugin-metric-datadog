package datadog

import (
	"context"
	"fmt"
	"os"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

type Credentials struct {
	APIKey  string
	AppKey  string
	Address string
}

type SecretRefInput struct {
	Name       string
	Namespaced bool
}

// SecretGetter abstracts reading a Secret's data, so tests can fake it.
type SecretGetter interface {
	GetSecret(ctx context.Context, namespace, name string) (map[string][]byte, error)
}

type Resolver struct {
	Secrets             SecretGetter
	ControllerNamespace string
}

func (r *Resolver) Resolve(ctx context.Context, runNamespace string, ref *SecretRefInput) (Credentials, error) {
	// 1. explicit secretRef
	if ref != nil && ref.Name != "" {
		ns := r.ControllerNamespace
		if ref.Namespaced {
			ns = runNamespace
		}
		return r.fromSecret(ctx, ns, ref.Name)
	}

	// 2. environment variables
	if ak := os.Getenv("DD_API_KEY"); ak != "" {
		creds := Credentials{APIKey: ak, AppKey: os.Getenv("DD_APP_KEY"), Address: os.Getenv("DD_ADDRESS")}
		if creds.AppKey == "" {
			return Credentials{}, fmt.Errorf("DD_API_KEY set but DD_APP_KEY missing")
		}
		return creds, nil
	}

	// 3. secret literally named "datadog" in the controller namespace
	return r.fromSecret(ctx, r.ControllerNamespace, "datadog")
}

func (r *Resolver) fromSecret(ctx context.Context, ns, name string) (Credentials, error) {
	data, err := r.Secrets.GetSecret(ctx, ns, name)
	if err != nil {
		return Credentials{}, fmt.Errorf("reading secret %s/%s: %w", ns, name, err)
	}
	ak := string(data["api-key"])
	pk := string(data["app-key"])
	if ak == "" || pk == "" {
		return Credentials{}, fmt.Errorf("secret %s/%s missing api-key/app-key", ns, name)
	}
	return Credentials{APIKey: ak, AppKey: pk, Address: string(data["address"])}, nil
}

// --- real kube client ---

type kubeSecretGetter struct {
	client kubernetes.Interface
}

func (k *kubeSecretGetter) GetSecret(ctx context.Context, ns, name string) (map[string][]byte, error) {
	s, err := k.client.CoreV1().Secrets(ns).Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		return nil, err
	}
	return s.Data, nil
}

const saNamespaceFile = "/var/run/secrets/kubernetes.io/serviceaccount/namespace"

func NewKubeSecretGetter() (SecretGetter, string, error) {
	cfg, err := rest.InClusterConfig()
	if err != nil {
		return nil, "", fmt.Errorf("in-cluster config: %w", err)
	}
	cs, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		return nil, "", fmt.Errorf("kube client: %w", err)
	}
	ns := "argo-rollouts"
	if b, err := os.ReadFile(saNamespaceFile); err == nil && len(b) > 0 {
		ns = string(b)
	}
	return &kubeSecretGetter{client: cs}, ns, nil
}
