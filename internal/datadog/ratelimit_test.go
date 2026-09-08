package datadog

import (
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLimiter_CapsQPS(t *testing.T) {
	var count int64
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt64(&count, 1)
		// No X-RateLimit-Name header: observe() returns early, so all 30
		// requests gate on the single "_default" bucket. This avoids the
		// two-bucket split (first N on _default, rest on learned bucket)
		// that would defeat the timing assertion below.
		w.WriteHeader(200)
	}))
	defer ts.Close()

	l := NewLimiter(LimiterOptions{Enabled: true, DefaultRPS: 10, MaxConcurrent: 4})
	client := &http.Client{Transport: l.Transport()}

	var wg sync.WaitGroup
	start := time.Now()
	for i := 0; i < 30; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			resp, err := client.Get(ts.URL)
			require.NoError(t, err)
			_ = resp.Body.Close()
		}()
	}
	wg.Wait()
	elapsed := time.Since(start)
	// Timing: burst=max(1,10)=10 tokens at t=0, then 1 token/100ms.
	// 30 requests = 10 burst + 20 metered → metered tokens span 20×100ms = 2s.
	// So elapsed ≥ 2s deterministically (single _default bucket, no split).
	assert.GreaterOrEqual(t, elapsed, 2*time.Second)
	assert.Equal(t, int64(30), atomic.LoadInt64(&count))
}

func TestLimiter_AdaptsCeilingFromHeaders(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-RateLimit-Name", "metrics_query")
		w.Header().Set("X-RateLimit-Limit", "1600")
		w.Header().Set("X-RateLimit-Period", "60")
		w.Header().Set("X-RateLimit-Remaining", "1599")
		w.Header().Set("X-RateLimit-Reset", "60")
		w.WriteHeader(200)
	}))
	defer ts.Close()
	l := NewLimiter(LimiterOptions{Enabled: true, DefaultRPS: 5, MaxConcurrent: 2, BudgetFraction: 0.5})
	client := &http.Client{Transport: l.Transport()}
	resp, err := client.Get(ts.URL)
	require.NoError(t, err)
	_ = resp.Body.Close()
	assert.Equal(t, "metrics_query", l.LearnedBucket(http.MethodGet, ""))
}

func TestLimiter_LearnsBucketPerPathAndGatesOnIt(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-RateLimit-Name", "metrics_query")
		w.WriteHeader(200)
	}))
	defer ts.Close()
	l := NewLimiter(LimiterOptions{Enabled: true, DefaultRPS: 100, MaxConcurrent: 4})
	client := &http.Client{Transport: l.Transport()}

	// Before any call, the endpoint has no learned bucket (gates on _default).
	assert.Equal(t, "", l.LearnedBucket(http.MethodGet, "/api/v2/query/scalar"))

	resp, err := client.Get(ts.URL + "/api/v2/query/scalar")
	require.NoError(t, err)
	_ = resp.Body.Close()

	// After one response, the path is mapped to the header bucket, so subsequent
	// RoundTrips gate on (and adapt) that named bucket rather than _default.
	assert.Equal(t, "metrics_query", l.LearnedBucket(http.MethodGet, "/api/v2/query/scalar"))
}

func TestLimiter_DisabledIsPassthrough(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(200) }))
	defer ts.Close()
	l := NewLimiter(LimiterOptions{Enabled: false})
	client := &http.Client{Transport: l.Transport()}
	resp, err := client.Get(ts.URL)
	require.NoError(t, err)
	_ = resp.Body.Close()
}
