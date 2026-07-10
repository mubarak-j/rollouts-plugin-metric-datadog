package datadog

import (
	"net/http"
	"strconv"
	"sync"

	"golang.org/x/time/rate"
)

// LimiterOptions configures the shared rate-limit transport.
type LimiterOptions struct {
	Enabled        bool
	DefaultRPS     float64
	MaxConcurrent  int
	BudgetFraction float64
	StaticBuckets  map[string]float64 // bucket name -> rps ceiling
}

// RateInfo holds the last-seen X-RateLimit-* header values for a named bucket.
// Exposed for observability (Task 11 surfaces it).
type RateInfo struct {
	Name         string
	Limit        float64
	Remaining    float64
	ResetSeconds float64
}

// Limiter is a shared http.RoundTripper decorator that enforces:
//  1. A per-bucket token-bucket QPS ceiling (layer 1).
//  2. Adaptation of the ceiling from X-RateLimit-* response headers (layer 2).
//  3. A concurrency cap via a semaphore channel (layer 4).
//
// SDK 429/5xx retry (layer 5) is already enabled in Task 4 — this transport
// shapes outbound rate so 429s are rare; it does NOT duplicate 429 retry.
type Limiter struct {
	opts LimiterOptions
	sem  chan struct{}

	mu         sync.Mutex
	buckets    map[string]*rate.Limiter
	lastInfo   map[string]RateInfo
	pathBucket map[string]string // "METHOD path" -> X-RateLimit-Name learned from responses
}

// NewLimiter builds a Limiter with the given options.
func NewLimiter(opts LimiterOptions) *Limiter {
	if opts.DefaultRPS <= 0 {
		opts.DefaultRPS = 10
	}
	if opts.BudgetFraction <= 0 {
		opts.BudgetFraction = 0.5
	}
	var sem chan struct{}
	if opts.MaxConcurrent > 0 {
		sem = make(chan struct{}, opts.MaxConcurrent)
	}
	return &Limiter{
		opts:       opts,
		sem:        sem,
		buckets:    map[string]*rate.Limiter{},
		lastInfo:   map[string]RateInfo{},
		pathBucket: map[string]string{},
	}
}

// Transport returns an http.RoundTripper that applies rate limiting.
func (l *Limiter) Transport() http.RoundTripper {
	return &limitRT{l: l, next: http.DefaultTransport}
}

// LastInfo returns the last-seen RateLimit headers for a named bucket.
func (l *Limiter) LastInfo(bucket string) RateInfo {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.lastInfo[bucket]
}

// LearnedBucket reports the X-RateLimit-Name observed for a method+path, or ""
// if none has been seen yet. Exposed for observability and tests.
func (l *Limiter) LearnedBucket(method, path string) string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.pathBucket[method+" "+path]
}

// bucketFor returns the rate.Limiter for the named bucket, creating it if needed.
// An empty name resolves to "_default".
func (l *Limiter) bucketFor(name string) *rate.Limiter {
	l.mu.Lock()
	defer l.mu.Unlock()
	if name == "" {
		name = "_default"
	}
	if lim, ok := l.buckets[name]; ok {
		return lim
	}
	rps := l.opts.DefaultRPS
	if v, ok := l.opts.StaticBuckets[name]; ok && v > 0 {
		rps = v
	}
	// Burst = max(1, rps) allows a short initial burst equal to one second's
	// worth of tokens, which avoids strictly serialising concurrent canary-wave
	// traffic while still respecting the per-second ceiling.
	lim := rate.NewLimiter(rate.Limit(rps), max(1, int(rps)))
	l.buckets[name] = lim
	return lim
}

// observe reads X-RateLimit-* headers from resp, records RateInfo, learns the
// path→bucket mapping, and adapts the token-bucket ceiling.
func (l *Limiter) observe(resp *http.Response) {
	name := resp.Header.Get("X-RateLimit-Name")
	if name == "" {
		return
	}
	info := RateInfo{
		Name:         name,
		Limit:        parseFloat(resp.Header.Get("X-RateLimit-Limit")),
		Remaining:    parseFloat(resp.Header.Get("X-RateLimit-Remaining")),
		ResetSeconds: parseFloat(resp.Header.Get("X-RateLimit-Reset")),
	}
	period := parseFloat(resp.Header.Get("X-RateLimit-Period"))

	l.mu.Lock()
	defer l.mu.Unlock()
	l.lastInfo[name] = info
	if resp.Request != nil {
		l.pathBucket[pathKey(resp.Request)] = name // learn which bucket gates this endpoint
	}
	// Adapt the ceiling: limit/period * budgetFraction.
	if info.Limit > 0 && period > 0 {
		rps := (info.Limit / period) * l.opts.BudgetFraction
		if rps <= 0 {
			rps = l.opts.DefaultRPS
		}
		if lim, ok := l.buckets[name]; ok {
			lim.SetLimit(rate.Limit(rps))
			lim.SetBurst(max(1, int(rps)))
		} else {
			l.buckets[name] = rate.NewLimiter(rate.Limit(rps), max(1, int(rps)))
		}
	}
}

func pathKey(req *http.Request) string { return req.Method + " " + req.URL.Path }

// limitRT is the http.RoundTripper that enforces concurrency + rate limits.
type limitRT struct {
	l    *Limiter
	next http.RoundTripper
}

func (t *limitRT) RoundTrip(req *http.Request) (*http.Response, error) {
	if !t.l.opts.Enabled {
		return t.next.RoundTrip(req)
	}

	// Layer 4: concurrency cap via semaphore.
	if t.l.sem != nil {
		select {
		case t.l.sem <- struct{}{}:
			defer func() { <-t.l.sem }()
		case <-req.Context().Done():
			return nil, req.Context().Err()
		}
	}

	// Layer 1: token-bucket QPS ceiling.
	// Gate on the bucket learned for this endpoint. The first call to a given
	// path has no learned name yet and waits on "_default"; once a response's
	// X-RateLimit-Name is observed, subsequent calls to the same path gate on
	// (and benefit from the adapted ceiling of) that named bucket.
	bucket := t.l.bucketFor(t.l.LearnedBucket(req.Method, req.URL.Path))
	if err := bucket.Wait(req.Context()); err != nil {
		return nil, err
	}

	resp, err := t.next.RoundTrip(req)
	if err != nil {
		return resp, err
	}

	// Layer 2: adapt ceiling from X-RateLimit-* headers.
	t.l.observe(resp)
	return resp, nil
}

func parseFloat(s string) float64 {
	f, _ := strconv.ParseFloat(s, 64)
	return f
}
