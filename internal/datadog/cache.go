package datadog

import (
	"context"
	"sync"
	"time"

	"golang.org/x/sync/singleflight"
)

// CacheOptions configures the short-TTL coalescing cache.
type CacheOptions struct {
	Enabled bool
	TTL     time.Duration
}

// Entry holds a cached value with its metadata and storage timestamp.
type Entry struct {
	Value    interface{}
	Meta     map[string]string
	StoredAt time.Time
}

// Cache coalesces concurrent identical queries via singleflight and caches
// successful results for a short TTL. Errors always propagate — no stale
// value is ever served.
type Cache struct {
	opts CacheOptions
	now  func() time.Time

	mu      sync.Mutex
	entries map[string]Entry
	group   singleflight.Group
}

// NewCache returns a Cache configured with opts.
func NewCache(opts CacheOptions) *Cache {
	return &Cache{opts: opts, now: time.Now, entries: map[string]Entry{}}
}

type doResult struct {
	value interface{}
	meta  map[string]string
}

func (c *Cache) get(key string) (Entry, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	e, ok := c.entries[key]
	return e, ok
}

// put stores an entry and evicts all entries that have expired as of now.
// Eviction is bounded to entries whose TTL has elapsed, so the map never
// grows unboundedly when the cache key is time-varying.
func (c *Cache) put(key string, e Entry, now time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for k, existing := range c.entries {
		if now.Sub(existing.StoredAt) > c.opts.TTL {
			delete(c.entries, k)
		}
	}
	c.entries[key] = e
}

// len returns the number of cached entries. Exposed for tests only.
func (c *Cache) len() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.entries)
}

// Do returns a value for key, coalescing concurrent calls and caching results.
// source is one of: fresh | coalesced | cached.
// On fn error the error propagates; no stale value is served.
func (c *Cache) Do(ctx context.Context, key string, fresh bool, fn func() (interface{}, map[string]string, error)) (interface{}, map[string]string, string, error) {
	if !c.opts.Enabled {
		v, meta, err := fn()
		return v, meta, "fresh", err
	}

	// TTL hit — serve without calling fn.
	if !fresh {
		if e, ok := c.get(key); ok && c.now().Sub(e.StoredAt) <= c.opts.TTL {
			return e.Value, e.Meta, "cached", nil
		}
	}

	res, err, shared := c.group.Do(key, func() (interface{}, error) {
		v, meta, e := fn()
		if e != nil {
			return nil, e
		}
		now := c.now()
		c.put(key, Entry{Value: v, Meta: meta, StoredAt: now}, now)
		return doResult{value: v, meta: meta}, nil
	})

	if err != nil {
		return nil, nil, "", err
	}
	dr := res.(doResult)
	src := "fresh"
	if shared {
		src = "coalesced"
	}
	return dr.value, dr.meta, src, nil
}
