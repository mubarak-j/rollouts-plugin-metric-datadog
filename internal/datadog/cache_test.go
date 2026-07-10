package datadog

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCache_CoalescesConcurrent(t *testing.T) {
	c := NewCache(CacheOptions{Enabled: true, TTL: time.Minute})
	var calls int64
	fn := func() (interface{}, map[string]string, error) {
		atomic.AddInt64(&calls, 1)
		time.Sleep(50 * time.Millisecond)
		return 1.0, nil, nil
	}
	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, _, _, err := c.Do(context.Background(), "k", false, fn)
			require.NoError(t, err)
		}()
	}
	wg.Wait()
	assert.Equal(t, int64(1), atomic.LoadInt64(&calls)) // all calls collapsed into one
}

func TestCache_TTLHit(t *testing.T) {
	c := NewCache(CacheOptions{Enabled: true, TTL: time.Minute})
	var calls int64
	fn := func() (interface{}, map[string]string, error) {
		atomic.AddInt64(&calls, 1)
		return 2.0, nil, nil
	}
	_, _, s1, _ := c.Do(context.Background(), "k", false, fn)
	_, _, s2, _ := c.Do(context.Background(), "k", false, fn)
	assert.Equal(t, "fresh", s1)
	assert.Equal(t, "cached", s2)
	assert.Equal(t, int64(1), atomic.LoadInt64(&calls))
}

func TestCache_ErrorPropagates(t *testing.T) {
	c := NewCache(CacheOptions{Enabled: true, TTL: time.Minute})
	_, _, _, err := c.Do(context.Background(), "k", false, func() (interface{}, map[string]string, error) {
		return nil, nil, errors.New("boom")
	})
	assert.Error(t, err)
}

func TestCache_EvictsExpiredEntries(t *testing.T) {
	base := time.Now()
	c := NewCache(CacheOptions{Enabled: true, TTL: time.Second})
	c.now = func() time.Time { return base }

	// Seed one entry at base time
	_, _, _, err := c.Do(context.Background(), "old-key", false, func() (interface{}, map[string]string, error) {
		return 1.0, nil, nil
	})
	require.NoError(t, err)
	assert.Equal(t, 1, c.len())

	// Advance past TTL and store a new entry; eviction should remove the old one
	c.now = func() time.Time { return base.Add(2 * time.Second) }
	_, _, _, err = c.Do(context.Background(), "new-key", false, func() (interface{}, map[string]string, error) {
		return 2.0, nil, nil
	})
	require.NoError(t, err)

	// Only the new entry should remain; expired entry was evicted on store
	assert.Equal(t, 1, c.len())
}
