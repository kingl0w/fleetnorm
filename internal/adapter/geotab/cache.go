package geotab

import (
	"context"
	"sync"
	"time"
)

// Entity is a MyGeotab object flattened to the handful of fields the mapping
// reads. one struct covers every type we resolve, because four near identical
// structs would buy nothing.
type Entity struct {
	Name string //Device.name, Controller.name, Diagnostic.name
	VIN  string //Device.vehicleIdentificationNumber
	Code *int   //Diagnostic.code (an SPN when Kind says so), FailureMode.code
	Kind string //Diagnostic.diagnosticType

	//Diagnostic.source, one of Geotab's SourceXxxId sentinels. read for what it
	//says and never resolved
	Source string
}

// Resolver turns entity ids into resolved objects, one type at a time. an id
// the source could not resolve is simply absent from the result, which the
// caller reports rather than treating as an error.
//
// this is the seam a future OEM adapter reuses. it stays here until a second
// adapter needs it: one adapter is not a pattern.
type Resolver interface {
	Resolve(ctx context.Context, typeName string, ids []string) (map[string]Entity, error)
}

// FetchFunc looks up the ids a cache does not have.
type FetchFunc func(ctx context.Context, typeName string, ids []string) (map[string]Entity, error)

const (
	DefaultCacheRefresh = time.Hour
	DefaultCacheSize    = 10000
)

type cacheEntry struct {
	entity Entity
	at     time.Time
}

// cache is a bounded, time expiring map in front of fetch. diagnostics and
// devices are near static, and the 500/min Get budget is the tight one, so the
// point is to never ask twice for the same id.
//
// ponytail: FIFO eviction, not LRU. the working set is a fleet's devices and
// diagnostics, which fits; swap in an LRU if a customer ever overflows it.
type cache struct {
	mu      sync.Mutex
	entries map[string]cacheEntry //typeName + "/" + id
	order   []string              //insertion order, for eviction
	refresh time.Duration
	max     int
	fetch   FetchFunc
	now     func() time.Time
}

func newCache(fetch FetchFunc, refresh time.Duration, max int) *cache {
	if refresh <= 0 {
		refresh = DefaultCacheRefresh
	}
	if max <= 0 {
		max = DefaultCacheSize
	}
	return &cache{
		entries: map[string]cacheEntry{},
		refresh: refresh,
		max:     max,
		fetch:   fetch,
		now:     func() time.Time { return time.Now() },
	}
}

// Resolve returns what it has and fetches the rest in one call. a fetch error
// is returned with the hits still in hand, so a caller can emit what it can.
func (c *cache) Resolve(ctx context.Context, typeName string, ids []string) (map[string]Entity, error) {
	out := make(map[string]Entity, len(ids))
	var misses []string

	c.mu.Lock()
	now := c.now()
	seen := make(map[string]bool, len(ids))
	for _, id := range ids {
		if id == "" || seen[id] {
			continue
		}
		seen[id] = true
		if e, ok := c.entries[typeName+"/"+id]; ok && now.Sub(e.at) < c.refresh {
			out[id] = e.entity
			continue
		}
		misses = append(misses, id)
	}
	c.mu.Unlock()

	if len(misses) == 0 {
		return out, nil
	}
	fetched, err := c.fetch(ctx, typeName, misses)

	c.mu.Lock()
	defer c.mu.Unlock()
	for id, e := range fetched {
		out[id] = e
		c.put(typeName+"/"+id, e)
	}
	return out, err
}

// put stores one entry, evicting the oldest once the cache is full. caller
// holds the lock.
func (c *cache) put(key string, e Entity) {
	if _, exists := c.entries[key]; !exists {
		if len(c.order) >= c.max {
			delete(c.entries, c.order[0])
			c.order = c.order[1:]
		}
		c.order = append(c.order, key)
	}
	c.entries[key] = cacheEntry{entity: e, at: c.now()}
}
