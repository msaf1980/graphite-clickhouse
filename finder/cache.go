package finder

import (
	"sync"
	"sync/atomic"
	"time"
)

type CacheEntry struct {
	expire int64 // unix timestamp
	lock   sync.RWMutex

	result Result
	done   bool
}

func (e *CacheEntry) Result() (Result, bool) {
	e.RLock()
	var r Result
	done := e.done
	if done {
		r = e.result
	}
	e.Unlock()

	return r, done
}

func (e *CacheEntry) Lock() {
	e.lock.Lock()
}

func (e *CacheEntry) RLock() {
	e.lock.RLock()
}

func (e *CacheEntry) Unlock() {
	e.lock.Unlock()
}

type Cache struct {
	lock sync.RWMutex

	data map[string]*CacheEntry

	stopCh chan struct{}

	//metrics
	hits  uint64
	miss  uint64
	count uint64
}

func (c *Cache) LockEntry(key string) {
	c.Lock()
	e, ok := c.data[key]
	if !ok {
		e = new(CacheEntry)
		c.data[key] = e
	}
	e.Lock()
	c.Unlock()
}

func (c *Cache) Lock() {
	c.lock.Lock()
}

func (c *Cache) RLock() {
	c.lock.RLock()
}

func (c *Cache) Unlock() {
	c.lock.Unlock()
}

// Cahe Put item, call LockEntry before
func (c *Cache) PutEntry(key string, result Result, expire time.Duration) {
	c.RLock()
	if e, ok := c.data[key]; ok {
		e.result = result
		e.expire = time.Now().Add(expire).Unix()
		e.done = true
		atomic.AddUint64(&c.count, 1)
		e.Unlock()
	}
	c.Unlock()
}

// Cahe Put item, call LockEntry before
func (c *Cache) GetEntry(key string) (Result, bool) {
	var result Result
	var done bool

	c.RLock()
	if e, ok := c.data[key]; ok {
		result, done = e.Result()
		if done {
			atomic.AddUint64(&c.hits, 1)
		} else {
			atomic.AddUint64(&c.miss, 1)
		}
	} else {
		atomic.AddUint64(&c.miss, 1)
	}
	c.Unlock()

	return result, done
}

func (c *Cache) DeleteEntry(key string) {
	c.Lock()
	delete(c.data, key)
	c.Unlock()
}

func (c *Cache) ExpireEntries() {
	now := time.Now().Unix()
	c.Lock()
	for k, e := range c.data {
		if e.expire < now {
			delete(c.data, k)
		}
	}
	atomic.StoreUint64(&c.count, uint64(len(c.data)))
	c.Unlock()
}

func (c *Cache) Start() {
	c.stopCh = make(chan struct{})

	for {
		select {
		case <-c.stopCh:
			return

		}
	}
}

func (c *Cache) Stop() {
	c.stopCh <- struct{}{}
}
