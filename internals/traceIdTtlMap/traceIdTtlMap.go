package traceIdTtlMap

import (
	"sync"
	"time"
)

type TTLMap struct {
	m        map[string]int64
	mu       sync.RWMutex
	maxTtl   int64
	stopCh   chan struct{}
	stopOnce sync.Once
}

/**
 * Creates a new TTLMap with the given maximum TTL.
 * The TTLMap will automatically remove expired trace ids from the map, every second.
 * Map is thread-safe.
 */
func New(initSize int, maxTtl int) (m *TTLMap) {
	m = &TTLMap{
		m:      make(map[string]int64, initSize),
		maxTtl: int64(maxTtl),
		stopCh: make(chan struct{}),
	}

	go func() {
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				var expiredKeys []string
				now := time.Now().Unix()

				m.mu.RLock()
				for key, value := range m.m {
					if value < now {
						expiredKeys = append(expiredKeys, key)
					}
				}
				m.mu.RUnlock()

				for _, key := range expiredKeys {
					m.mu.Lock()
					delete(m.m, key)
					m.mu.Unlock()
				}
			case <-m.stopCh:
				return
			}
		}
	}()

	return
}

/**
 * Stops all go routines.
 * Should be called when the TTLMap is no longer needed.
 */
func (m *TTLMap) Stop() {
	m.stopOnce.Do(func() {
		close(m.stopCh)
	})
}

/**
 * Adds a trace id to the map.
 * When providing the same trace id twice, the second invocation will be ignored.
 */
func (m *TTLMap) Add(key string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	_, ok := m.m[key]
	if ok {
		return
	}

	m.m[key] = time.Now().Unix() + m.maxTtl
}

/**
 * Checks if a trace id exists in the map.
 * Returns true if the trace id exists and has not expired.
 * Returns false if the trace id does not exist or has expired.
 * Removes the trace id from the map if it has expired.
 */
func (m *TTLMap) Exists(key string) bool {
	_, ok := m.Get(key)
	return ok
}

/**
 * Inserts a new entry into the map.
 * Only used for testing.
 */
func (m *TTLMap) insertEntry(key string, value int64) {
	m.mu.Lock()
	m.m[key] = value
	m.mu.Unlock()
}

/**
 * Gets an entry from the map.
 */
func (m *TTLMap) Get(key string) (int64, bool) {
	m.mu.RLock()
	value, ok := m.m[key]
	m.mu.RUnlock()

	if ok {
		if value < time.Now().Unix() {
			m.mu.Lock()
			delete(m.m, key)
			m.mu.Unlock()
			return 0, false
		}
	}

	return value, ok
}

/**
 * Gets an entry from the map.
 * Only used for testing.
 */
func (m *TTLMap) getEntry(key string) (int64, bool) {
	m.mu.RLock()
	value, ok := m.m[key]
	m.mu.RUnlock()
	return value, ok
}

/**
 * Gets the size of the map.
 * Only used for testing.
 */
func (m *TTLMap) getSize() int {
	m.mu.RLock()
	size := len(m.m)
	m.mu.RUnlock()

	return size
}
