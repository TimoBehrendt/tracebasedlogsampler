package traceIdTtlMap

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestNew(t *testing.T) {
	m := New(10, 10)
	defer m.Stop()

	assert.Equal(t, 0, m.getSize())
}

func TestNew_Cleanup(t *testing.T) {
	m := New(10, 10)
	defer m.Stop()

	m.insertEntry("4bf92f3577b34da6a3ce929d0e0e4736", time.Now().Unix()-10)
	assert.Equal(t, 1, m.getSize())

	// Inserted entry should be deleted after >1 second
	time.Sleep(time.Second * 2)
	assert.Equal(t, 0, m.getSize())
}

func TestAdd(t *testing.T) {
	m := New(10, 10)
	defer m.Stop()

	m.Add("4bf92f3577b34da6a3ce929d0e0e4736")
	// Intentionally adding the same trace id twice, should not add it again
	m.Add("4bf92f3577b34da6a3ce929d0e0e4736")
	m.Add("d0240fe9f68b48e687d25c185d4c17c5")

	assert.Equal(t, 2, m.getSize())

	_, ok := m.getEntry("4bf92f3577b34da6a3ce929d0e0e4736")
	assert.True(t, ok)
	_, ok = m.getEntry("d0240fe9f68b48e687d25c185d4c17c5")
	assert.True(t, ok)
}

func TestAdd_ResetTtl(t *testing.T) {
	m := New(10, 10)
	defer m.Stop()

	m.Add("4bf92f3577b34da6a3ce929d0e0e4736")
	insertTime, _ := m.getEntry("4bf92f3577b34da6a3ce929d0e0e4736")

	time.Sleep(time.Second)

	m.Add("4bf92f3577b34da6a3ce929d0e0e4736")
	updatedTime, _ := m.getEntry("4bf92f3577b34da6a3ce929d0e0e4736")

	// Delete time of the second entry should remain the same.
	assert.Equal(t, updatedTime, insertTime)
}

func TestExists(t *testing.T) {
	m := New(10, 10)
	defer m.Stop()

	m.insertEntry("4bf92f3577b34da6a3ce929d0e0e4736", time.Now().Unix()+10)

	// Existing and valid trace
	assert.True(t, m.Exists("4bf92f3577b34da6a3ce929d0e0e4736"))
	// Non existing trace
	assert.False(t, m.Exists("d0240fe9f68b48e687d25c185d4c17c5"))
}

func TestExists_ExpiredTrace(t *testing.T) {
	m := New(10, 10)
	defer m.Stop()

	m.insertEntry("4bf92f3577b34da6a3ce929d0e0e4736", time.Now().Unix()-10)

	// Existing and but invalid trace
	assert.False(t, m.Exists("4bf92f3577b34da6a3ce929d0e0e4736"))
}

func TestAddExists_Concurrent(t *testing.T) {
	m := New(10, 10)
	defer m.Stop()

	var wg sync.WaitGroup
	keys := []string{"k1", "k2", "k3", "k4", "k5", "k6", "k7", "k8", "k9", "k10"}

	for i := range 100 {
		wg.Add(2)

		go func(i int) {
			defer wg.Done()
			m.Add(keys[i%len(keys)])
		}(i)

		go func(i int) {
			defer wg.Done()
			_ = m.Exists(keys[i%len(keys)])
		}(i)
	}

	wg.Wait()
}

func TestStop(t *testing.T) {
	m := New(10, 10)
	m.Stop()
	m.Stop()
}
