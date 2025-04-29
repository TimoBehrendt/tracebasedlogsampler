package traceIdTtlMap

import (
	"strconv"
	"testing"
)

func BenchmarkTTLMap_AddExists(b *testing.B) {
	m := New(1000, 10)
	defer m.Stop()

	b.Run("Add", func(b *testing.B) {
		for i := 0; b.Loop(); i++ {
			m.Add(strconv.Itoa(i))
		}
	})

	for i := range 1000 {
		m.Add(strconv.Itoa(i))
	}

	b.Run("Exists", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			key := strconv.Itoa(i % 1000)
			m.Exists(key)
		}
	})
}

func BenchmarkTTLMap_Concurrent(b *testing.B) {
	m := New(1000, 10)
	defer m.Stop()

	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			key := strconv.Itoa(i % 1000)
			m.Add(key)
			m.Exists(key)
			i++
		}
	})
}
