package kv

import (
    "testing"
    "time"
)

func BenchmarkStoreSetGet(b *testing.B) {
    store := NewStore(10*time.Second, 1*time.Second)
    defer store.Stop()
    key := "bench"
    val := map[string]any{"x": 123}
    b.ResetTimer()
    for i := 0; i < b.N; i++ {
        store.Set(key, val)
        if _, ok := store.Peek(key); !ok {
            b.Fatal("peek failed")
        }
    }
}
