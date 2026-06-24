package kv

import "sync"

type entry struct {
	mu sync.RWMutex
	v  interface{}
}

var global struct {
	mu sync.RWMutex
	m  map[string]*entry
}

func init() {
	global.m = make(map[string]*entry)
}

func Put(id string, v interface{}) {
	global.mu.Lock()
	e, ok := global.m[id]
	if !ok {
		e = &entry{}
		global.m[id] = e
	}
	global.mu.Unlock()

	e.mu.Lock()
	e.v = v
	e.mu.Unlock()
}

func Get(id string) (interface{}, bool) {
	global.mu.RLock()
	e, ok := global.m[id]
	global.mu.RUnlock()
	if !ok {
		return nil, false
	}

	e.mu.RLock()
	v := e.v
	e.mu.RUnlock()
	return v, true
}

func Delete(id string) {
	global.mu.Lock()
	delete(global.m, id)
	global.mu.Unlock()
}

func Keys() []string {
	global.mu.RLock()
	keys := make([]string, 0, len(global.m))
	for k := range global.m {
		keys = append(keys, k)
	}
	global.mu.RUnlock()
	return keys
}

func Len() int {
	global.mu.RLock()
	n := len(global.m)
	global.mu.RUnlock()
	return n
}
