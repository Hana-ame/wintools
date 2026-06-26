package relay

import (
	"context"
	"sync"
	"time"
)

type entry struct {
	value   any
	created time.Time
	broadCh chan struct{}
}

type Relay struct {
	mu   sync.Mutex
	m    map[string]*entry
	ttl  time.Duration
	done chan struct{}
}

func NewRelay(ttl time.Duration) *Relay {
	r := &Relay{
		m:    make(map[string]*entry),
		ttl:  ttl,
		done: make(chan struct{}),
	}
	if ttl > 0 {
		go r.evictLoop()
	}
	return r
}

func (r *Relay) Stop() {
	close(r.done)
}

func (r *Relay) Put(id string, value any) {
	r.mu.Lock()
	defer r.mu.Unlock()

	e, ok := r.m[id]
	if !ok {
		e = &entry{broadCh: make(chan struct{})}
		r.m[id] = e
	}
	e.value = value
	e.created = time.Now()
	close(e.broadCh)
}

func (r *Relay) Get(ctx context.Context, id string) (any, bool) {
	r.mu.Lock()
	e, ok := r.m[id]
	if ok {
		delete(r.m, id)
	}
	r.mu.Unlock()

	if ok {
		return e.value, true
	}

	select {
	case <-ctx.Done():
		return nil, false
	default:
		return nil, false
	}
}

func (r *Relay) GetWait(ctx context.Context, id string) (any, bool) {
	r.mu.Lock()
	e, ok := r.m[id]
	if ok {
		delete(r.m, id)
		r.mu.Unlock()
		return e.value, true
	}

	e = &entry{broadCh: make(chan struct{})}
	r.m[id] = e
	broadCh := e.broadCh
	r.mu.Unlock()

	select {
	case <-broadCh:
		r.mu.Lock()
		entry, exists := r.m[id]
		if exists {
			delete(r.m, id)
			r.mu.Unlock()
			return entry.value, true
		}
		r.mu.Unlock()
		return nil, false
	case <-ctx.Done():
		return nil, false
	}
}

func (r *Relay) evictLoop() {
	tick := time.NewTicker(r.ttl / 2)
	defer tick.Stop()
	for {
		select {
		case <-r.done:
			return
		case now := <-tick.C:
			r.mu.Lock()
			for id, e := range r.m {
				if now.After(e.created.Add(r.ttl)) {
					delete(r.m, id)
					close(e.broadCh)
				}
			}
			r.mu.Unlock()
		}
	}
}
