package rolldingWindow

import (
	"context"
	"sync"
	"time"
)

type windowKey struct {
	WindowStartMs int64
	WindowEndMs   int64
}

type bucketItem[in any] struct {
	tsMs int64
	data in
}

type rollingBucket[in any] struct {
	mu              sync.RWMutex
	items           []bucketItem[in]
	windowDuration  time.Duration
	cleanupInterval time.Duration
	retention       time.Duration
}

func newRollingBucket[in any](windowDuration time.Duration, cleanupInterval time.Duration) *rollingBucket[in] {
	if cleanupInterval <= 0 {
		cleanupInterval = windowDuration
	}
	return &rollingBucket[in]{
		items:           make([]bucketItem[in], 0, 1024),
		windowDuration:  windowDuration,
		cleanupInterval: cleanupInterval,
		retention:       windowDuration * 2,
	}
}

func (b *rollingBucket[in]) add(tsMs int64, data in) {
	b.mu.Lock()
	b.items = append(b.items, bucketItem[in]{
		tsMs: tsMs,
		data: data,
	})
	b.mu.Unlock()
}

func (b *rollingBucket[in]) get(wk windowKey) []in {
	b.mu.RLock()
	defer b.mu.RUnlock()

	out := make([]in, 0)
	for _, item := range b.items {
		if item.tsMs >= wk.WindowStartMs && item.tsMs < wk.WindowEndMs {
			out = append(out, item.data)
		}
	}
	return out
}

func (b *rollingBucket[in]) cleanup(nowMs int64) {
	expireBefore := nowMs - b.retention.Milliseconds()

	b.mu.Lock()
	defer b.mu.Unlock()

	idx := 0
	for _, item := range b.items {
		if item.tsMs >= expireBefore {
			b.items[idx] = item
			idx++
		}
	}

	var zero bucketItem[in]
	for i := idx; i < len(b.items); i++ {
		b.items[i] = zero
	}

	b.items = b.items[:idx]
}

func (b *rollingBucket[in]) run(ctx context.Context) error {
	ticker := time.NewTicker(b.cleanupInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case now := <-ticker.C:
			b.cleanup(now.UnixMilli())
		}
	}
}
