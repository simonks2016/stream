package watermark

import "sync"

type Watermark struct {
	mu sync.Mutex

	// 每个 source（流）看到的最大事件时间
	maxSeenBySource map[string]int64

	// 允许迟到时间（毫秒）
	allowedLateness int64
}

func NewWatermark(allowedLateness int64) *Watermark {
	return &Watermark{
		maxSeenBySource: make(map[string]int64),
		allowedLateness: allowedLateness,
	}
}

// Update 根据新消息推进 watermark，并返回当前 watermark
func (w *Watermark) Update(sourceSymbol string, ts int64) int64 {
	w.mu.Lock()
	defer w.mu.Unlock()

	// 更新该 source 的最大事件时间
	if ts > w.maxSeenBySource[sourceSymbol] {
		w.maxSeenBySource[sourceSymbol] = ts
	}
	return w.currentLocked()
}

// currentLocked
func (w *Watermark) currentLocked() int64 {
	if len(w.maxSeenBySource) == 0 {
		return 0
	}

	var min int64 = -1

	for _, ts := range w.maxSeenBySource {
		if min == -1 || ts < min {
			min = ts
		}
	}
	// watermark = 所有流中最慢的那个时间 - lateness
	return min - w.allowedLateness
}

// Current 获取当前 Watermark
func (w *Watermark) Current() int64 {
	w.mu.Lock()
	defer w.mu.Unlock()

	return w.currentLocked()
}
