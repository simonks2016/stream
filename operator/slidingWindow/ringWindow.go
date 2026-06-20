package slidingWindow

type ringWindow[T any] struct {
	start int
	count int
	buf   []T
}

func (w *ringWindow[T]) add(value T) (full bool, slid bool) {
	// 还没填满
	if w.count < len(w.buf) {
		idx := (w.start + w.count) % len(w.buf)
		w.buf[idx] = value
		w.count++

		return w.count == len(w.buf), false
	}

	// 已经满了 -> 开始滑动
	w.buf[w.start] = value
	w.start = (w.start + 1) % len(w.buf)

	return true, true
}

func (w *ringWindow[T]) values() []T {
	result := make([]T, 0, w.count)

	for i := 0; i < w.count; i++ {
		idx := (w.start + i) % len(w.buf)
		result = append(result, w.buf[idx])
	}

	return result
}
