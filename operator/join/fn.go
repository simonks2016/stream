package join

import (
	"fmt"

	"github.com/simonks2016/stream/stream"
)

// 这里不自己算 watermark，只尊重上游塞进来的 WatermarkTs
func (j *JoinOperatorImpl) isLate(msg stream.Message[any]) bool {
	if msg.WatermarkTs == 0 {
		return false
	}
	return msg.Ts < msg.WatermarkTs
}

func (j *JoinOperatorImpl) cleanupLocked(currentWM int64) {
	if currentWM == 0 {
		return
	}

	for key, st := range j.state {
		if st == nil {
			delete(j.state, key)
			continue
		}
		if j.ready(st) {
			continue
		}

		latestTs := maxStateTs(st)
		if latestTs < currentWM {
			delete(j.state, key)
		}
	}
}

func minWatermark(msgs []stream.Message[any]) int64 {
	if len(msgs) == 0 {
		return 0
	}

	min1 := msgs[0].WatermarkTs
	for i := 1; i < len(msgs); i++ {
		if msgs[i].WatermarkTs < min1 {
			min1 = msgs[i].WatermarkTs
		}
	}
	return min1
}

func maxStateTs(st *State) int64 {
	var maxTs int64
	for _, msg := range st.Messages {
		if msg.Ts > maxTs {
			maxTs = msg.Ts
		}
	}
	return maxTs
}

func endpointID(ep stream.Endpoint) string {
	return fmt.Sprintf("%d:%s:%s", ep.Kind, ep.EndpointSourceId, ep.Name)
}

func (j *JoinOperatorImpl) ready(st *State) bool {
	if st == nil {
		return false
	}
	if len(st.Messages) < len(j.inputs) {
		return false
	}

	for _, ep := range j.inputs {
		if _, ok := st.Messages[endpointID(ep)]; !ok {
			return false
		}
	}
	return true
}
