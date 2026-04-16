package join

import (
	"eventBus/stream"
	"time"
)

func (j *JoinOperatorImpl) isLate(msg stream.Message[any]) bool {
	if time.Now().UnixMilli()-msg.WatermarkTs > j.allowedLateness {
		return true
	}
	return false
}
