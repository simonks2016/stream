package scheduler

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/simonks2016/stream/stream"
)

type DefaultScheduler struct {
	mu       sync.RWMutex
	pipeline stream.Pipeline
	jobs     []stream.SchedulerJob

	cancel  context.CancelFunc
	wg      sync.WaitGroup
	running bool
}

func NewScheduler() *DefaultScheduler {
	return &DefaultScheduler{
		jobs: make([]stream.SchedulerJob, 0),
	}
}

func (s *DefaultScheduler) Register(pipeline stream.Pipeline) stream.Scheduler {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.pipeline = pipeline
	return s
}

func (s *DefaultScheduler) On(jobs ...stream.SchedulerJob) stream.Scheduler {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.jobs = append(s.jobs, jobs...)
	return s
}

func (s *DefaultScheduler) Run(ctx context.Context) {
	s.mu.Lock()
	if s.running {
		s.mu.Unlock()
		return
	}
	if s.pipeline == nil {
		s.mu.Unlock()
		panic("scheduler: pipeline is nil")
	}

	runCtx, cancel := context.WithCancel(ctx)
	s.cancel = cancel
	s.running = true

	jobs := append([]stream.SchedulerJob(nil), s.jobs...)
	s.mu.Unlock()

	for _, job := range jobs {
		if job == nil {
			continue
		}
		if job.Interval() <= 0 {
			log.Printf("[scheduler] skip job=%s, invalid interval=%s", job.Name(), job.Interval())
			continue
		}

		s.wg.Go(func() {
			s.runJob(runCtx, job)
		})
	}
}

func (s *DefaultScheduler) runJob(ctx context.Context, job stream.SchedulerJob) {
	
	ticker := time.NewTicker(job.Interval())
	defer ticker.Stop()

	log.Printf("[scheduler] job started: name=%s interval=%s target=%s",
		job.Name(), job.Interval(), job.Target().Name)

	for {
		select {
		case <-ctx.Done():
			log.Printf("[scheduler] job stopped: name=%s", job.Name())
			return

		case <-ticker.C:
			msg := job.BuildMessage()
			targetEndPoint := job.Target()

			// 如果消息时间没带，这里补一下
			if msg.Ts == 0 {
				msg.Ts = time.Now().UnixMilli()
			}

			if err := s.publish(ctx, targetEndPoint, msg); err != nil {
				log.Printf("[scheduler] publish failed: job=%s target=%s err=%v",
					job.Name(), job.Target().Name, err)
				continue
			}

			log.Printf("[scheduler] job triggered: name=%s target=%s",
				job.Name(), job.Target().Name)
		}
	}
}

func (s *DefaultScheduler) publish(ctx context.Context, endpoint stream.Endpoint, msg stream.Message[any]) error {
	s.mu.RLock()
	p := s.pipeline
	s.mu.RUnlock()

	if p == nil {
		return fmt.Errorf("scheduler pipeline is nil")
	}

	return p.Publish(endpoint, msg)
}

func (s *DefaultScheduler) Stop() {
	s.mu.Lock()
	if !s.running {
		s.mu.Unlock()
		return
	}
	cancel := s.cancel
	s.running = false
	s.cancel = nil
	s.mu.Unlock()

	if cancel != nil {
		cancel()
	}

	s.wg.Wait()
}
