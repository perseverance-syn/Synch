package supervisor

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"sync"
	"time"
)

type Supervisor struct {
	cfg                   Config
	sink                  Sink
	mu                    sync.Mutex
	runs                  map[string]*Proc
	recovering            map[string]bool
	recovered             map[string]ExitInfo
	pressure              Pressure
	closed, lastAnnounced bool
	sample                sync.Mutex
	stop, stopped         chan struct{}
}

// --- Initialization and pressure monitoring (Q-18) ---

// New initializes output storage, samples the data volume and starts the pressure monitor.
func New(cfg Config, sink Sink) (*Supervisor, error) {
	if sink == nil || cfg.DataDir == "" {
		return nil, errors.New("sink and data directory required")
	}
	if err := os.MkdirAll(cfg.DataDir, 0700); err != nil {
		return nil, fmt.Errorf("create output directory: %w", err)
	}
	if cfg.FreeSpace == nil {
		cfg.FreeSpace = freeSpace
	}
	if cfg.Threshold == nil {
		cfg.Threshold = func(n uint64) uint64 { return max(2<<30, (n/20)+boolUint(n%20 != 0)) }
	}
	if cfg.PollInterval <= 0 {
		cfg.PollInterval = time.Minute
	}
	if cfg.DrainTimeout <= 0 {
		cfg.DrainTimeout = 2 * time.Second
	}
	if cfg.Grace <= 0 {
		cfg.Grace = time.Second
	}
	s := &Supervisor{cfg: cfg, sink: sink, runs: map[string]*Proc{}, recovering: map[string]bool{}, recovered: map[string]ExitInfo{}, stop: make(chan struct{}), stopped: make(chan struct{})}
	if err := s.CheckPressure(); err != nil {
		return nil, err
	}
	go func() {
		defer close(s.stopped)
		t := time.NewTicker(cfg.PollInterval)
		defer t.Stop()
		for {
			select {
			case <-s.stop:
				return
			case <-t.C:
				if err := s.CheckPressure(); err != nil {
					slog.Error("disk pressure check", "error", err)
				}
			}
		}
	}()
	return s, nil
}

// boolUint converts a condition to zero or one for rounding up the disk threshold.
func boolUint(b bool) uint64 {
	if b {
		return 1
	}
	return 0
}

// CheckPressure samples available space and persists pressure transitions with hysteresis.
func (s *Supervisor) CheckPressure() error {
	// Serialize sample and emit across callers. Acquire sample before mu and release mu before calling the sink.
	s.sample.Lock()
	defer s.sample.Unlock()
	free, total, err := s.cfg.FreeSpace(s.cfg.DataDir)
	if err != nil {
		return fmt.Errorf("sample data volume: %w", err)
	}
	threshold := s.cfg.Threshold(total)
	s.mu.Lock()
	old := s.pressure.On
	s.mu.Unlock()
	on := free < threshold || old && free/2 < threshold
	p := Pressure{on, free, threshold}
	// Close the gate immediately on low space. Reopen after the off fact is durable.
	if on {
		s.mu.Lock()
		s.pressure = p
		s.mu.Unlock()
	}
	// A failed announcement leaves lastAnnounced intact so the next successful sample can retry it.
	if on != s.lastAnnounced {
		if err := s.sink.Emit(Fact{Type: "resource.pressure", Pressure: &p}); err != nil {
			return fmt.Errorf("persist disk pressure: %w", err)
		}
		s.lastAnnounced = on
	}
	s.mu.Lock()
	s.pressure = p
	s.mu.Unlock()
	return nil
}

// --- Process admission and shutdown (execution-driver ?2, T30) ---

// Close stops admission and monitoring, then waits for the managed process trees.
func (s *Supervisor) Close() error {
	s.mu.Lock()
	if !s.closed {
		s.closed = true
		close(s.stop)
	}
	var roots []*Proc
	for _, p := range s.runs {
		if p.parent == nil {
			roots = append(roots, p)
		}
	}
	s.mu.Unlock()
	var err error
	for _, p := range roots {
		p.request("cancelled")
		<-p.done
		err = errors.Join(err, p.err)
	}
	<-s.stopped
	return err
}

// Spawn starts a contained process and registers it under its managed parent, if supplied.
func (s *Supervisor) Spawn(ctx context.Context, spec Spec) (*Proc, error) {
	// Keep the admission check, parent check and registration atomic against cancellation and recovery.
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if s.closed {
		return nil, ErrClosed
	}
	if s.pressure.On {
		return nil, &PressureError{s.pressure}
	}
	if spec.ID == "" {
		return nil, errors.New("run ID required")
	}
	if s.runs[spec.ID] != nil || s.recovering[spec.ID] {
		return nil, ErrDuplicate
	}
	parent := s.runs[spec.ParentID]
	if spec.ParentID != "" && (parent == nil || parent.closing) {
		return nil, ErrParent
	}
	if spec.StallTimeout <= 0 {
		spec.StallTimeout = 10 * time.Minute
	}
	if cp := spec.Work.CheckpointRevision; cp != nil {
		v := *cp
		spec.Work.CheckpointRevision = &v
	}
	cmd := exec.Command(spec.Path, spec.Args...)
	cmd.Dir = spec.Dir
	cmd.Env = spec.Env
	p := &Proc{s: s, spec: spec, parent: parent, cmd: cmd, done: make(chan struct{}), wake: make(chan struct{}, 1), subs: map[chan OutputChunk]struct{}{}, last: time.Now()}
	if err := p.startWithStreams(); err != nil {
		return nil, err
	}
	s.runs[spec.ID] = p
	go p.run(ctx)
	return p, nil
}

// --- Recovery of interrupted work (thread-state-machine ?5) ---

// Recover classifies missing runs and returns matching live records for caller disposition.
// Live records without creation times are returned as unverified.
func (s *Supervisor) Recover(previous []RunRecord) (Recovery, error) {
	var out Recovery
	for _, r := range previous {
		s.mu.Lock()
		_, done := s.recovered[r.ID]
		busy := s.recovering[r.ID] || s.runs[r.ID] != nil
		if !done && !busy {
			s.recovering[r.ID] = true
		}
		s.mu.Unlock()
		if done {
			continue
		}
		if busy {
			return out, ErrDuplicate
		}
		// Hold the run ID reservation across the OS probe and sink write without holding mu.
		birth, live, err := identify(r.PID)
		if err == nil && live {
			switch {
			case r.CreationTime == "":
				out.Unverified = append(out.Unverified, r)
			case birth == r.CreationTime:
				out.Unmanaged = append(out.Unmanaged, r)
			default:
				live = false
			}
		}
		info := ExitInfo{ExecutionOutcome: "lost_on_reboot", Work: Classify(false, r.Work), ExitCode: -1, CleanupReason: "original process tree unavailable during recovery"}
		if err == nil && !live {
			err = s.sink.Emit(Fact{Type: "process.exited", RunID: r.ID, ThreadID: r.ThreadID, Exit: &info})
		}
		s.mu.Lock()
		delete(s.recovering, r.ID)
		if err == nil && !live {
			s.recovered[r.ID] = info
			out.Exited = append(out.Exited, info)
		}
		s.mu.Unlock()
		if err != nil {
			return out, fmt.Errorf("recover run %s: %w", r.ID, err)
		}
	}
	return out, nil
}
