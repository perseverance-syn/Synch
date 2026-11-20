package supervisor

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
	"unicode/utf8"
)

// --- Subprocess fixtures and test utilities ---

// TestHelper supplies subprocess modes for AC1, AC2, AC4 and AC6.
func TestHelper(t *testing.T) {
	mode := os.Getenv("SYNCH_HELPER")
	if mode == "" {
		return
	}
	switch mode {
	case "idle":
		time.Sleep(time.Minute)
	case "echo":
		if _, err := io.Copy(os.Stdout, os.Stdin); err != nil {
			os.Exit(4)
		}
	case "large":
		if _, err := os.Stdout.Write(bytes.Repeat([]byte("界"), 30000)); err != nil {
			os.Exit(4)
		}
		if _, err := fmt.Fprint(os.Stderr, "error stream"); err != nil {
			os.Exit(4)
		}
	case "tree":
		cmd := exec.Command(os.Args[0], "-test.run=^TestHelper$")
		cmd.Env = append(os.Environ(), "SYNCH_HELPER=idle")
		if err := cmd.Start(); err != nil {
			os.Exit(3)
		}
		if _, err := fmt.Fprintln(os.Stdout, cmd.Process.Pid); err != nil {
			os.Exit(4)
		}
		time.Sleep(time.Minute)
	}
	os.Exit(0)
}

type recorder struct {
	mu    sync.Mutex
	facts []Fact
	hook  func(Fact) error
}

// Emit captures accepted facts and lets tests intercept persistence before recording.
func (r *recorder) Emit(f Fact) error {
	if r.hook != nil {
		if err := r.hook(f); err != nil {
			return err
		}
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.facts = append(r.facts, f)
	return nil
}

// of returns a snapshot of accepted facts of the requested type.
func (r *recorder) of(kind string) []Fact {
	r.mu.Lock()
	defer r.mu.Unlock()
	var result []Fact
	for _, f := range r.facts {
		if f.Type == kind {
			result = append(result, f)
		}
	}
	return result
}

// setup starts a supervisor with an isolated data directory and controllable disk space.
func setup(t *testing.T, r *recorder, opts ...func(*Config)) *Supervisor {
	t.Helper()
	cfg := Config{DataDir: t.TempDir(), Grace: 30 * time.Millisecond, FreeSpace: func(string) (uint64, uint64, error) { return 10 << 30, 20 << 30, nil }}
	for _, opt := range opts {
		opt(&cfg)
	}
	s, err := New(cfg, r)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := s.Close(); err != nil {
			t.Error(err)
		}
	})
	return s
}

// spec builds the command spec for a named subprocess fixture mode.
func spec(id, mode string) Spec {
	return Spec{ID: id, ThreadID: "thread", Path: os.Args[0], Args: []string{"-test.run=^TestHelper$"}, Env: append(os.Environ(), "SYNCH_HELPER="+mode, "GORACE=atexit_sleep_ms=0")}
}

// spawn starts a fixture process and fails the test if startup fails.
func spawn(t *testing.T, s *Supervisor, sp Spec) *Proc {
	t.Helper()
	p, err := s.Spawn(context.Background(), sp)
	if err != nil {
		t.Fatal(err)
	}
	return p
}

// wait waits for a fixture to finish with a test deadline.
func wait(t *testing.T, p *Proc) (ExitInfo, error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	return p.Wait(ctx)
}

// --- Output and cancellation (AC1?AC4, AC8) ---

// TestPersistBeforeForward proves AC1 by blocking and failing the sink before live delivery.
func TestPersistBeforeForward(t *testing.T) {
	for _, fail := range []bool{false, true} {
		t.Run(fmt.Sprint(fail), func(t *testing.T) {
			entered := make(chan struct{})
			release := make(chan struct{})
			bad := errors.New("sink failure")
			r := &recorder{hook: func(f Fact) error {
				if f.Output != nil {
					close(entered)
					<-release
					if fail {
						return bad
					}
				}
				return nil
			}}
			s := setup(t, r)
			p := spawn(t, s, spec("a", "echo"))
			ch, unsub := p.Subscribe(8)
			defer unsub()
			_, err := p.WriteInput([]byte("hello"))
			if err != nil {
				t.Fatal(err)
			}
			if err := p.CloseInput(); err != nil {
				t.Fatal(err)
			}
			select {
			case <-entered:
			case <-time.After(5 * time.Second):
				close(release)
				t.Fatal("sink never entered")
			}
			select {
			case <-ch:
				close(release)
				t.Fatal("forwarded early")
			default:
			}
			close(release)
			info, err := wait(t, p)
			if info.ExecutionOutcome != "exited" || info.Work != "complete" || !info.DescendantsCleaned || !errors.Is(err, map[bool]error{true: bad}[fail]) {
				t.Fatal(info, err)
			}
			c, ok := <-ch
			if ok == fail || ok && c.Chunk != "hello" {
				t.Fatal(c, ok)
			}
			if len(r.of("process.exited")) != 1 {
				t.Fatal("exit count")
			}
		})
	}
}

// TestUTF8CapAndArtifact proves AC2 by checking UTF-8 boundaries and the artifact digest.
func TestUTF8CapAndArtifact(t *testing.T) {
	r := &recorder{}
	s := setup(t, r)
	p := spawn(t, s, spec("a", "large"))
	if _, err := wait(t, p); err != nil {
		t.Fatal(err)
	}
	var preview, stderr strings.Builder
	var art *Artifact
	for _, f := range r.of("engine.output") {
		c := f.Output
		if len(c.Chunk) > OutputCap || !utf8.ValidString(c.Chunk) {
			t.Fatal("invalid chunk")
		}
		if c.Stream == "stderr" {
			stderr.WriteString(c.Chunk)
			continue
		}
		preview.WriteString(c.Chunk)
		if c.Truncated {
			art = c.FullOutput
		}
	}
	raw := bytes.Repeat([]byte("界"), 30000)
	if stderr.String() != "error stream" || preview.String() != string(raw[:65535]) || art == nil {
		t.Fatal("bad preview or artifact")
	}
	u, err := url.Parse(art.URI)
	if err != nil || u.Scheme != "file" {
		t.Fatal(art)
	}
	path := filepath.FromSlash(u.Path)
	if filepath.Separator == '\\' {
		path = strings.TrimPrefix(path, "\\")
	}
	got, err := os.ReadFile(path)
	if err != nil || !bytes.Equal(raw, got) || art.Digest != fmt.Sprintf("sha256:%x", sha256.Sum256(raw)) {
		t.Fatal("artifact mismatch", err)
	}
}

// TestContextCancel proves AC3 by cancelling the spawn context and observing a cleaned exit.
func TestContextCancel(t *testing.T) {
	r := &recorder{}
	s := setup(t, r)
	ctx, cancel := context.WithCancel(context.Background())
	p, err := s.Spawn(ctx, spec("a", "idle"))
	if err != nil {
		t.Fatal(err)
	}
	cancel()
	i, err := wait(t, p)
	if err != nil || i.ExecutionOutcome != "cancelled" || !i.DescendantsCleaned || len(r.of("process.exited")) != 1 {
		t.Fatal(i, err)
	}
}

// TestSubscriberDisconnect proves AC8 by verifying work and output continue after a client disconnects.
func TestSubscriberDisconnect(t *testing.T) {
	r := &recorder{}
	s := setup(t, r)
	p := spawn(t, s, spec("detached", "echo"))
	_, disconnect := p.Subscribe(1)
	disconnect()
	if _, err := p.WriteInput([]byte("after disconnect")); err != nil {
		t.Fatal(err)
	}
	if err := p.CloseInput(); err != nil {
		t.Fatal(err)
	}
	if info, err := wait(t, p); err != nil || info.Work != "complete" {
		t.Fatal(info, err)
	}
	var output strings.Builder
	for _, f := range r.of("engine.output") {
		output.WriteString(f.Output.Chunk)
	}
	if output.String() != "after disconnect" {
		t.Fatal(output.String())
	}
}

// --- Stall, restart and work disposition (AC6?AC9) ---

// TestStall proves AC6 by testing fully idle, progress-only and output-only processes.
func TestStall(t *testing.T) {
	for _, activity := range []string{"idle", "progress", "output"} {
		t.Run(activity, func(t *testing.T) {
			s := setup(t, &recorder{})
			sp := spec("a", "echo")
			sp.StallTimeout = 350 * time.Millisecond
			p := spawn(t, s, sp)
			if activity != "idle" {
				end := time.Now().Add(3 * sp.StallTimeout)
				for time.Now().Before(end) {
					if activity == "output" {
						if _, err := p.WriteInput([]byte("x")); err != nil {
							t.Fatal(err)
						}
					} else {
						p.Progress()
					}
					select {
					case <-p.done:
						t.Fatal("premature stall")
					case <-time.After(40 * time.Millisecond):
					}
				}
			}
			i, err := wait(t, p)
			if err != nil || i.ExecutionOutcome != "stalled" {
				t.Fatal(i, err)
			}
		})
	}
}

// TestClassify proves AC7 with a matrix of exit, checkpoint and native session inputs.
func TestClassify(t *testing.T) {
	for _, normal := range []bool{true, false} {
		for _, rev := range []int{-1, 1, 2, 3} {
			for _, native := range []bool{true, false} {
				for _, ref := range []bool{true, false} {
					w := WorkInputs{InputRevision: 2, QualifiedNativeResume: native, KnownSessionRef: ref}
					if rev >= 0 {
						r := uint64(rev)
						w.CheckpointRevision = &r
					}
					want := "lost"
					if normal {
						want = "complete"
					} else if rev >= 2 || native && ref {
						want = "resumable"
					}
					if got := Classify(normal, w); got != want {
						t.Fatal(normal, rev, native, ref, got, want)
					}
				}
			}
		}
	}
}

// TestRecover proves AC8 by classifying a missing process exactly once while leaving a live PID alone.
func TestRecover(t *testing.T) {
	r := &recorder{}
	s := setup(t, r)
	p := spawn(t, s, spec("previous", "large"))
	if _, err := wait(t, p); err != nil {
		t.Fatal(err)
	}
	records := []RunRecord{{ID: "missing", PID: p.PID(), Work: WorkInputs{QualifiedNativeResume: true, KnownSessionRef: true}}, {ID: "live", PID: os.Getpid()}}
	infos, err := s.Recover(records)
	if err != nil || len(infos.Exited) != 1 || infos.Exited[0].ExecutionOutcome != "lost_on_reboot" || infos.Exited[0].Work != "resumable" {
		t.Fatal(infos, err)
	}
	infos, err = s.Recover(records)
	if err != nil || len(infos.Exited) != 0 || len(r.of("process.exited")) != 2 {
		t.Fatal(infos, err)
	}
}

// TestCancelManagedDescendants proves AC9 by observing cleanup and terminal facts from leaves to root.
func TestCancelManagedDescendants(t *testing.T) {
	r := &recorder{}
	s := setup(t, r)
	p := spawn(t, s, spec("root", "idle"))
	parent := p
	for _, id := range []string{"child", "grandchild"} {
		sp := spec(id, "idle")
		sp.ParentID = parent.ID()
		parent = spawn(t, s, sp)
	}
	if err := p.Cancel(context.Background()); err != nil {
		t.Fatal(err)
	}
	facts := r.of("process.exited")
	if len(facts) != 3 {
		t.Fatal(facts)
	}
	for idx, id := range []string{"grandchild", "child", "root"} {
		f := facts[idx]
		if f.RunID != id || !f.Exit.DescendantsCleaned || f.Exit.ExecutionOutcome != "cancelled" {
			t.Fatal(f)
		}
	}
}

// --- Disk pressure and hysteresis (AC10) ---

// TestPressure proves AC10 by checking admission, continued work and hysteresis under injected pressure.
func TestPressure(t *testing.T) {
	var free atomic.Uint64
	free.Store(100)
	r := &recorder{}
	s := setup(t, r, func(c *Config) {
		c.FreeSpace = func(string) (uint64, uint64, error) { return free.Load(), 100, nil }
		c.Threshold = func(uint64) uint64 { return 10 }
	})
	p := spawn(t, s, spec("root", "echo"))
	for _, v := range []uint64{9, 10, 19} {
		free.Store(v)
		if err := s.CheckPressure(); err != nil {
			t.Fatal(err)
		}
		_, err := s.Spawn(context.Background(), spec("blocked", "idle"))
		var pe *PressureError
		if !errors.As(err, &pe) {
			t.Fatal(err)
		}
	}
	if _, err := p.WriteInput([]byte("still running")); err != nil {
		t.Fatal(err)
	}
	if err := p.CloseInput(); err != nil {
		t.Fatal(err)
	}
	if i, err := wait(t, p); err != nil || i.Work != "complete" {
		t.Fatal(i, err)
	}
	free.Store(20)
	if err := s.CheckPressure(); err != nil {
		t.Fatal(err)
	}
	spawn(t, s, spec("accepted", "idle"))
	events := r.of("resource.pressure")
	if len(events) != 2 || !events[0].Pressure.On || events[1].Pressure.On {
		t.Fatal(events)
	}
}

// TestPressureMonitor proves AC10 by exercising timed sampling and the default thresholds.
func TestPressureMonitor(t *testing.T) {
	var free atomic.Uint64
	const total = 100 << 30
	free.Store(6 << 30)
	r := &recorder{}
	s := setup(t, r, func(c *Config) {
		c.PollInterval = 5 * time.Millisecond
		c.FreeSpace = func(string) (uint64, uint64, error) { return free.Load(), total, nil }
	})
	for index, n := range []uint64{5<<30 - 1, 10 << 30} {
		free.Store(n)
		deadline := time.Now().Add(time.Second)
		for len(r.of("resource.pressure")) <= index && time.Now().Before(deadline) {
			time.Sleep(time.Millisecond)
		}
		facts := r.of("resource.pressure")
		if len(facts) != index+1 || facts[index].Pressure.On != (index == 0) || facts[index].Pressure.Threshold != 5<<30 {
			t.Fatalf("unexpected pressure facts: %v", facts)
		}
	}
	if got := s.cfg.Threshold(10 << 30); got != 2<<30 {
		t.Fatalf("minimum threshold = %d", got)
	}
}

// --- Platform containment and review regressions (AC2, AC4, AC8) ---

// verifyTree proves AC4 on the current platform by checking a real grandchild before and after cancellation.
func verifyTree(t *testing.T) {
	r := &recorder{}
	s := setup(t, r)
	p := spawn(t, s, spec("tree", "tree"))
	deadline := time.Now().Add(5 * time.Second)
	var data strings.Builder
	var pid int
	for time.Now().Before(deadline) {
		data.Reset()
		for _, f := range r.of("engine.output") {
			data.WriteString(f.Output.Chunk)
		}
		if strings.Contains(data.String(), "\n") {
			pid, _ = strconv.Atoi(strings.TrimSpace(data.String()))
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if pid == 0 {
		t.Fatal("grandchild PID unavailable", data.String())
	}
	if live, err := processAlive(pid); !live || err != nil {
		t.Fatal("grandchild did not start", live, err)
	}
	if err := p.Cancel(context.Background()); err != nil {
		t.Fatal(err)
	}
	if live, err := processAlive(pid); live || err != nil {
		t.Fatal("grandchild survived", live, err)
	}
	if !p.info.DescendantsCleaned {
		t.Fatal(p.info)
	}
}

// TestRecoverReusedPID proves AC8 and G2?G3 by distinguishing reused, matching and unverified identities.
func TestRecoverReusedPID(t *testing.T) {
	r := &recorder{}
	s := setup(t, r)
	other := setup(t, &recorder{})
	live := spawn(t, other, spec("unmanaged", "idle")).Record()
	if live.CreationTime == "" {
		t.Fatal("missing creation time")
	}
	reused := live
	reused.ID = "reused"
	reused.CreationTime += "-different"
	legacy := live
	legacy.ID = "legacy"
	legacy.CreationTime = ""
	out, err := s.Recover([]RunRecord{reused, live, legacy})
	if err != nil {
		t.Fatal(err)
	}
	if len(out.Unmanaged) != 1 || out.Unmanaged[0] != live || len(out.Unverified) != 1 || out.Unverified[0] != legacy || len(out.Exited) != 1 {
		t.Fatal(out)
	}
	facts := r.of("process.exited")
	if len(facts) != 1 || facts[0].RunID != "reused" || facts[0].Exit.ExecutionOutcome != "lost_on_reboot" || facts[0].Exit.DescendantsCleaned || facts[0].Exit.CleanupReason == "" {
		t.Fatal(facts)
	}
}

// TestCutRangeBeforeEOF proves AC2 and D-07 by verifying the cut reference and later output before EOF.
func TestCutRangeBeforeEOF(t *testing.T) {
	r := &recorder{}
	s := setup(t, r)
	p := spawn(t, s, spec("cut", "echo"))
	raw := strings.Repeat("a", 70000) + "\nTAIL"
	if _, err := p.WriteInput([]byte(raw)); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		facts := r.of("engine.output")
		var preview strings.Builder
		covered := 0
		cuts := 0
		for _, f := range facts {
			c := f.Output
			preview.WriteString(c.Chunk)
			if !c.Truncated {
				if c.FullOutput != nil {
					t.Fatal("unexpected reference")
				}
				continue
			}
			if cuts == 0 && c.Chunk == "" {
				t.Fatal("cut preview was already forwarded unflagged")
			}
			cuts++
			if c.FullOutput == nil {
				t.Fatal("cut without reference")
			}
			u, err := url.Parse(c.FullOutput.URI)
			if err != nil {
				t.Fatal(err)
			}
			end, err := strconv.Atoi(strings.TrimPrefix(u.Fragment, "bytes=0-"))
			if err != nil || end > len(raw) || end <= OutputCap || end < covered {
				t.Fatal(u.Fragment)
			}
			path := filepath.FromSlash(u.Path)
			if filepath.Separator == '\\' {
				path = strings.TrimPrefix(path, "\\")
			}
			data, err := os.ReadFile(path)
			if err != nil || len(data) < end || string(data[:end]) != raw[:end] || fmt.Sprintf("sha256:%x", sha256.Sum256(data[:end])) != c.FullOutput.Digest {
				t.Fatal("invalid range", err)
			}
			covered = end
		}
		if strings.HasSuffix(preview.String(), "TAIL") {
			if preview.String() != raw[:OutputCap]+"\nTAIL" || covered < 70000 {
				t.Fatal("missing preview or range", covered)
			}
			if err := p.CloseInput(); err != nil {
				t.Fatal(err)
			}
			if _, err := wait(t, p); err != nil {
				t.Fatal(err)
			}
			if len(r.of("engine.output")) != len(facts) {
				t.Fatal("unexpected EOF marker")
			}
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("cut and tail did not arrive before EOF")
}
