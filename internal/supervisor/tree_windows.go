//go:build windows

package supervisor

import (
	"errors"
	"fmt"
	"os/exec"
	"syscall"
	"time"
	"unsafe"
)

var kernel = syscall.NewLazyDLL("kernel32.dll")
var (
	createJob    = kernel.NewProc("CreateJobObjectW")
	setJob       = kernel.NewProc("SetInformationJobObject")
	assignJob    = kernel.NewProc("AssignProcessToJobObject")
	queryJob     = kernel.NewProc("QueryInformationJobObject")
	killJob      = kernel.NewProc("TerminateJobObject")
	firstThread  = kernel.NewProc("Thread32First")
	nextThread   = kernel.NewProc("Thread32Next")
	openThread   = kernel.NewProc("OpenThread")
	resumeThread = kernel.NewProc("ResumeThread")
	ctrlEvent    = kernel.NewProc("GenerateConsoleCtrlEvent")
	diskSpace    = kernel.NewProc("GetDiskFreeSpaceExW")
)

type jobLimits struct {
	ProcessTime, JobTime   int64
	Flags                  uint32
	MinWorking, MaxWorking uintptr
	Active                 uint32
	Affinity               uintptr
	Priority, Scheduling   uint32
	IO                     [6]uint64
	Memory                 [4]uintptr
}
type threadEntry struct {
	Size, Usage, ID, Owner uint32
	Base, Delta            int32
	Flags                  uint32
}
type processTree struct {
	job      syscall.Handle
	pid      int
	creation string
}

// --- Job assignment before the primary thread starts (execution-driver ?2) ---

// resume finds and resumes the suspended child's primary thread after job assignment.
func resume(pid int) error {
	// The Go launcher retains the process handle, so enumerate threads to find the suspended primary thread.
	snap, err := syscall.CreateToolhelp32Snapshot(4, 0)
	if err != nil {
		return err
	}
	defer releaseHandle(snap)
	entry := threadEntry{Size: uint32(unsafe.Sizeof(threadEntry{}))}
	for proc := firstThread; ; proc = nextThread {
		ok, _, err := proc.Call(uintptr(snap), uintptr(unsafe.Pointer(&entry)))
		if ok == 0 {
			return fmt.Errorf("enumerate suspended thread: %w", err)
		}
		if entry.Owner != uint32(pid) {
			continue
		}
		h, _, err := openThread.Call(2, 0, uintptr(entry.ID))
		if h == 0 {
			return err
		}
		defer releaseHandle(syscall.Handle(h))
		prev, _, err := resumeThread.Call(h)
		if uint32(prev) == 0xffffffff {
			return err
		}
		return nil
	}
}

// startTree creates a kill-on-close job and assigns the child before allowing it to run.
func startTree(cmd *exec.Cmd) (processTree, error) {
	h, _, err := createJob.Call(0, 0)
	if h == 0 {
		return processTree{}, err
	}
	t := processTree{job: syscall.Handle(h)}
	success := false
	defer func() {
		if !success {
			releaseHandle(t.job)
		}
	}()
	// Kill-on-close makes the kernel terminate the job if the supervisor loses its last handle.
	limits := jobLimits{Flags: 0x2000}
	ok, _, err := setJob.Call(h, 9, uintptr(unsafe.Pointer(&limits)), unsafe.Sizeof(limits))
	if ok == 0 {
		return t, fmt.Errorf("set kill on close: %w", err)
	}
	// CREATE_SUSPENDED closes the start/assign race. The primary thread cannot spawn before assignment.
	cmd.SysProcAttr = &syscall.SysProcAttr{CreationFlags: 0x4 | 0x200, HideWindow: true}
	if err := cmd.Start(); err != nil {
		return t, err
	}
	defer func() {
		if !success {
			_ = cmd.Process.Kill()
			_ = cmd.Wait()
		}
	}()
	t.pid = cmd.Process.Pid
	// Request assignment, termination and limited query access for the suspended process.
	process, e := syscall.OpenProcess(0x100|1|0x1000, false, uint32(t.pid))
	if e != nil {
		return t, e
	}
	defer releaseHandle(process)
	ok, _, err = assignJob.Call(h, uintptr(process))
	if ok == 0 {
		return t, fmt.Errorf("assign job: %w", err)
	}
	if t.creation, e = creationTime(process); e != nil {
		return t, e
	}
	if e = resume(t.pid); e != nil {
		return t, e
	}
	success = true
	return t, nil
}

// --- Graceful interruption and job cleanup (AC3, AC4) ---

// interrupt sends a best-effort Ctrl+Break to the child's console process group.
func (t processTree) interrupt() { _, _, _ = ctrlEvent.Call(1, uintptr(t.pid)) }

// clean terminates job members and confirms departure before releasing the job handle.
func (t processTree) clean() error {
	defer releaseHandle(t.job)
	// ActiveProcesses can reach zero before a terminating process object is
	// signaled. Hold synchronize handles across termination to observe exit.
	handles, snapshotErr := t.members()
	defer func() {
		for _, h := range handles {
			releaseHandle(h)
		}
	}()
	ok, _, err := killJob.Call(uintptr(t.job), 1)
	if ok == 0 {
		return fmt.Errorf("terminate job: %w", err)
	}
	// Bound each handle wait so a stuck member surfaces a cleanup error.
	for _, h := range handles {
		state, waitErr := syscall.WaitForSingleObject(h, 10000)
		if waitErr != nil {
			return fmt.Errorf("wait for job member: %w", waitErr)
		}
		if state != syscall.WAIT_OBJECT_0 {
			return errors.New("job member still terminating")
		}
	}
	if snapshotErr != nil {
		return snapshotErr
	}
	// JOBOBJECT_BASIC_ACCOUNTING_INFORMATION contains four LARGE_INTEGERs and four DWORDs.
	var stats struct {
		Times                             [4]int64
		Faults, Total, Active, Terminated uint32
	}
	// Bound accounting polls for members beyond the original handle snapshot.
	deadline := time.Now().Add(10 * time.Second)
	for {
		ok, _, err = queryJob.Call(uintptr(t.job), 1, uintptr(unsafe.Pointer(&stats)), unsafe.Sizeof(stats), 0)
		if ok == 0 {
			return fmt.Errorf("query job: %w", err)
		}
		if stats.Active == 0 {
			return nil
		}
		if time.Now().After(deadline) {
			return errors.New("job still has active processes")
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// members snapshots job member PIDs and opens handles that can be waited on during cleanup.
func (t processTree) members() ([]syscall.Handle, error) {
	for capacity := 16; ; capacity *= 2 {
		// JOBOBJECT_BASIC_PROCESS_ID_LIST starts with two DWORDs followed by ULONG_PTR IDs.
		buf := make([]uintptr, capacity+8/int(unsafe.Sizeof(uintptr(0))))
		size := uintptr(len(buf)) * unsafe.Sizeof(buf[0])
		ok, _, err := queryJob.Call(uintptr(t.job), 3, uintptr(unsafe.Pointer(&buf[0])), size, 0)
		if ok == 0 {
			// Retry with a larger buffer when the PID list outgrows the current allocation.
			if errors.Is(err, syscall.ERROR_MORE_DATA) {
				continue
			}
			return nil, fmt.Errorf("list job members: %w", err)
		}
		count := (*[2]uint32)(unsafe.Pointer(&buf[0]))[1]
		start := 8 / int(unsafe.Sizeof(buf[0]))
		var handles []syscall.Handle
		for _, pid := range buf[start : start+int(count)] {
			h, err := syscall.OpenProcess(0x100000, false, uint32(pid))
			if errors.Is(err, syscall.Errno(87)) {
				continue
			}
			if err != nil {
				return handles, fmt.Errorf("open job member: %w", err)
			}
			handles = append(handles, h)
		}
		return handles, nil
	}
}

// --- Process identity, handle release and volume queries ---

// processAlive probes a PID without blocking by checking whether its process handle has signaled.
func processAlive(pid int) (bool, error) {
	if pid <= 0 {
		return false, errors.New("invalid pid")
	}
	h, err := syscall.OpenProcess(0x100000, false, uint32(pid))
	if errors.Is(err, syscall.Errno(87)) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	defer releaseHandle(h)
	state, err := syscall.WaitForSingleObject(h, 0)
	return state == syscall.WAIT_TIMEOUT, err
}

// freeSpace queries caller-available and total bytes on the data volume.
func freeSpace(path string) (uint64, uint64, error) {
	p, err := syscall.UTF16PtrFromString(path)
	if err != nil {
		return 0, 0, err
	}
	var free, total uint64
	ok, _, err := diskSpace.Call(uintptr(unsafe.Pointer(p)), uintptr(unsafe.Pointer(&free)), uintptr(unsafe.Pointer(&total)), 0)
	if ok == 0 {
		return 0, 0, err
	}
	return free, total, nil
}

// releaseHandle releases a Windows handle after the caller has finished using it.
func releaseHandle(h syscall.Handle) { _ = syscall.CloseHandle(h) }

// creationTime reads the creation FILETIME used to distinguish reused PIDs.
func creationTime(h syscall.Handle) (string, error) {
	var birth, exit, kernel, user syscall.Filetime
	if err := syscall.GetProcessTimes(h, &birth, &exit, &kernel, &user); err != nil {
		return "", err
	}
	return fmt.Sprintf("%08x%08x", birth.HighDateTime, birth.LowDateTime), nil
}

// identify queries liveness and creation time through the same handle to avoid PID reuse between lookups.
func identify(pid int) (string, bool, error) {
	if pid <= 0 {
		return "", false, errors.New("invalid pid")
	}
	h, err := syscall.OpenProcess(0x100000|0x1000, false, uint32(pid))
	if errors.Is(err, syscall.Errno(87)) {
		return "", false, nil
	}
	if err != nil {
		return "", false, err
	}
	defer releaseHandle(h)
	state, err := syscall.WaitForSingleObject(h, 0)
	if err != nil || state != syscall.WAIT_TIMEOUT {
		return "", false, err
	}
	birth, err := creationTime(h)
	return birth, true, err
}
