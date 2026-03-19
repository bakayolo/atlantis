package events

import (
	"os"
	"sync"

	"github.com/runatlantis/atlantis/server/logging"
)

// ProcessTracker tracks running OS processes per PR key so they can be
// killed when an autoplan is superseded by a newer push.
type ProcessTracker struct {
	mu        sync.Mutex
	processes map[string][]*os.Process
	logger    logging.SimpleLogging
}

func NewProcessTracker(logger logging.SimpleLogging) *ProcessTracker {
	return &ProcessTracker{
		processes: make(map[string][]*os.Process),
		logger:    logger,
	}
}

// Register adds a process to the tracked list for a PR key.
func (pt *ProcessTracker) Register(prKey string, proc *os.Process) {
	pt.mu.Lock()
	defer pt.mu.Unlock()
	pt.processes[prKey] = append(pt.processes[prKey], proc)
}

// Deregister removes a specific process from the tracked list for a PR key.
func (pt *ProcessTracker) Deregister(prKey string, proc *os.Process) {
	pt.mu.Lock()
	defer pt.mu.Unlock()
	procs := pt.processes[prKey]
	for i, p := range procs {
		if p == proc {
			pt.processes[prKey] = append(procs[:i], procs[i+1:]...)
			break
		}
	}
	if len(pt.processes[prKey]) == 0 {
		delete(pt.processes, prKey)
	}
}

// KillAll kills all tracked processes for a PR key.
// It copies the process list under the lock to avoid holding it during kill.
func (pt *ProcessTracker) KillAll(prKey string) {
	pt.mu.Lock()
	procs := make([]*os.Process, len(pt.processes[prKey]))
	copy(procs, pt.processes[prKey])
	pt.mu.Unlock()

	for _, proc := range procs {
		if err := proc.Kill(); err != nil {
			pt.logger.Debug("killing process %d for %s: %s (may have already exited)", proc.Pid, prKey, err)
		}
	}
}
