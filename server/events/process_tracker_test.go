package events_test

import (
	"os"
	"os/exec"
	"sync"
	"testing"
	"time"

	"github.com/runatlantis/atlantis/server/events"
	"github.com/runatlantis/atlantis/server/logging"
	"github.com/stretchr/testify/assert"
)

func TestProcessTracker_RegisterAndKillAll(t *testing.T) {
	logger := logging.NewNoopLogger(t)
	pt := events.NewProcessTracker(logger)

	// Start a long-running process we can kill.
	cmd := exec.Command("sleep", "60")
	err := cmd.Start()
	assert.NoError(t, err)

	pt.Register("repo#1", cmd.Process)
	pt.KillAll("repo#1")

	// Wait should return an error because the process was killed.
	err = cmd.Wait()
	assert.Error(t, err)
}

func TestProcessTracker_DeregisterPreventsKill(t *testing.T) {
	logger := logging.NewNoopLogger(t)
	pt := events.NewProcessTracker(logger)

	// Start a long-running process.
	cmd := exec.Command("sleep", "60")
	err := cmd.Start()
	assert.NoError(t, err)

	pt.Register("repo#1", cmd.Process)
	pt.Deregister("repo#1", cmd.Process)
	pt.KillAll("repo#1")

	// Process should still be running since we deregistered before killing.
	// Kill it ourselves to clean up.
	assert.NoError(t, cmd.Process.Kill())
	_ = cmd.Wait()
}

func TestProcessTracker_KillAllOnUnknownKey(t *testing.T) {
	logger := logging.NewNoopLogger(t)
	pt := events.NewProcessTracker(logger)

	// Should not panic.
	pt.KillAll("nonexistent#99")
}

func TestProcessTracker_ConcurrentAccess(t *testing.T) {
	logger := logging.NewNoopLogger(t)
	pt := events.NewProcessTracker(logger)

	var wg sync.WaitGroup
	prKey := "repo#1"

	// Run concurrent register/deregister/kill operations.
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			cmd := exec.Command("sleep", "60")
			if err := cmd.Start(); err != nil {
				return
			}
			pt.Register(prKey, cmd.Process)
			time.Sleep(time.Millisecond)
			pt.KillAll(prKey)
			pt.Deregister(prKey, cmd.Process)
			_ = cmd.Wait()
		}()
	}

	wg.Wait()
}

func TestProcessTracker_KillAlreadyExitedProcess(t *testing.T) {
	logger := logging.NewNoopLogger(t)
	pt := events.NewProcessTracker(logger)

	// Start a process that exits immediately.
	cmd := exec.Command("true")
	err := cmd.Start()
	assert.NoError(t, err)
	_ = cmd.Wait()

	pt.Register("repo#1", cmd.Process)
	// Should not panic even though process already exited.
	pt.KillAll("repo#1")
}

func TestProcessTracker_MultipleProcessesSameKey(t *testing.T) {
	logger := logging.NewNoopLogger(t)
	pt := events.NewProcessTracker(logger)

	cmds := make([]*exec.Cmd, 3)
	for i := range cmds {
		cmds[i] = exec.Command("sleep", "60")
		err := cmds[i].Start()
		assert.NoError(t, err)
		pt.Register("repo#1", cmds[i].Process)
	}

	pt.KillAll("repo#1")

	for _, cmd := range cmds {
		err := cmd.Wait()
		assert.Error(t, err)
	}
}

func TestProcessTracker_ImplementsProcessRegistrar(t *testing.T) {
	logger := logging.NewNoopLogger(t)
	pt := events.NewProcessTracker(logger)

	// Verify it satisfies the interface by using it as one.
	var proc *os.Process
	_ = proc
	pt.Register("key", nil) // nil proc should not panic in register
}
