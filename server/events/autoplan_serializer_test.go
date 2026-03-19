package events_test

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/runatlantis/atlantis/server/events"
	"github.com/runatlantis/atlantis/server/events/models"
	"github.com/runatlantis/atlantis/server/logging"
	"github.com/stretchr/testify/assert"
)

// mockCommandRunner records calls and can simulate long-running autoplans.
type mockCommandRunner struct {
	mu               sync.Mutex
	autoplanCalls    int
	commentCalls     int
	autoplanDuration time.Duration
	onAutoplan       func()
}

func (m *mockCommandRunner) RunAutoplanCommand(baseRepo models.Repo, headRepo models.Repo, pull models.PullRequest, user models.User) {
	m.mu.Lock()
	m.autoplanCalls++
	onAutoplan := m.onAutoplan
	dur := m.autoplanDuration
	m.mu.Unlock()

	if onAutoplan != nil {
		onAutoplan()
	}

	if dur > 0 {
		time.Sleep(dur)
	}
}

func (m *mockCommandRunner) RunCommentCommand(baseRepo models.Repo, maybeHeadRepo *models.Repo, maybePull *models.PullRequest, user models.User, pullNum int, cmd *events.CommentCommand) {
	m.mu.Lock()
	m.commentCalls++
	m.mu.Unlock()
}

func (m *mockCommandRunner) getAutoplanCalls() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.autoplanCalls
}

func (m *mockCommandRunner) getCommentCalls() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.commentCalls
}

func testPull(num int) (models.Repo, models.Repo, models.PullRequest, models.User) {
	baseRepo := models.Repo{FullName: "owner/repo"}
	headRepo := models.Repo{FullName: "owner/repo"}
	pull := models.PullRequest{
		Num:      num,
		BaseRepo: baseRepo,
	}
	user := models.User{Username: "test"}
	return baseRepo, headRepo, pull, user
}

func TestAutoplanSerializer_SingleAutoplanRunsNormally(t *testing.T) {
	logger := logging.NewNoopLogger(t)
	underlying := &mockCommandRunner{}
	ct := events.NewCancellationTracker()
	pt := events.NewProcessTracker(logger)
	serializer := events.NewAutoplanSerializer(underlying, ct, pt, logger)

	baseRepo, headRepo, pull, user := testPull(1)
	serializer.RunAutoplanCommand(baseRepo, headRepo, pull, user)

	assert.Equal(t, 1, underlying.getAutoplanCalls())
}

func TestAutoplanSerializer_CommentCommandPassesThrough(t *testing.T) {
	logger := logging.NewNoopLogger(t)
	underlying := &mockCommandRunner{}
	ct := events.NewCancellationTracker()
	pt := events.NewProcessTracker(logger)
	serializer := events.NewAutoplanSerializer(underlying, ct, pt, logger)

	baseRepo, _, _, user := testPull(1)
	serializer.RunCommentCommand(baseRepo, nil, nil, user, 1, nil)

	assert.Equal(t, 1, underlying.getCommentCalls())
	assert.Equal(t, 0, underlying.getAutoplanCalls())
}

func TestAutoplanSerializer_SecondAutoplanCancelsFirst(t *testing.T) {
	logger := logging.NewNoopLogger(t)
	ct := events.NewCancellationTracker()
	pt := events.NewProcessTracker(logger)

	firstStarted := make(chan struct{})
	firstCanCheck := make(chan struct{})

	underlying := &mockCommandRunner{}
	var callCount atomic.Int32

	underlying.onAutoplan = func() {
		n := callCount.Add(1)
		if n == 1 {
			close(firstStarted)
			// Simulate long-running work — wait until told to check cancellation.
			<-firstCanCheck
		}
	}

	serializer := events.NewAutoplanSerializer(underlying, ct, pt, logger)
	baseRepo, headRepo, pull, user := testPull(1)

	// Start first autoplan in a goroutine.
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		serializer.RunAutoplanCommand(baseRepo, headRepo, pull, user)
	}()

	// Wait for first autoplan to start.
	<-firstStarted

	// Start second autoplan — this should cancel the first.
	wg.Add(1)
	go func() {
		defer wg.Done()
		serializer.RunAutoplanCommand(baseRepo, headRepo, pull, user)
	}()

	// Give second goroutine time to call Cancel and block on <-done.
	time.Sleep(50 * time.Millisecond)

	// Verify cancellation was set.
	assert.True(t, ct.IsCancelled(pull))

	// Let the first autoplan finish.
	close(firstCanCheck)

	wg.Wait()

	// Both autoplans should have run (first was cancelled but completed,
	// second ran after first finished).
	assert.Equal(t, int32(2), callCount.Load())

	// Cancellation should be cleared for the second run.
	assert.False(t, ct.IsCancelled(pull))
}

func TestAutoplanSerializer_DifferentPRsRunConcurrently(t *testing.T) {
	logger := logging.NewNoopLogger(t)
	ct := events.NewCancellationTracker()
	pt := events.NewProcessTracker(logger)

	var concurrent atomic.Int32
	var maxConcurrent atomic.Int32

	underlying := &mockCommandRunner{
		onAutoplan: func() {
			cur := concurrent.Add(1)
			for {
				old := maxConcurrent.Load()
				if cur <= old || maxConcurrent.CompareAndSwap(old, cur) {
					break
				}
			}
			time.Sleep(50 * time.Millisecond)
			concurrent.Add(-1)
		},
	}

	serializer := events.NewAutoplanSerializer(underlying, ct, pt, logger)

	var wg sync.WaitGroup
	for i := 1; i <= 3; i++ {
		wg.Add(1)
		go func(prNum int) {
			defer wg.Done()
			baseRepo, headRepo, pull, user := testPull(prNum)
			serializer.RunAutoplanCommand(baseRepo, headRepo, pull, user)
		}(i)
	}

	wg.Wait()

	assert.Equal(t, 3, underlying.getAutoplanCalls())
	// At least 2 should have been running concurrently since they're different PRs.
	assert.GreaterOrEqual(t, maxConcurrent.Load(), int32(2))
}

func TestAutoplanSerializer_DoneChannelClosedOnPanic(t *testing.T) {
	logger := logging.NewNoopLogger(t)
	ct := events.NewCancellationTracker()
	pt := events.NewProcessTracker(logger)

	panicked := make(chan struct{})
	underlying := &mockCommandRunner{
		onAutoplan: func() {
			close(panicked)
			panic("test panic")
		},
	}

	serializer := events.NewAutoplanSerializer(underlying, ct, pt, logger)
	baseRepo, headRepo, pull, user := testPull(1)

	// Run the panicking autoplan — the panic will propagate from
	// RunAutoplanCommand. We wrap it to recover.
	done := make(chan struct{})
	go func() {
		defer func() {
			recover()
			close(done)
		}()
		serializer.RunAutoplanCommand(baseRepo, headRepo, pull, user)
	}()

	<-panicked
	<-done

	// A subsequent autoplan for the same PR should run without deadlocking.
	underlying.onAutoplan = nil
	secondDone := make(chan struct{})
	go func() {
		defer close(secondDone)
		serializer.RunAutoplanCommand(baseRepo, headRepo, pull, user)
	}()

	select {
	case <-secondDone:
		// Good — didn't deadlock.
	case <-time.After(2 * time.Second):
		t.Fatal("second autoplan deadlocked after first panicked")
	}
}
