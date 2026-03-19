package events

import (
	"fmt"
	"sync"

	"github.com/runatlantis/atlantis/server/events/models"
	"github.com/runatlantis/atlantis/server/logging"
)

// runningAutoplan tracks a single in-flight autoplan execution.
type runningAutoplan struct {
	done chan struct{}
}

// AutoplanSerializer wraps a CommandRunner and ensures that only one autoplan
// runs at a time per PR. When a new autoplan arrives for a PR that already has
// one running, the running one is cancelled (its terraform processes are killed)
// and the new one waits for it to finish before starting.
//
// Comment commands are passed through unchanged.
type AutoplanSerializer struct {
	underlying          CommandRunner
	cancellationTracker CancellationTracker
	processTracker      *ProcessTracker
	logger              logging.SimpleLogging
	mu                  sync.Mutex
	running             map[string]*runningAutoplan // key: "repo#pullNum"
}

func NewAutoplanSerializer(
	underlying CommandRunner,
	cancellationTracker CancellationTracker,
	processTracker *ProcessTracker,
	logger logging.SimpleLogging,
) *AutoplanSerializer {
	return &AutoplanSerializer{
		underlying:          underlying,
		cancellationTracker: cancellationTracker,
		processTracker:      processTracker,
		logger:              logger,
		running:             make(map[string]*runningAutoplan),
	}
}

// RunCommentCommand passes through to the underlying runner unchanged.
func (s *AutoplanSerializer) RunCommentCommand(baseRepo models.Repo, maybeHeadRepo *models.Repo, maybePull *models.PullRequest, user models.User, pullNum int, cmd *CommentCommand) {
	s.underlying.RunCommentCommand(baseRepo, maybeHeadRepo, maybePull, user, pullNum, cmd)
}

// RunAutoplanCommand serializes autoplan execution per PR. If an autoplan is
// already running for this PR, cancel it and wait for it to finish before
// starting the new one.
func (s *AutoplanSerializer) RunAutoplanCommand(baseRepo models.Repo, headRepo models.Repo, pull models.PullRequest, user models.User) {
	prKey := fmt.Sprintf("%s#%d", baseRepo.FullName, pull.Num)

	s.mu.Lock()
	existing, ok := s.running[prKey]
	if ok {
		// Cancel the running autoplan: set the cancellation flag and kill its processes.
		s.logger.Info("cancelling running autoplan for %s to start new one", prKey)
		s.cancellationTracker.Cancel(pull)
		s.processTracker.KillAll(prKey)
		s.mu.Unlock()

		// Wait for the existing autoplan to finish.
		<-existing.done
	} else {
		s.mu.Unlock()
	}

	// Clear any stale cancellation flag so the new run proceeds normally.
	s.cancellationTracker.Clear(pull)

	rap := &runningAutoplan{done: make(chan struct{})}
	s.mu.Lock()
	s.running[prKey] = rap
	s.mu.Unlock()

	defer func() {
		close(rap.done)
		s.mu.Lock()
		// Only delete if this is still our entry (not replaced by yet another run).
		if s.running[prKey] == rap {
			delete(s.running, prKey)
		}
		s.mu.Unlock()
	}()

	s.underlying.RunAutoplanCommand(baseRepo, headRepo, pull, user)
}
