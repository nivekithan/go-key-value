package goraft

import (
	"log"
	"math/rand"
	"sync"
	"time"
)

type electionTimer struct {
	mu                  sync.Mutex
	upperboundTimeoutMs uint64
	currentTimeout      time.Time
	l                   *log.Logger
}

func newElectionTimeer(upperboundTimeoutMs uint64, l *log.Logger) electionTimer {
	return electionTimer{upperboundTimeoutMs: upperboundTimeoutMs, l: l}
}

func (e *electionTimer) reset() {
	e.mu.Lock()
	defer e.mu.Unlock()

	nextTimerDuration := time.Duration(rand.Intn(int(e.upperboundTimeoutMs)) + int(e.upperboundTimeoutMs))
	e.currentTimeout = time.Now().Add(nextTimerDuration * time.Millisecond)

	e.l.Println("Reseted election timer")
}

func (e *electionTimer) isElectionTimeoutPassed() bool {
	e.mu.Lock()
	defer e.mu.Unlock()

	isTimeoutPassed := time.Now().After(e.currentTimeout)

	// e.l.Printf("isElectionTimeoutPassed: %v", isTimeoutPassed)

	return isTimeoutPassed
}
