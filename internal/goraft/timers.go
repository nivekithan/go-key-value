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

type heartbeatTimer struct {
	mu               sync.Mutex
	heartbeartMs     uint64
	heartbeatTimeout time.Time
	l                *log.Logger
}

func newHeartbeatTimer(heartbeatMs uint64, l *log.Logger) heartbeatTimer {
	return heartbeatTimer{heartbeartMs: heartbeatMs, l: l}
}

func (h *heartbeatTimer) reset() {
	h.mu.Lock()
	defer h.mu.Unlock()

	h.l.Println("Reseting heartbeat timeout")
	nextHeartbeatDuration := time.Duration(h.heartbeartMs)
	timeout := time.Now().Add(nextHeartbeatDuration * time.Millisecond)

	h.heartbeatTimeout = timeout
}

func (h *heartbeatTimer) isHeartbeatTimeoutPassed() bool {
	h.mu.Lock()
	defer h.mu.Unlock()

	isTimeoutPassed := time.Now().After(h.heartbeatTimeout)
	if isTimeoutPassed {
		h.l.Println("heartbeat timeout passed")
		return true
	}
	return false
}
