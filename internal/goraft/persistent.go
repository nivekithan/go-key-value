package goraft

import "sync"

type persistentState struct {
	mu          sync.Mutex
	currentTerm uint64
	_votedFor   uint64
}

func newPersistent() persistentState {
	return persistentState{currentTerm: 0, _votedFor: 0}
}

func (p *persistentState) term() uint64 {
	p.mu.Lock()
	defer p.mu.Unlock()

	return p.currentTerm
}

func (p *persistentState) votedFor() uint64 {
	p.mu.Lock()
	defer p.mu.Unlock()

	return p._votedFor
}
