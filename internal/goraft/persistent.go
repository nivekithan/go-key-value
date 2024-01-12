package goraft

import "sync"

type persistentState struct {
	mu          sync.Mutex
	currentTerm uint64
	_votedFor   uint64
	log         []Entry
}

type Entry struct {
	term       uint64
	command    string
	resChan    chan []byte
	acceptedBy int
	isApplied  bool
}

func newPersistent() persistentState {
	return persistentState{currentTerm: 0, _votedFor: 0, log: []Entry{}}
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

func (p *persistentState) addEntry(entry Entry) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.log = append(p.log, entry)
}

func (p *persistentState) getEntry(index int) (Entry, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if index >= len(p.log) {
		return Entry{}, false
	}
	entry := p.log[index]

	return entry, true
}

func (p *persistentState) getLastEntry() (Entry, uint64, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()

	lenOfLog := len(p.log)

	if lenOfLog == 0 {
		return Entry{}, 0, false
	}

	return p.log[lenOfLog-1], uint64(lenOfLog - 1), true
}

func (p *persistentState) getLengthOfLog() int {
	p.mu.Lock()
	defer p.mu.Unlock()

	lenOfLog := len(p.log)

	return lenOfLog
}
