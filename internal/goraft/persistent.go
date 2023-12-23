package goraft

import "sync"

type persistent struct {
	mu          sync.Mutex
	currentTerm uint64
	votedFor    uint64
}

func newPersistent() persistent {
	return persistent{currentTerm: 0, votedFor: 0}
}
