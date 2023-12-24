package goraft

type raftEvents struct {
	kind possibleRaftEvents
}

type possibleRaftEvents = int

const (
	timeoutForElection = iota
	wonElection
	loseElection
)
