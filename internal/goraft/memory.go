package goraft

import "sync"

type memoryState struct {
	// Grpc server address
	_address  string
	muAddress sync.Mutex

	// Id of raft node
	_id  uint64
	muId sync.Mutex

	// Raft server _state (leader, follower, candidate)
	_state  PossibleServerState
	muState sync.Mutex

	// All Cluster members of raft server
	_members  []Member
	muMembers sync.Mutex
}

type Member struct {
	Address string
}

func newMemoryState(address string, id uint64, members []Member) memoryState {
	if address == "" {
		panic("Address is not present")
	}

	return memoryState{_address: address, _id: id, _state: followerState, _members: members}
}

func (a *memoryState) address() string {
	a.muAddress.Lock()
	defer a.muAddress.Unlock()

	return a._address
}

func (a *memoryState) id() uint64 {
	a.muId.Lock()
	defer a.muId.Unlock()

	return a._id
}

type PossibleServerState = int

const (
	leaderState PossibleServerState = iota
	followerState
	candidateState
)

func (a *memoryState) state() PossibleServerState {
	a.muState.Lock()
	defer a.muState.Unlock()

	return a._state
}

func (a *memoryState) setState(state PossibleServerState) {
	a.muState.Lock()
	defer a.muState.Unlock()

	a._state = state
}

func (a *memoryState) Members() *[]Member {
	a.muMembers.Lock()
	defer a.muMembers.Unlock()

	return &a._members
}
