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
	_members  []internalMember
	muMembers sync.Mutex
}

type internalMember struct {
	address     string
	nextIndex   uint64
	matchIndex  uint64
	stopChannel chan bool
}

type Member struct {
	Address string
}

func newMemoryState(address string, id uint64, members []Member) memoryState {
	if address == "" {
		panic("Address is not present")
	}

	allInternalMembers := []internalMember{}

	for _, member := range members {
		allInternalMembers = append(allInternalMembers, internalMember{address: member.Address})
	}

	return memoryState{_address: address, _id: id, _state: followerState, _members: allInternalMembers}
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

func (a *memoryState) Members() *[]internalMember {
	a.muMembers.Lock()
	defer a.muMembers.Unlock()

	return &a._members
}

func (a *memoryState) getNextIndex(index int) uint64 {
	a.muMembers.Lock()
	defer a.muMembers.Unlock()

	return a._members[index].nextIndex
}

func (a *memoryState) getMatchIndex(index int) uint64 {
	a.muMembers.Lock()
	defer a.muMembers.Unlock()

	return a._members[index].matchIndex
}

func (a *memoryState) getStopChannel(index int) chan bool {
	a.muAddress.Lock()
	defer a.muAddress.Unlock()

	return a._members[index].stopChannel
}

func (a *memoryState) setNextIndex(index int, nextIndex uint64) {
	a.muMembers.Lock()
	defer a.muMembers.Unlock()

	a._members[index].nextIndex = nextIndex
}

func (a *memoryState) getMemberAddres(index int) string {
	a.muMembers.Lock()
	defer a.muAddress.Unlock()

	return a._members[index].address
}

func (a *memoryState) setMatchIndex(index int, matchIndex uint64) {
	a.muMembers.Lock()
	defer a.muMembers.Unlock()

	a._members[index].matchIndex = matchIndex
}

func (a *memoryState) setStopChannel(index int, stopChannel chan bool) {
	a.muMembers.Lock()
	defer a.muMembers.Unlock()

	a._members[index].stopChannel = stopChannel
}
