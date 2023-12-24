package goraft

import (
	"fmt"
	"kv/api/raft"
	"log"
)

type RaftServer struct {
	persistentState persistentState
	memoryState     memoryState
	electionTimer   electionTimer

	eventCh chan raftEvents

	l *log.Logger

	*raft.UnimplementedRaftServiceServer
}

type Config struct {
	Id          uint64
	Address     string
	HeartBeatMs uint64
	Members     []Member
}

func NewRaftServer(c Config) *RaftServer {
	ch := make(chan raftEvents)
	logger := log.Default()

	logger.SetPrefix(fmt.Sprintf("%v: ", c.Id))
	return &RaftServer{
		persistentState: newPersistent(),
		memoryState:     newMemoryState(c.Address, c.Id, c.Members),
		electionTimer:   newElectionTimeer(c.HeartBeatMs*2, logger),
		eventCh:         ch,
		l:               logger,
	}
}

func (r *RaftServer) Start() {

	go r.respondToRaftEvents()
	go r.startGrpcServer()
	go func() {
		r.electionTimer.reset()
		for {
			currentState := r.memoryState.state()

			switch currentState {
			case followerState:
				if r.electionTimer.isElectionTimeoutPassed() {
					r.l.Printf("Election timeout is passed")
					r.eventCh <- raftEvents{kind: timeoutForElection}
					r.electionTimer.reset()
				}
			}
		}
	}()
}
