package goraft

import (
	"fmt"
	"kv/api/raft"
	"log"
)

type RaftServer struct {
	// States
	persistentState persistentState
	memoryState     memoryState

	// Timers
	electionTimer  electionTimer
	heartbeatTimer heartbeatTimer

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
		heartbeatTimer:  newHeartbeatTimer(c.HeartBeatMs, logger),
		eventCh:         ch,
		l:               logger,
	}
}

func (r *RaftServer) Start() {

	go r.respondToRaftEvents()
	go r.startGrpcServer()
	go func() {
		r.electionTimer.reset()
		r.heartbeatTimer.reset()
		for {
			currentState := r.memoryState.state()

			switch currentState {
			case followerState:
				if r.electionTimer.isElectionTimeoutPassed() {
					r.l.Printf("Election timeout is passed")
					r.eventCh <- raftEvents{kind: timeoutForElection}
					r.electionTimer.reset()
				}
			case leaderState:
				if r.heartbeatTimer.isHeartbeatTimeoutPassed() {
					r.eventCh <- raftEvents{kind: timeoutForHeartbeat}
					r.heartbeatTimer.reset()
				}
			}
		}
	}()
}
