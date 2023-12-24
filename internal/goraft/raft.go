package goraft

import (
	"context"
	"fmt"
	"kv/api/raft"
	"log"
	"net"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
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

func (r *RaftServer) startGrpcServer() {
	li, err := net.Listen("tcp", r.memoryState.address())

	if err != nil {
		r.l.Panic(err)
	}

	grpcServer := grpc.NewServer()

	raft.RegisterRaftServiceServer(grpcServer, r)

	if err := grpcServer.Serve(li); err != nil {
		r.l.Panic(err)
	}

}

func (r *RaftServer) respondToRaftEvents() {
	for {
		event := <-r.eventCh

		switch event.kind {
		case timeoutForElection:
			r.respondToTimeoutForElectionEvent()
		}
	}
}

func (r *RaftServer) respondToTimeoutForElectionEvent() {
	if r.memoryState.state() != followerState {
		r.l.Printf("Got timeoutForElection event when state is not follower")
		return
	}

	electionResChan := make(chan bool)
	go r.startElection(electionResChan)

	electionRes := <-electionResChan

	if electionRes {
		r.l.Printf("Transistion to leader")
		r.memoryState.setState(leaderState)
	} else {
		r.l.Printf("Transition to follower")
		r.memoryState.setState(followerState)
	}
}

func (r *RaftServer) startElection(electionResChan chan bool) {
	r.l.Println("Starting election")

	r.electionTimer.reset()
	r.memoryState.setState(candidateState)

	r.persistentState.mu.Lock()
	r.persistentState.currentTerm++
	r.persistentState._votedFor = r.memoryState.id()
	// TODO: Add peristance here
	r.persistentState.mu.Unlock()
	// Send RequestVote RPCS
	var wg sync.WaitGroup
	voteCh := make(chan bool)

	go func() {
		wg.Wait()
		close(voteCh)
	}()

	for _, member := range *r.memoryState.Members() {
		wg.Add(1)
		go func(member Member, term uint64, candidateId uint64) {
			cc, err := grpc.Dial(
				member.Address,
				grpc.WithTransportCredentials(insecure.NewCredentials()),
			)

			if err != nil {
				r.l.Panic(err)
			}

			defer cc.Close()

			if err != nil {
				r.l.Panic(err)
			}
			raftClient := raft.NewRaftServiceClient(cc)

			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()

			res, err := raftClient.RequestVote(
				ctx,
				&raft.RequestVoteArgs{
					Term:         term,
					CandidateId:  candidateId,
					LastLogIndex: 0,
					LastLogTerm:  0,
					LogLength:    0,
				},
				grpc.WaitForReady(true),
			)

			if err != nil {
				r.l.Panic(err)
			}

			voteCh <- res.VoteGranted
			wg.Done()
		}(member, r.persistentState.term(), r.memoryState.id())
	}

	// We vote for ourselves. Thats why its starting from 1
	numOfPositiveVotes := 1

	for vote := range voteCh {
		if vote {
			numOfPositiveVotes++
		}
	}

	totalNumberOfMembers := 1 + len(*r.memoryState.Members())

	isWonElection := numOfPositiveVotes > (totalNumberOfMembers / 2)

	r.l.Printf("IsWonElection: %v", isWonElection)
	electionResChan <- isWonElection
}
