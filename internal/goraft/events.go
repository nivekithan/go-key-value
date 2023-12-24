package goraft

import (
	"context"
	"kv/api/raft"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type raftEvents struct {
	kind possibleRaftEvents
}

type possibleRaftEvents = int

const (
	timeoutForElection = iota
	convertToFollower
)

func (r *RaftServer) respondToRaftEvents() {
	for {
		event := <-r.eventCh

		switch event.kind {
		case timeoutForElection:
			r.respondToTimeoutForElectionEvent()
		case convertToFollower:
			r.respondToConvertToFollower()

		}
	}
}

func (r *RaftServer) respondToConvertToFollower() {
	if r.memoryState.state() == followerState {
		r.l.Println("Got convertToFollower event when state is follower")
		return
	}

	r.l.Println("Converting to follower state")
	r.memoryState.setState(followerState)
	r.electionTimer.reset()
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
