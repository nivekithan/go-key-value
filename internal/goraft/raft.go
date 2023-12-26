package goraft

import (
	"context"
	"fmt"
	"kv/api/raft"
	"log"
	"os"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
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
	logger := log.New(os.Stderr, fmt.Sprintf("%d: ", c.Id), log.LstdFlags)

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

func (r *RaftServer) AddEntry(command string) bool {
	r.l.Println("Adding a entry")

	if r.memoryState.state() != leaderState {
		r.l.Println("Cannot add entry when raftNode is not leader")
		return false
	}

	lastEntry, lastEntryIndex, isAvailabe := r.persistentState.getLastEntry()

	logLength := func() uint64 {
		if !isAvailabe {
			return 0
		}
		return lastEntryIndex + 1
	}()

	r.persistentState.addEntry(Entry{term: r.persistentState.term(), command: command})

	var wg sync.WaitGroup
	resChan := make(chan bool)

	go func() {
		wg.Wait()
		close(resChan)
	}()

	for _, member := range *r.memoryState.Members() {
		wg.Add(1)

		go func(memberAddres string) {
			res := r.sendAppendEntriesToMember(
				memberAddres,
				&raft.AppendEntriesArgs{
					Term:         r.persistentState.term(),
					LeaderId:     r.memoryState.id(),
					PrevLogIndex: lastEntryIndex,
					PrevLogTerm:  lastEntry.term,
					LogLength:    logLength,
					Entries: []*raft.Entry{
						{Term: r.persistentState.term(), Command: []byte(command)},
					},
				},
			)
			resChan <- res
			wg.Done()
		}(member.Address)
	}

	// Including ourselves
	allPositiveResults := 1

	for res := range resChan {
		if res {
			allPositiveResults++
		}

		if allPositiveResults > len(*r.memoryState.Members())/2 {
			// Commit the command
			r.l.Println("Commiting log")
			return true
		}
	}

	return false
}

func (r *RaftServer) sendAppendEntriesToMember(memberAddress string, entries *raft.AppendEntriesArgs) bool {

	r.l.Printf("Sending AppendEntries to Member: %v", memberAddress)

	cc, err := grpc.Dial(memberAddress, grpc.WithTransportCredentials(insecure.NewCredentials()))

	if err != nil {
		return r.sendAppendEntriesToMember(memberAddress, entries)
	}

	defer cc.Close()

	client := raft.NewRaftServiceClient(cc)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)

	defer cancel()

	res, err := client.AppendEntries(ctx, entries)

	if err != nil {
		return r.sendAppendEntriesToMember(memberAddress, entries)
	}

	r.updateTerm(res.Term)
	return res.Success
}
