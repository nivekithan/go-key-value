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

func (r *RaftServer) initalizeSendingAppendEntriesRPC() {
	if r.memoryState.state() != leaderState {
		return
	}

	r.l.Println("Initalizing Sending Append Entries RPC")

	for index, member := range *r.memoryState.Members() {
		if member.stopChannel != nil {
			member.stopChannel <- true
		}

		stopChannel := make(chan bool)

		r.memoryState.setNextIndex(index, uint64(r.persistentState.getLengthOfLog()))
		r.memoryState.setMatchIndex(index, 0)
		r.memoryState.setStopChannel(index, stopChannel)

		go func(memberIndex int) {
			var (
				memberAddress = r.memoryState.getMemberAddres(memberIndex)
			)
			for {
				stopChannel := r.memoryState.getStopChannel(memberIndex)

				if len(stopChannel) != 0 {
					// Message has been passed to stopChannel which means we have to stop this co routine
					// by first reading the message from stopChannel and then stopping the execuation of
					// go routine
					_ = <-stopChannel
					return
				}

				nextIndexToSend := r.memoryState.getNextIndex(memberIndex)
				currentLengthOfIndex := uint64(r.persistentState.getLengthOfLog())

				if currentLengthOfIndex <= nextIndexToSend {
					continue
				}

				preceddingEntry, isAvailabe := r.persistentState.getEntry(int(nextIndexToSend) - 1)

				rpcArgs := raft.AppendEntriesArgs{Term: r.persistentState.term(), LeaderId: r.memoryState.id()}

				if !isAvailabe {
					rpcArgs.LogLength = 0
				} else {
					rpcArgs.PrevLogIndex = nextIndexToSend - 1
					rpcArgs.PrevLogTerm = preceddingEntry.term
					allEntries := []raft.Entry{}

					r.persistentState.mu.Lock()
					for _, log := range r.persistentState.log[nextIndexToSend:] {
						allEntries = append(allEntries, raft.Entry{Term: log.term, Command: []byte(log.command)})
					}
					rpcArgs.LogLength = uint64(len(r.persistentState.log) - len(allEntries))
					r.persistentState.mu.Unlock()
				}

				resOfRpc := r.sendAppendEntriesToMember(memberAddress, &rpcArgs)

				isRpcFailed := !resOfRpc

				if isRpcFailed {
					if nextIndexToSend == 0 {
						// If nextIndexToSend is 0, which means there is no prevIndex. The only reason
						// follower will not accept the AppendEntriesRPC is because our term is lower
						// than them. Which means we will be converting to follower state concurrently in
						// another go routine. So we can safely exit this go routine
						//
						// To safely exit the go routine we will wait for a message in stopChannelRes
						_ = <-stopChannel

						return
					}
					// If the call fails due to log inconsistencies we will decrement the nextIndexToSend
					// and try again
					r.memoryState.setNextIndex(memberIndex, nextIndexToSend-1)
					continue
				} else {
					newNextIndex := nextIndexToSend + uint64(len(rpcArgs.Entries))
					r.memoryState.setNextIndex(memberIndex, newNextIndex)
					r.memoryState.setMatchIndex(memberIndex, newNextIndex-1)
				}
			}
		}(index)
	}
}

func (r *RaftServer) stopSendingAppendEntriesRPC() {
	var wg sync.WaitGroup
	for index, member := range *r.memoryState.Members() {

		stopChannel := member.stopChannel

		if stopChannel == nil {
			continue
		}

		wg.Add(1)
		go func(stopChannel chan bool, index int) {
			stopChannel <- true
			r.memoryState.setStopChannel(index, nil)
			wg.Done()
		}(stopChannel, index)
	}

	wg.Wait()
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

		}(member.address)
	}

	go func() {
		wg.Wait()
		close(resChan)
	}()

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
