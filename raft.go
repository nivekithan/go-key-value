package goraft

import (
	"bufio"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net"
	"os"
	"sync"
	"time"

	"kv/api/raft"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type Entry struct {
	command []byte
	term    uint64
}

type RaftServer struct {
	// Perist state of a Rafe server
	currentTerm uint64
	votedFor    uint64
	log         []Entry

	state         RaftServerState
	electionTimer electionTimer

	members []Member
	fd      *os.File
	id      uint64

	raft.UnimplementedRaftServiceServer
	address string

	l *log.Logger
}

var _ raft.RaftServiceServer = (*RaftServer)(nil)

func (r *RaftServer) RequestVote(ctx context.Context, in *raft.RequestVoteArgs) (*raft.RequestVoteResult, error) {
	r.electionTimer.reset(r.l)
	r.l.Println("Responding to RequestVote")
	lastLogIndex := r.getLastLogIndex()
	lastLogTerm := r.getLastLogTerm()
	logLength := len(r.log)

	r.l.Printf("Current node: lastLogIndex %d, lastLogTerm %d, logLength %d, currentTerm %d, votedFor %d", lastLogIndex, lastLogTerm, logLength, r.currentTerm, r.votedFor)
	r.l.Printf(
		"Incoming node: lastLogIndex %d, lastLogTerm %d, logLength %d, currentTerm %d", in.LastLogIndex, in.LastLogTerm, in.LogLength, in.Term,
	)

	if r.votedFor != 0 && r.votedFor != in.CandidateId {
		return &raft.RequestVoteResult{Term: r.currentTerm, VoteGranted: false}, nil
	}

	if in.LogLength == 0 && logLength != 0 {
		return &raft.RequestVoteResult{Term: r.currentTerm, VoteGranted: false}, nil
	}

	if r.currentTerm > in.Term {
		return &raft.RequestVoteResult{Term: r.currentTerm, VoteGranted: false}, nil
	}

	if lastLogTerm > in.LastLogTerm {
		return &raft.RequestVoteResult{Term: r.currentTerm, VoteGranted: false}, nil
	}

	if lastLogTerm < in.LastLogTerm {
		return &raft.RequestVoteResult{Term: r.currentTerm, VoteGranted: true}, nil
	}

	if lastLogIndex > in.LastLogIndex {
		return &raft.RequestVoteResult{Term: r.currentTerm, VoteGranted: false}, nil
	}

	return &raft.RequestVoteResult{Term: r.currentTerm, VoteGranted: true}, nil
}

func (r *RaftServer) AppendEntries(ctx context.Context, in *raft.AppendEntriesArgs) (*raft.AppendEntriesResult, error) {
	r.l.Println("Responding to AppendEntries")
	r.electionTimer.reset(r.l)

	if in.Term > r.currentTerm {
		r.currentTerm = in.Term
	}

	r.persist(0)
	if in.Term < r.currentTerm {
		return &raft.AppendEntriesResult{Term: r.currentTerm, Success: false}, nil
	}

	return &raft.AppendEntriesResult{Term: r.currentTerm, Success: true}, nil
}

type Config struct {
	HeartbeatMs int
	Member      []Member
	Address     string
	Id          uint64
}

func NewRaftServer(fd *os.File, c Config) *RaftServer {
	logger := log.Default()
	logger.SetPrefix(fmt.Sprintf("%d: ", c.Id))
	return &RaftServer{
		currentTerm:   0,
		votedFor:      0,
		log:           []Entry{},
		fd:            fd,
		state:         RaftServerState{state: followerState},
		electionTimer: electionTimer{heartbeatMs: c.HeartbeatMs},
		members:       c.Member,
		address:       c.Address,
		id:            c.Id,
		l:             logger,
	}
}

/**
 * 0:8 => currentTerm
 * 8:16 => votedFor
 */
const PAGE_SIZE = 4096

func encodePage(currentTerm uint64, votedFor uint64) [PAGE_SIZE]byte {
	var page [PAGE_SIZE]byte

	binary.LittleEndian.PutUint64(page[0:8], currentTerm)
	binary.LittleEndian.PutUint64(page[8:16], votedFor)

	return page
}

func decodePage(page [PAGE_SIZE]byte) (currentTerm uint64, votedFor uint64) {
	currentTerm = binary.LittleEndian.Uint64(page[0:8])
	votedFor = binary.LittleEndian.Uint64(page[8:16])

	return currentTerm, votedFor
}

// Maximum size of a single command in log entry
const ENTRY_COMMAND_SIZE = 1024

/**
 * 0:8 => Term for entry
 * 8:16 => Size of entry command
 * 16:N => Entry command
 */
const ENTRY_HEADER_SIZE = 16

/**
 * Total size of Entry
 */
const ENTRY_SIZE = ENTRY_COMMAND_SIZE + ENTRY_HEADER_SIZE

func encodeEntry(entry *Entry) [ENTRY_SIZE]byte {
	term := entry.term
	sizeOfCommand := len(entry.command)

	if sizeOfCommand > ENTRY_COMMAND_SIZE {
		panic("Command size is greater than ENTRY_COMMAND_SIZE")
	}

	var encodedEntry [ENTRY_SIZE]byte

	binary.LittleEndian.PutUint64(encodedEntry[0:8], term)
	binary.LittleEndian.PutUint64(encodedEntry[8:16], uint64(sizeOfCommand))
	copy(encodedEntry[ENTRY_HEADER_SIZE:ENTRY_HEADER_SIZE+sizeOfCommand], entry.command)

	return encodedEntry
}

func decodeEntry(entry [ENTRY_SIZE]byte) Entry {
	term := binary.LittleEndian.Uint64(entry[0:8])
	sizeOfCommand := binary.LittleEndian.Uint64(entry[8:16])

	command := make([]byte, sizeOfCommand)

	copy(command, entry[ENTRY_HEADER_SIZE:ENTRY_HEADER_SIZE+sizeOfCommand])

	entryStruct := Entry{term: term, command: command}

	return entryStruct
}

func (r *RaftServer) persist(nNewEntries int) {
	page := encodePage(r.currentTerm, r.votedFor)
	if _, err := r.fd.Seek(0, io.SeekStart); err != nil {
		panic(err)
	}

	if _, err := r.fd.Write(page[:]); err != nil {
		panic(err)
	}

	if nNewEntries > 0 {
		newLogOffset := max(len(r.log)-nNewEntries, 0)

		if _, err := r.fd.Seek(int64(PAGE_SIZE+newLogOffset*ENTRY_SIZE), 0); err != nil {
			panic(err)
		}

		bw := bufio.NewWriter(r.fd)

		for i := newLogOffset; i < len(r.log); i++ {
			entry := encodeEntry(&r.log[i])

			if _, err := bw.Write(entry[:]); err != nil {
				panic(err)
			}

		}

		if err := bw.Flush(); err != nil {
			panic(err)
		}
	}
	if err := r.fd.Sync(); err != nil {
		panic(err)
	}
}

func (r *RaftServer) restore() {
	if _, err := r.fd.Seek(0, io.SeekStart); err != nil {
		panic(err)
	}

	var page [PAGE_SIZE]byte

	if _, err := r.fd.Read(page[:]); err != nil {
		if err == io.EOF {
			return
		} else {
			panic(err)
		}
	}

	r.currentTerm, r.votedFor = decodePage(page)

	br := bufio.NewReader(r.fd)

	allEntries := []Entry{}
	for {
		var encodedEntry [ENTRY_SIZE]byte

		if _, err := br.Read(encodedEntry[:]); err != nil {
			if err == io.EOF {
				// We have reached end of log entris
				break
			} else {
				panic(err)
			}
		}

		entry := decodeEntry(encodedEntry)
		allEntries = append(allEntries, entry)
	}

	r.log = allEntries
}

func (r *RaftServer) Start() {
	r.l.Println("Starting Raft Server")
	grpcServer := grpc.NewServer()

	lis, err := net.Listen("tcp", r.address)

	if err != nil {
		r.l.Panic(err)
	}

	raft.RegisterRaftServiceServer(grpcServer, r)

	go func() {
		r.l.Printf("Staring grpc server at address %v", lis.Addr())
		if err := grpcServer.Serve(lis); err != nil {
			r.l.Panic(err)
		}
	}()

	go func() {
		for {
			state := r.state.Get()
			r.electionTimer.reset(r.l)

			switch state {
			case followerState:
				r.electionTimer.waitForElectionTimeout(r.l)
				r.startElection()
			case leaderState:
				r.electionTimer.waitForHeartbeatTimeout(r.l)
				r.heartbeat()
			}
		}
	}()
}

func (r *RaftServer) heartbeat() {

	r.l.Println("Sending heartbeat")

	var wg sync.WaitGroup
	for _, member := range r.members {
		wg.Add(1)
		go func(leaderTerm uint64, leaderId uint64, address string) {
			defer wg.Done()
			conn, err := grpc.Dial(address, grpc.WithTransportCredentials(insecure.NewCredentials()))

			if err != nil {
				panic(err)
			}

			c := raft.NewRaftServiceClient(conn)

			ctx, cancel := context.WithTimeout(context.Background(), time.Second)

			defer cancel()
			_, err = c.AppendEntries(ctx, &raft.AppendEntriesArgs{Term: leaderTerm, LeaderId: leaderId})

			if err != nil {
				panic(err)
			}

		}(r.currentTerm, r.id, member.Address)
	}

	wg.Wait()
}
func (r *RaftServer) startElection() {
	r.l.Println("Staring election")
	r.currentTerm++
	// We vote for ourseveles
	r.votedFor = r.id

	r.persist(0)
	r.state.Set(candidateState)

	var wg sync.WaitGroup
	ch := make(chan bool)

	for _, member := range r.members {
		wg.Add(1)
		go func(term uint64, candidateId uint64, lastLogIndex uint64, lastLogTerm uint64, address string, logLength uint64) {

			conn, err := grpc.Dial(address, grpc.WithTransportCredentials(insecure.NewCredentials()))

			if err != nil {
				panic(err)
			}

			defer conn.Close()

			client := raft.NewRaftServiceClient(conn)

			// TODO: Proper value for time.Second

			ctx, cancel := context.WithTimeout(context.Background(), time.Second)

			defer cancel()

			res, err := client.RequestVote(ctx, &raft.RequestVoteArgs{Term: term, CandidateId: candidateId, LastLogIndex: lastLogIndex, LastLogTerm: lastLogTerm, LogLength: logLength}, grpc.WaitForReady(true))

			if err != nil {
				panic(err)
			}

			ch <- res.VoteGranted
			wg.Done()
		}(r.currentTerm, r.id, r.getLastLogIndex(), r.getLastLogTerm(), member.Address, uint64(len(r.log)))
	}

	go func() {
		wg.Wait()
		close(ch)
	}()

	allPositiveVote := []bool{}

	for vote := range ch {
		if vote {
			allPositiveVote = append(allPositiveVote, vote)
		}
	}

	// We vote for ourselves
	noOfVotes := len(allPositiveVote) + 1
	r.l.Printf("No of votes: %d", noOfVotes)

	// We include ourvelselves
	totalNoMembers := len(r.members) + 1

	isVoteGrantedToBecomeLeader := noOfVotes > (totalNoMembers / 2)
	r.l.Printf("Is becoming leader %v", isVoteGrantedToBecomeLeader)

	if isVoteGrantedToBecomeLeader {
		// Transistion to become a leader
		r.state.Set(leaderState)
	} else {
		// Transistion back to follower
		r.state.Set(followerState)
	}
}

func (r *RaftServer) getLastLogIndex() uint64 {
	if len(r.log) == 0 {
		return uint64(0)
	}

	return uint64(len(r.log) - 1)
}

func (r *RaftServer) getLastLogTerm() uint64 {
	if len(r.log) == 0 {
		return uint64(0)
	}

	return r.log[len(r.log)-1].term
}

type PossibleServerState = string

const (
	leaderState    PossibleServerState = "leader"
	followerState  PossibleServerState = "follower"
	candidateState PossibleServerState = "candidate"
)

type RaftServerState struct {
	mu    sync.Mutex
	state PossibleServerState
}

func (r *RaftServerState) Get() PossibleServerState {
	r.mu.Lock()

	defer r.mu.Unlock()

	return r.state
}

func (r *RaftServerState) Set(newState PossibleServerState) {
	r.mu.Lock()

	defer r.mu.Unlock()

	r.state = newState
}

type electionTimer struct {
	mu                 sync.Mutex
	electionTimeoutAt  time.Time
	heartbeatTimeoutAt time.Time
	heartbeatMs        int
}

func (e *electionTimer) reset(l *log.Logger) {
	l.Println("Reseting election timer")
	e.mu.Lock()
	defer e.mu.Unlock()
	electionInterval := time.Duration((rand.Intn(e.heartbeatMs*2) + e.heartbeatMs))

	electionTimeout := time.Now().Add(electionInterval * time.Millisecond)

	e.electionTimeoutAt = electionTimeout

	heartbeatInterval := time.Duration(e.heartbeatMs)
	heartbeatTimeout := time.Now().Add(heartbeatInterval * time.Millisecond)

	e.heartbeatTimeoutAt = heartbeatTimeout
}

func (e *electionTimer) isElectionPassed() bool {
	e.mu.Lock()
	defer e.mu.Unlock()

	return time.Now().After(e.electionTimeoutAt)
}

func (e *electionTimer) isHeartbeatPassed() bool {
	e.mu.Lock()
	defer e.mu.Unlock()

	return time.Now().After(e.heartbeatTimeoutAt)
}

func (e *electionTimer) waitForElectionTimeout(l *log.Logger) {
	l.Println("Waiting for timeout")
	for {
		if e.isElectionPassed() {
			l.Println("Timeout has passed")
			return
		}
	}
}

func (e *electionTimer) waitForHeartbeatTimeout(l *log.Logger) {
	l.Println("Waiting for heartbeat timeout")
	for {
		if e.isHeartbeatPassed() {
			l.Println("Heartbeat has passed")
			return
		}
	}
}

type Member struct {
	Id      uint64
	Address string
}
