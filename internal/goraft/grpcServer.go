package goraft

import (
	"context"
	"kv/api/raft"
	"net"

	"google.golang.org/grpc"
)

var _ raft.RaftServiceServer = (*RaftServer)(nil)

func (r *RaftServer) RequestVote(ctx context.Context, in *raft.RequestVoteArgs) (*raft.RequestVoteResult, error) {

	r.l.Printf("Responding to requestVote from %v", in.CandidateId)
	r.updateTerm(in.Term)

	if in.Term < r.persistentState.term() {
		r.l.Printf("Not providing vote since currentTerm is bigger than in.Term")
		return &raft.RequestVoteResult{Term: r.persistentState.term(), VoteGranted: false}, nil
	}

	if r.persistentState.votedFor() != 0 && r.persistentState.votedFor() != in.CandidateId {
		r.l.Printf("Not providing vote since already voted to someone else in this term")
		return &raft.RequestVoteResult{Term: r.persistentState.term(), VoteGranted: true}, nil
	}

	r.persistentState.mu.Lock()
	r.persistentState._votedFor = in.CandidateId
	// TODO: Add persistance
	r.persistentState.mu.Unlock()

	return &raft.RequestVoteResult{Term: r.persistentState.term(), VoteGranted: true}, nil
}

func (r *RaftServer) AppendEntries(ctx context.Context, in *raft.AppendEntriesArgs) (*raft.AppendEntriesResult, error) {
	r.l.Printf("Responding to AppendEntries by %d", in.LeaderId)

	r.updateTerm(in.Term)

	if r.persistentState.term() > in.Term {
		r.l.Println("Responding false since incomding term is lesser than current term")
		return &raft.AppendEntriesResult{Term: r.persistentState.currentTerm, Success: false}, nil
	}

	r.electionTimer.reset()
	return &raft.AppendEntriesResult{Term: r.persistentState.currentTerm, Success: true}, nil
}

func (r *RaftServer) updateTerm(incomingTerm uint64) {
	if r.persistentState.term() < incomingTerm {
		r.l.Printf(
			"Incoming RPC term is bigger than currentTerm. Updating current term to %d and reseting votedFor", incomingTerm,
		)

		r.persistentState.mu.Lock()
		r.persistentState.currentTerm = incomingTerm
		r.persistentState._votedFor = 0
		// TODO: Add persistance
		r.persistentState.mu.Unlock()

		if r.memoryState.state() != followerState {
			r.l.Println("Since RPC term is bigger than currentTerm transisting to follower")
			r.eventCh <- raftEvents{kind: convertToFollower}
		}
	}
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
