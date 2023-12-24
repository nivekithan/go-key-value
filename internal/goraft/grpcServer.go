package goraft

import (
	"context"
	"kv/api/raft"
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
	}
}
