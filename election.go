package raft

import "sync"

// RequestVoteArgs is the RPC argument for leader election.
type RequestVoteArgs struct {
	Term         int
	CandidateID  int
	LastLogIndex int
	LastLogTerm  int
}

// RequestVoteReply is the RPC reply for leader election.
type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

// RequestVote handles an incoming vote request (RPC handler).
func (n *Node) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	n.mu.Lock()
	defer n.mu.Unlock()

	reply.Term = n.currentTerm
	reply.VoteGranted = false

	if args.Term < n.currentTerm {
		return
	}
	if args.Term > n.currentTerm {
		n.currentTerm = args.Term
		n.state = Follower
		n.votedFor = -1
	}

	upToDate := n.isCandidateUpToDate(args.LastLogIndex, args.LastLogTerm)
	if (n.votedFor == -1 || n.votedFor == args.CandidateID) && upToDate {
		n.votedFor = args.CandidateID
		reply.VoteGranted = true
		send(n.grantVoteCh)
	}
}

// isCandidateUpToDate returns true if the candidate's log is at least as
// up-to-date as ours (§5.4.1 of the Raft paper).
func (n *Node) isCandidateUpToDate(lastIndex, lastTerm int) bool {
	myLastIndex, myLastTerm := n.lastLogIndexTerm()
	if lastTerm != myLastTerm {
		return lastTerm > myLastTerm
	}
	return lastIndex >= myLastIndex
}

func (n *Node) lastLogIndexTerm() (int, int) {
	if len(n.log) == 0 {
		return 0, 0
	}
	last := n.log[len(n.log)-1]
	return last.Index, last.Term
}

// broadcastRequestVote sends RequestVote RPCs to all peers concurrently.
func (n *Node) broadcastRequestVote() {
	n.mu.Lock()
	args := RequestVoteArgs{
		Term:        n.currentTerm,
		CandidateID: n.id,
	}
	args.LastLogIndex, args.LastLogTerm = n.lastLogIndexTerm()
	peers := n.peers
	n.mu.Unlock()

	votes := 1 // vote for self
	majority := (len(peers)+1)/2 + 1
	var mu sync.Mutex
	var once sync.Once

	for _, peer := range peers {
		go func(p int) {
			var reply RequestVoteReply
			if err := n.transport.Call(p, "RequestVote", args, &reply); err != nil {
				return
			}
			n.mu.Lock()
			if reply.Term > n.currentTerm {
				n.currentTerm = reply.Term
				n.state = Follower
				n.votedFor = -1
				n.mu.Unlock()
				return
			}
			n.mu.Unlock()

			if reply.VoteGranted {
				mu.Lock()
				votes++
				if votes >= majority {
					once.Do(func() { send(n.winElectionCh) })
				}
				mu.Unlock()
			}
		}(peer)
	}
}

// send is a non-blocking channel send.
func send(ch chan struct{}) {
	select {
	case ch <- struct{}{}:
	default:
	}
}
