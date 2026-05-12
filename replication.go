package raft

// AppendEntriesArgs is the RPC argument for log replication and heartbeats.
type AppendEntriesArgs struct {
	Term         int
	LeaderID     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

// AppendEntriesReply is the RPC reply for log replication.
type AppendEntriesReply struct {
	Term          int
	Success       bool
	ConflictIndex int // optimised back-tracking hint
	ConflictTerm  int
}

// AppendEntries handles incoming AppendEntries RPCs (log replication + heartbeat).
func (n *Node) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	n.mu.Lock()
	defer n.mu.Unlock()

	reply.Term = n.currentTerm
	reply.Success = false

	if args.Term < n.currentTerm {
		return
	}
	send(n.heartbeatCh)

	if args.Term > n.currentTerm {
		n.currentTerm = args.Term
		n.votedFor = -1
	}
	n.state = Follower

	// Consistency check: does our log contain prevLogIndex with prevLogTerm?
	if args.PrevLogIndex > 0 {
		if args.PrevLogIndex > len(n.log) {
			reply.ConflictIndex = len(n.log) + 1
			return
		}
		if n.log[args.PrevLogIndex-1].Term != args.PrevLogTerm {
			reply.ConflictTerm = n.log[args.PrevLogIndex-1].Term
			for i, e := range n.log {
				if e.Term == reply.ConflictTerm {
					reply.ConflictIndex = i + 1
					break
				}
			}
			return
		}
	}

	// Append new entries, overwriting any conflicting suffix
	insertIdx := args.PrevLogIndex
	for _, entry := range args.Entries {
		if insertIdx < len(n.log) {
			if n.log[insertIdx].Term != entry.Term {
				n.log = n.log[:insertIdx] // truncate conflict
			}
		}
		if insertIdx >= len(n.log) {
			n.log = append(n.log, entry)
		}
		insertIdx++
	}

	// Advance commit index
	if args.LeaderCommit > n.commitIndex {
		newCommit := args.LeaderCommit
		if len(n.log) < newCommit {
			newCommit = len(n.log)
		}
		n.commitIndex = newCommit
		go n.applyCommitted()
	}
	reply.Success = true
}

// broadcastAppendEntries sends AppendEntries to all peers (heartbeat or replication).
func (n *Node) broadcastAppendEntries() {
	n.mu.Lock()
	if n.state != Leader {
		n.mu.Unlock()
		return
	}
	peers := n.peers
	term := n.currentTerm
	leaderID := n.id
	commitIndex := n.commitIndex
	n.mu.Unlock()

	for _, peer := range peers {
		go func(p int) {
			n.mu.Lock()
			nextIdx := n.nextIndex[p]
			prevLogIndex := nextIdx - 1
			prevLogTerm := 0
			if prevLogIndex > 0 && prevLogIndex <= len(n.log) {
				prevLogTerm = n.log[prevLogIndex-1].Term
			}
			entries := append([]LogEntry{}, n.log[nextIdx-1:]...)
			n.mu.Unlock()

			args := AppendEntriesArgs{
				Term:         term,
				LeaderID:     leaderID,
				PrevLogIndex: prevLogIndex,
				PrevLogTerm:  prevLogTerm,
				Entries:      entries,
				LeaderCommit: commitIndex,
			}
			var reply AppendEntriesReply
			if err := n.transport.Call(p, "AppendEntries", args, &reply); err != nil {
				return
			}

			n.mu.Lock()
			defer n.mu.Unlock()

			if reply.Term > n.currentTerm {
				n.currentTerm = reply.Term
				n.state = Follower
				n.votedFor = -1
				return
			}
			if reply.Success {
				newMatch := prevLogIndex + len(entries)
				if newMatch > n.matchIndex[p] {
					n.matchIndex[p] = newMatch
					n.nextIndex[p] = newMatch + 1
				}
				n.maybeAdvanceCommitIndex()
			} else {
				// Back-track using conflict hint
				if reply.ConflictIndex > 0 {
					n.nextIndex[p] = reply.ConflictIndex
				} else if n.nextIndex[p] > 1 {
					n.nextIndex[p]--
				}
			}
		}(peer)
	}
}

// maybeAdvanceCommitIndex advances commitIndex when a majority has replicated an entry.
func (n *Node) maybeAdvanceCommitIndex() {
	majority := (len(n.peers)+1)/2 + 1
	for idx := len(n.log); idx > n.commitIndex; idx-- {
		if n.log[idx-1].Term != n.currentTerm {
			continue
		}
		count := 1
		for _, p := range n.peers {
			if n.matchIndex[p] >= idx {
				count++
			}
		}
		if count >= majority {
			n.commitIndex = idx
			go n.applyCommitted()
			break
		}
	}
}

// applyCommitted sends newly committed entries to the state machine channel.
func (n *Node) applyCommitted() {
	n.mu.Lock()
	defer n.mu.Unlock()
	for n.lastApplied < n.commitIndex {
		n.lastApplied++
		entry := n.log[n.lastApplied-1]
		n.applyCh <- ApplyMsg{
			CommandValid: true,
			Command:      entry.Command,
			CommandIndex: entry.Index,
		}
	}
}
