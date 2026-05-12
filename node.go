package raft

import (
	"math/rand"
	"sync"
	"time"
)

// NodeState represents the Raft node role.
type NodeState int

const (
	Follower NodeState = iota
	Candidate
	Leader
)

// LogEntry is a single entry in the replicated log.
type LogEntry struct {
	Term    int
	Index   int
	Command interface{}
}

// Node holds the complete state of a Raft participant.
type Node struct {
	mu sync.Mutex

	id      int
	peers   []int
	state   NodeState
	stopped bool

	// Persistent state (written to stable storage before responding to RPCs)
	currentTerm int
	votedFor    int
	log         []LogEntry

	// Volatile state
	commitIndex int
	lastApplied int

	// Leader-only volatile state (reinitialized after election)
	nextIndex  map[int]int
	matchIndex map[int]int

	// Channels
	applyCh       chan ApplyMsg
	heartbeatCh   chan struct{}
	grantVoteCh   chan struct{}
	winElectionCh chan struct{}

	// Transport
	transport Transport
}

// ApplyMsg is delivered to the state machine when a log entry is committed.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
	// Snapshot fields
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

// NewNode initialises a Raft node and starts its background goroutines.
func NewNode(id int, peers []int, transport Transport, applyCh chan ApplyMsg) *Node {
	n := &Node{
		id:            id,
		peers:         peers,
		state:         Follower,
		votedFor:      -1,
		applyCh:       applyCh,
		heartbeatCh:   make(chan struct{}, 1),
		grantVoteCh:   make(chan struct{}, 1),
		winElectionCh: make(chan struct{}, 1),
		nextIndex:     make(map[int]int),
		matchIndex:    make(map[int]int),
		transport:     transport,
	}
	go n.run()
	return n
}

// run is the main event loop — drives state transitions.
func (n *Node) run() {
	for {
		n.mu.Lock()
		state := n.state
		n.mu.Unlock()

		switch state {
		case Follower:
			n.runFollower()
		case Candidate:
			n.runCandidate()
		case Leader:
			n.runLeader()
		}
	}
}

func electionTimeout() time.Duration {
	return time.Duration(300+rand.Intn(200)) * time.Millisecond
}

func (n *Node) runFollower() {
	timer := time.NewTimer(electionTimeout())
	defer timer.Stop()
	for {
		select {
		case <-n.heartbeatCh:
			timer.Reset(electionTimeout())
		case <-n.grantVoteCh:
			timer.Reset(electionTimeout())
		case <-timer.C:
			n.mu.Lock()
			n.state = Candidate
			n.mu.Unlock()
			return
		}
	}
}

func (n *Node) runCandidate() {
	n.mu.Lock()
	n.currentTerm++
	n.votedFor = n.id
	n.mu.Unlock()

	go n.broadcastRequestVote()

	timer := time.NewTimer(electionTimeout())
	defer timer.Stop()
	for {
		select {
		case <-n.heartbeatCh:
			n.mu.Lock()
			n.state = Follower
			n.mu.Unlock()
			return
		case <-n.winElectionCh:
			n.mu.Lock()
			n.state = Leader
			// Initialise leader volatile state
			for _, p := range n.peers {
				n.nextIndex[p] = len(n.log) + 1
				n.matchIndex[p] = 0
			}
			n.mu.Unlock()
			return
		case <-timer.C:
			// Split vote — restart election
			return
		}
	}
}

func (n *Node) runLeader() {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	n.broadcastAppendEntries() // immediate heartbeat
	for {
		select {
		case <-ticker.C:
			n.mu.Lock()
			state := n.state
			n.mu.Unlock()
			if state != Leader {
				return
			}
			n.broadcastAppendEntries()
		case <-n.heartbeatCh:
			// Received higher-term message — step down
			n.mu.Lock()
			n.state = Follower
			n.mu.Unlock()
			return
		}
	}
}

// Submit appends a command to the leader's log. Returns (index, term, isLeader).
func (n *Node) Submit(command interface{}) (int, int, bool) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.state != Leader {
		return -1, -1, false
	}
	entry := LogEntry{
		Term:    n.currentTerm,
		Index:   len(n.log) + 1,
		Command: command,
	}
	n.log = append(n.log, entry)
	return entry.Index, entry.Term, true
}

// GetState returns (currentTerm, isLeader).
func (n *Node) GetState() (int, bool) {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.currentTerm, n.state == Leader
}
