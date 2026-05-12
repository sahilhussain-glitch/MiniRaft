package raft

import "encoding/json"

// Snapshot holds a point-in-time state machine snapshot plus the log prefix metadata.
type Snapshot struct {
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte
}

// TakeSnapshot is called by the upper layer to compact the log up to snapshotIndex.
func (n *Node) TakeSnapshot(snapshotIndex int, data []byte) {
	n.mu.Lock()
	defer n.mu.Unlock()

	if snapshotIndex <= n.commitIndex {
		return
	}
	snap := Snapshot{
		LastIncludedIndex: snapshotIndex,
		LastIncludedTerm:  n.log[snapshotIndex-1].Term,
		Data:              data,
	}
	// Trim the prefix of the log that is now covered by the snapshot
	n.log = n.log[snapshotIndex:]
	_ = snap // persist snap to stable storage (omitted for brevity)
}

// InstallSnapshot RPC handler — sent by the leader to lagging followers.
func (n *Node) InstallSnapshot(snap *Snapshot, reply *AppendEntriesReply) {
	n.mu.Lock()
	defer n.mu.Unlock()

	reply.Term = n.currentTerm
	if snap.LastIncludedIndex <= n.commitIndex {
		return // already have this or newer
	}
	n.log = nil
	n.commitIndex = snap.LastIncludedIndex
	n.lastApplied = snap.LastIncludedIndex

	n.applyCh <- ApplyMsg{
		SnapshotValid: true,
		Snapshot:      snap.Data,
		SnapshotTerm:  snap.LastIncludedTerm,
		SnapshotIndex: snap.LastIncludedIndex,
	}
}

// encodeState serialises persistent state for crash recovery.
func (n *Node) encodeState() []byte {
	state := struct {
		CurrentTerm int
		VotedFor    int
		Log         []LogEntry
	}{n.currentTerm, n.votedFor, n.log}
	b, _ := json.Marshal(state)
	return b
}
