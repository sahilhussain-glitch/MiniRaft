package store

import "sync"

// Op represents a state machine command.
type Op struct {
	Type  string // "Put" | "Append" | "Get"
	Key   string
	Value string
}

// KVStore is a fault-tolerant key-value store backed by a Raft log.
type KVStore struct {
	mu   sync.RWMutex
	data map[string]string
}

// NewKVStore creates an empty store.
func NewKVStore() *KVStore {
	return &KVStore{data: make(map[string]string)}
}

// Apply executes a committed Raft command against the store.
func (s *KVStore) Apply(op Op) string {
	s.mu.Lock()
	defer s.mu.Unlock()
	switch op.Type {
	case "Put":
		s.data[op.Key] = op.Value
	case "Append":
		s.data[op.Key] += op.Value
	case "Get":
		return s.data[op.Key]
	}
	return ""
}

// Snapshot serialises the store for Raft log compaction.
func (s *KVStore) Snapshot() map[string]string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	cp := make(map[string]string, len(s.data))
	for k, v := range s.data {
		cp[k] = v
	}
	return cp
}

// Restore rebuilds the store from a snapshot.
func (s *KVStore) Restore(snap map[string]string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.data = snap
}
