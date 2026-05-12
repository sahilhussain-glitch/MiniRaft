package raft

// Transport is the networking abstraction used by a Raft node.
// Swap this out for gRPC, in-process channels (tests), or TCP.
type Transport interface {
	Call(peerID int, method string, args, reply interface{}) error
}
