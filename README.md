# MiniRaft
# MiniRaft — Distributed Consensus Engine

A fully functional implementation of the **Raft consensus protocol** in Go, supporting leader election, log replication, and fault-tolerant state machines across distributed nodes.

## Features

- **Leader Election** — randomized election timeouts, term-based voting
- **Log Replication** — AppendEntries RPCs with consistency checks
- **Fault Tolerance** — cluster survives up to `(N-1)/2` node failures
- **Snapshotting** — log compaction to prevent unbounded growth
- **Membership Changes** — dynamic cluster reconfiguration
- **10K+ req/sec** with zero data loss under node failures

## Architecture

```
┌─────────────────────────────────────────────────────────┐
│                     Raft Cluster                         │
│                                                          │
│  ┌──────────┐     ┌──────────┐     ┌──────────┐        │
│  │  Node 0  │◄───►│  Node 1  │◄───►│  Node 2  │        │
│  │ (Leader) │     │(Follower)│     │(Follower)│        │
│  └──────────┘     └──────────┘     └──────────┘        │
│       │                                                  │
│  AppendEntries RPC  /  RequestVote RPC  /  Heartbeat    │
└─────────────────────────────────────────────────────────┘
```

## Getting Started

### Prerequisites
- Go 1.21+
- Docker (optional, for multi-node simulation)

### Run Locally

```bash
git clone https://github.com/arjunsharma/MiniRaft
cd MiniRaft
go mod tidy

# Start a 3-node cluster
go run cmd/cluster/main.go --nodes 3

# Run tests
go test ./... -v -race

# Chaos test (kills random nodes)
go test ./tests/chaos/... -v
```

### Docker (multi-node)

```bash
docker-compose up --scale node=5
```

## Project Structure

```
MiniRaft/
├── cmd/
│   ├── cluster/        # Cluster bootstrap entrypoint
│   └── client/         # CLI client to submit commands
├── raft/
│   ├── node.go         # Core Raft node state machine
│   ├── election.go     # Leader election logic
│   ├── replication.go  # Log replication (AppendEntries)
│   ├── snapshot.go     # Log compaction & snapshotting
│   └── rpc.go          # gRPC transport layer
├── store/
│   └── kv.go           # Example key-value state machine
├── tests/
│   ├── unit/           # Unit tests per component
│   └── chaos/          # Fault injection tests
├── docker-compose.yml
└── go.mod
```

## Benchmarks

| Scenario | Throughput | Latency (P99) |
|---|---|---|
| 3-node, no failures | 12,400 req/s | 8ms |
| 5-node, 1 failed node | 9,800 req/s | 14ms |
| 5-node, leader failover | — | <150ms |

## References

- [Raft Paper — In Search of an Understandable Consensus Algorithm](https://raft.github.io/raft.pdf)
- [Raft Visualization](https://raft.github.io)
