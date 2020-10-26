package raft

import "errors"

// 每个节点可能的三种状态
const (
	follower  = "Follower"
	candidate = "Candidate"
	leader    = "Leader"
)

const (
	unKnownLeader = 0
	noVote        = 0
)

// 论文里写的好象是 150ms ~ 300ms
var (
	MinimumElectionTimeoutMS int32 = 250
	MaximumElectionTimeoutMS       = 2 * MinimumElectionTimeoutMS
)

var (
	errNotLeader             = errors.New("not the leader")
	errUnknownLeader         = errors.New("unknown leader")
	errDeposed               = errors.New("deposed during replication")
	errAppendEntriesRejected = errors.New("appendEntries RPC rejected")
	errReplicationFailed     = errors.New("command replication failed (but will keep retrying)")
	errOutOfSync             = errors.New("out of sync")
	errAlreadyRunning        = errors.New("already running")
)
