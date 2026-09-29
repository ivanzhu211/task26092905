package callcenter

import (
	"sync"
	"time"
)

// OverflowAction is what happens to a call when it overflows a queue.
type OverflowAction int

const (
	OverflowTransfer OverflowAction = iota
	OverflowVoicemail
	OverflowDisconnect
)

// OverflowRule describes a queue's overflow behavior.
type OverflowRule struct {
	Action OverflowAction
	Target string // target queue name, only for OverflowTransfer
}

// QueueConfig configures a single queue.
type QueueConfig struct {
	Name      string
	Capacity  int
	Threshold time.Duration // max wait before overflow
	Wrapup    time.Duration // after-call work duration
	Overflow  OverflowRule
}

// AgentConfig configures a single agent.
type AgentConfig struct {
	ID     string
	Queues []string // queues this agent can serve
}

// Config is the engine configuration.
type Config struct {
	Queues []QueueConfig
	Agents []AgentConfig
}

// AgentState is the requested/observed state of an agent.
type AgentState int

const (
	AgentIdle AgentState = iota
	AgentOnBreak
	AgentOffline
	AgentInCall
	AgentWrapup
)

// CallState is the lifecycle state of a call.
type CallState int

const (
	CallWaiting CallState = iota
	CallAnswered
	CallVoicemail
	CallDisconnected
	CallAbandoned
)

// CallInfo is a snapshot of a call's status.
type CallInfo struct {
	ID     string
	State  CallState
	Queue  string // queue it is waiting in or was answered from
	Agent  string // answering agent, if answered
	Exists bool
}

// AgentInfo is a snapshot of an agent's status.
type AgentInfo struct {
	ID     string
	State  AgentState
	Call   string // bound call, if any
	Exists bool
}

// call is the internal representation of a call.
type call struct {
	id           string
	queue        string // current queue
	priority     int
	firstArrival time.Duration // first entry into the system
	enteredAt    time.Duration // entry into current queue

	mu       sync.Mutex
	state    CallState
	agent    string
	answered bool
}

// agent is the internal representation of an agent.
type agent struct {
	id     string
	queues []*queue

	mu           sync.Mutex
	state        AgentState
	call         *call
	idleSince    time.Duration
	wrapupEnd    time.Duration
	pendingState AgentState
	hasPending   bool
}

// queue is the internal representation of a queue.
type queue struct {
	name      string
	capacity  int
	threshold time.Duration
	wrapup    time.Duration
	overflow  OverflowRule
	agents    []*agent

	mu      sync.Mutex
	waiting []*call
}
