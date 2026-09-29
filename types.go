package callcenter

import (
	"errors"
	"time"
)

// OverflowKind 是队列溢出规则的种类。
type OverflowKind int

const (
	// OverflowDisconnect 断开该来电（终态）。
	OverflowDisconnect OverflowKind = iota
	// OverflowTransfer 将来电转入另一队列。
	OverflowTransfer
	// OverflowVoicemail 将来电转入语音信箱（终态）。
	OverflowVoicemail
)

// OverflowRule 描述队列溢出时的处理规则。
type OverflowRule struct {
	Kind OverflowKind
	// Target 仅在 Kind == OverflowTransfer 时有效，为目标队列名。
	Target string
}

// QueueConfig 描述一个队列的配置。
type QueueConfig struct {
	Name string
	// Capacity 等待人数上限，1 到 150。
	Capacity int
	// WaitThreshold 等待超过该时长即触发溢出，必须大于 0。
	WaitThreshold time.Duration
	// AfterCallWork 该队列通话结束后的话后时长，必须非负。
	AfterCallWork time.Duration
	Overflow      OverflowRule
}

// AgentConfig 描述一个坐席的配置。
type AgentConfig struct {
	ID string
	// Queues 该坐席可服务的队列名列表。
	Queues []string
}

// Config 是引擎的构造配置。
type Config struct {
	Queues []QueueConfig
	Agents []AgentConfig
}

// CallState 是来电的状态。
type CallState int

const (
	// CallWaiting 正在队列中等待。
	CallWaiting CallState = iota
	// CallConnected 已接通某坐席（终态）。
	CallConnected
	// CallVoicemail 已进入语音信箱（终态）。
	CallVoicemail
	// CallDisconnected 已被断开（终态）。
	CallDisconnected
	// CallAbandoned 等待中被来电方放弃（终态）。
	CallAbandoned
)

func (s CallState) String() string {
	switch s {
	case CallWaiting:
		return "waiting"
	case CallConnected:
		return "connected"
	case CallVoicemail:
		return "voicemail"
	case CallDisconnected:
		return "disconnected"
	case CallAbandoned:
		return "abandoned"
	}
	return "unknown"
}

// Terminal 报告该状态是否为终态。
func (s CallState) Terminal() bool { return s != CallWaiting }

// CallInfo 是 CallStatus 的返回结果。
type CallInfo struct {
	State CallState
	// Queue 是来电当前（或最后）所在队列。
	Queue string
	// Agent 仅在 State == CallConnected 时非空，为接通的坐席 ID。
	Agent string
}

// AgentState 是坐席状态。
type AgentState int

const (
	// AgentIdle 空闲，可被分配。
	AgentIdle AgentState = iota
	// AgentTalking 通话中。
	AgentTalking
	// AgentAfterCallWork 话后处理中。
	AgentAfterCallWork
	// AgentBreak 小休。
	AgentBreak
	// AgentOffline 离线。
	AgentOffline
)

func (s AgentState) String() string {
	switch s {
	case AgentIdle:
		return "idle"
	case AgentTalking:
		return "talking"
	case AgentAfterCallWork:
		return "after_call_work"
	case AgentBreak:
		return "break"
	case AgentOffline:
		return "offline"
	}
	return "unknown"
}

// AgentInfo 是 AgentStatus 的返回结果。
type AgentInfo struct {
	State AgentState
	// Call 仅在 State == AgentTalking 时非空，为当前通话的来电 ID。
	Call string
	// Pending 非空时表示通话/话后期间申请、将在话后结束时生效的状态。
	Pending *AgentState
}

var (
	ErrUnknownQueue     = errors.New("callcenter: unknown queue")
	ErrUnknownAgent     = errors.New("callcenter: unknown agent")
	ErrUnknownCall      = errors.New("callcenter: unknown call")
	ErrDuplicateCall    = errors.New("callcenter: duplicate call ID")
	ErrInvalidPriority  = errors.New("callcenter: priority must be between 1 and 10")
	ErrCallNotWaiting   = errors.New("callcenter: call is not waiting")
	ErrCallNotConnected = errors.New("callcenter: call is not connected")
	ErrInvalidDuration  = errors.New("callcenter: duration must be non-negative")
)
