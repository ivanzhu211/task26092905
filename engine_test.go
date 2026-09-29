package callcenter

import (
	"testing"
	"time"
)

func sec(n int) time.Duration { return time.Duration(n) * time.Second }

// TestSameInstantWrapupBeatsOverflow: in one Advance, a call hits its
// overflow threshold at the exact instant an agent serving its queue
// finishes wrapup. The call must be answered by that agent, not overflowed.
func TestSameInstantWrapupBeatsOverflow(t *testing.T) {
	e := New(Config{
		Queues: []QueueConfig{{
			Name:      "q1",
			Capacity:  10,
			Threshold: sec(10),
			Wrapup:    sec(10),
			Overflow:  OverflowRule{Action: OverflowDisconnect},
		}},
		Agents: []AgentConfig{{ID: "a1", Queues: []string{"q1"}}},
	})

	e.Enqueue("c1", "q1", 5)
	if got := e.CallStatus("c1"); got.State != CallAnswered || got.Agent != "a1" {
		t.Fatalf("c1 should be answered by a1, got %+v", got)
	}
	e.Enqueue("c2", "q1", 5)
	if got := e.CallStatus("c2"); got.State != CallWaiting {
		t.Fatalf("c2 should be waiting, got %+v", got)
	}
	// a1 enters wrapup at t=0, wrapup ends at t=10; c2 entered at t=0 and
	// hits its overflow threshold at t=10 as well.
	e.EndCall("c1")
	if got := e.AgentStatus("a1"); got.State != AgentWrapup {
		t.Fatalf("a1 should be in wrapup, got %+v", got)
	}

	e.Advance(sec(10))

	if got := e.CallStatus("c2"); got.State != CallAnswered || got.Agent != "a1" {
		t.Fatalf("c2 should be answered by a1, not overflowed; got %+v", got)
	}
	if got := e.AgentStatus("a1"); got.State != AgentInCall || got.Call != "c2" {
		t.Fatalf("a1 should be on c2, got %+v", got)
	}
}

// TestBasicLifecycle covers dispatch, wrapup, and idle recovery.
func TestBasicLifecycle(t *testing.T) {
	e := New(Config{
		Queues: []QueueConfig{{
			Name:      "q1",
			Capacity:  5,
			Threshold: sec(30),
			Wrapup:    sec(5),
			Overflow:  OverflowRule{Action: OverflowVoicemail},
		}},
		Agents: []AgentConfig{{ID: "a1", Queues: []string{"q1"}}},
	})
	e.Enqueue("c1", "q1", 3)
	if got := e.CallStatus("c1"); got.State != CallAnswered {
		t.Fatalf("c1 answered, got %+v", got)
	}
	e.EndCall("c1")
	if got := e.AgentStatus("a1"); got.State != AgentWrapup {
		t.Fatalf("a1 wrapup, got %+v", got)
	}
	e.Advance(sec(4))
	if got := e.AgentStatus("a1"); got.State != AgentWrapup {
		t.Fatalf("a1 still wrapup, got %+v", got)
	}
	e.Advance(sec(1))
	if got := e.AgentStatus("a1"); got.State != AgentIdle {
		t.Fatalf("a1 idle after wrapup, got %+v", got)
	}
}

// TestThresholdOverflow checks waiting past the threshold triggers the rule.
func TestThresholdOverflow(t *testing.T) {
	e := New(Config{
		Queues: []QueueConfig{
			{
				Name:      "src",
				Capacity:  5,
				Threshold: sec(10),
				Overflow:  OverflowRule{Action: OverflowTransfer, Target: "dst"},
			},
			{
				Name:      "dst",
				Capacity:  5,
				Threshold: sec(60),
				Overflow:  OverflowRule{Action: OverflowVoicemail},
			},
		},
		Agents: nil,
	})
	e.Enqueue("c1", "src", 7)
	e.Advance(sec(9))
	if got := e.CallStatus("c1"); got.State != CallWaiting || got.Queue != "src" {
		t.Fatalf("c1 waiting in src, got %+v", got)
	}
	e.Advance(sec(1))
	got := e.CallStatus("c1")
	if got.State != CallWaiting || got.Queue != "dst" {
		t.Fatalf("c1 transferred to dst, got %+v", got)
	}
	// Threshold restarts in the new queue: 60 more seconds.
	e.Advance(sec(59))
	if got := e.CallStatus("c1"); got.State != CallWaiting {
		t.Fatalf("c1 still waiting, got %+v", got)
	}
	e.Advance(sec(1))
	if got := e.CallStatus("c1"); got.State != CallVoicemail {
		t.Fatalf("c1 voicemail after dst threshold, got %+v", got)
	}
}

// TestCapacityOverflow checks a full queue applies its own rule to the
// incoming call immediately.
func TestCapacityOverflow(t *testing.T) {
	e := New(Config{
		Queues: []QueueConfig{
			{
				Name:      "q1",
				Capacity:  1,
				Threshold: sec(100),
				Overflow:  OverflowRule{Action: OverflowDisconnect},
			},
		},
		Agents: nil,
	})
	e.Enqueue("c1", "q1", 1)
	e.Enqueue("c2", "q1", 1)
	if got := e.CallStatus("c1"); got.State != CallWaiting {
		t.Fatalf("c1 waiting, got %+v", got)
	}
	if got := e.CallStatus("c2"); got.State != CallDisconnected {
		t.Fatalf("c2 disconnected on full queue, got %+v", got)
	}
}

// TestAbandonRaceFree: abandon a waiting call, then it cannot be dispatched.
func TestAbandon(t *testing.T) {
	e := New(Config{
		Queues: []QueueConfig{{
			Name:      "q1",
			Capacity:  5,
			Threshold: sec(100),
			Overflow:  OverflowRule{Action: OverflowDisconnect},
		}},
		Agents: nil,
	})
	e.Enqueue("c1", "q1", 1)
	e.Abandon("c1")
	if got := e.CallStatus("c1"); got.State != CallAbandoned {
		t.Fatalf("c1 abandoned, got %+v", got)
	}
	// Abandon after terminal state is a no-op.
	e.Abandon("c1")
	if got := e.CallStatus("c1"); got.State != CallAbandoned {
		t.Fatalf("c1 still abandoned, got %+v", got)
	}
}

// TestLongestIdleAgent: an incoming call goes to the longest-idle agent.
func TestLongestIdleAgent(t *testing.T) {
	e := New(Config{
		Queues: []QueueConfig{{
			Name:      "q1",
			Capacity:  5,
			Threshold: sec(100),
			Wrapup:    sec(5),
			Overflow:  OverflowRule{Action: OverflowDisconnect},
		}},
		Agents: []AgentConfig{
			{ID: "a1", Queues: []string{"q1"}},
			{ID: "a2", Queues: []string{"q1"}},
		},
	})
	// Both idle since t=0; tie broken by config order (a1).
	e.Enqueue("c1", "q1", 1)
	if got := e.CallStatus("c1"); got.Agent != "a1" {
		t.Fatalf("c1 to a1, got %+v", got)
	}
	e.EndCall("c1") // a1 wrapup until t=5
	e.Advance(sec(5))
	// Now a1 idle since t=5, a2 idle since t=0: a2 is longer idle.
	e.Enqueue("c2", "q1", 1)
	if got := e.CallStatus("c2"); got.Agent != "a2" {
		t.Fatalf("c2 to longest-idle a2, got %+v", got)
	}
}

// TestPendingStateRequest: state requests during a call apply at wrapup end.
func TestPendingStateRequest(t *testing.T) {
	e := New(Config{
		Queues: []QueueConfig{{
			Name:      "q1",
			Capacity:  5,
			Threshold: sec(100),
			Wrapup:    sec(5),
			Overflow:  OverflowRule{Action: OverflowDisconnect},
		}},
		Agents: []AgentConfig{{ID: "a1", Queues: []string{"q1"}}},
	})
	e.Enqueue("c1", "q1", 1)
	e.SetAgentState("a1", AgentOnBreak)
	if got := e.AgentStatus("a1"); got.State != AgentInCall {
		t.Fatalf("request pends during call, got %+v", got)
	}
	e.EndCall("c1")
	if got := e.AgentStatus("a1"); got.State != AgentWrapup {
		t.Fatalf("wrapup, got %+v", got)
	}
	e.Advance(sec(5))
	if got := e.AgentStatus("a1"); got.State != AgentOnBreak {
		t.Fatalf("pending break applied at wrapup end, got %+v", got)
	}
	// On-break agent must not receive calls.
	e.Enqueue("c2", "q1", 1)
	if got := e.CallStatus("c2"); got.State != CallWaiting {
		t.Fatalf("c2 waits while a1 on break, got %+v", got)
	}
	e.SetAgentState("a1", AgentIdle)
	if got := e.CallStatus("c2"); got.State != CallAnswered || got.Agent != "a1" {
		t.Fatalf("c2 answered once a1 idle, got %+v", got)
	}
}
