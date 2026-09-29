package callcenter

import (
	"testing"
	"time"
)

// 转入的来电保留优先级和首次到达时间，在新队列重新计时。
func TestTransferPreservesPriorityAndFirstArrival(t *testing.T) {
	eng, err := NewEngine(Config{
		Queues: []QueueConfig{
			{Name: "q1", Capacity: 5, WaitThreshold: 5 * time.Second, Overflow: OverflowRule{Kind: OverflowTransfer, Target: "q2"}},
			{Name: "q2", Capacity: 5, WaitThreshold: 10000 * time.Second, AfterCallWork: 1000 * time.Second, Overflow: OverflowRule{Kind: OverflowDisconnect}},
		},
		Agents: []AgentConfig{
			{ID: "a1", Queues: []string{"q2"}},
			{ID: "a2", Queues: []string{"q2"}},
		},
	})
	if err != nil {
		t.Fatalf("NewEngine: %v", err)
	}
	// t=0：两个坐席都被占住并进入长话后。
	mustEnqueue(t, eng, "busy1", "q2", 1)
	mustEnqueue(t, eng, "busy2", "q2", 1)
	mustEndCall(t, eng, "busy1")
	mustEndCall(t, eng, "busy2")
	// t=0：c1 进入无人服务的 q1。
	mustEnqueue(t, eng, "c1", "q1", 5)
	// t=2：c2 直接进入 q2，优先级 9。
	advance(t, eng, 2*time.Second)
	mustEnqueue(t, eng, "c2", "q2", 9)
	// t=4：c3 直接进入 q2，与 c1 同优先级但首次到达更晚。
	advance(t, eng, 2*time.Second)
	mustEnqueue(t, eng, "c3", "q2", 5)
	// t=5：c1 在 q1 到达阈值，转入 q2，保留优先级 5 与首次到达 t=0。
	advance(t, eng, time.Second)
	assertState(t, eng, "c1", CallWaiting)
	if info, _ := eng.CallStatus("c1"); info.Queue != "q2" {
		t.Fatalf("c1 queue = %q, want q2", info.Queue)
	}
	// t=1000：a1、a2 话后结束。a1 先恢复，接优先级最高的 c2；
	// a2 接 c1 而非 c3——c1 的首次到达时间（t=0）早于 c3（t=4）。
	advance(t, eng, 995*time.Second)
	assertConnected(t, eng, "c2", "a1")
	assertConnected(t, eng, "c1", "a2")
	assertState(t, eng, "c3", CallWaiting)
}

// 配置与 API 的参数校验。
func TestValidation(t *testing.T) {
	if _, err := NewEngine(Config{}); err == nil {
		t.Fatal("empty config accepted")
	}
	bad := []Config{
		{Queues: []QueueConfig{{Name: "q", Capacity: 0, WaitThreshold: time.Second}}},
		{Queues: []QueueConfig{{Name: "q", Capacity: 151, WaitThreshold: time.Second}}},
		{Queues: []QueueConfig{{Name: "q", Capacity: 1, WaitThreshold: 0}}},
		{Queues: []QueueConfig{{Name: "q", Capacity: 1, WaitThreshold: time.Second, AfterCallWork: -time.Second}}},
		{Queues: []QueueConfig{{Name: "q", Capacity: 1, WaitThreshold: time.Second, Overflow: OverflowRule{Kind: OverflowTransfer, Target: "nope"}}}},
		{Queues: []QueueConfig{{Name: "q", Capacity: 1, WaitThreshold: time.Second}, {Name: "q", Capacity: 1, WaitThreshold: time.Second}}},
		{
			Queues: []QueueConfig{{Name: "q", Capacity: 1, WaitThreshold: time.Second}},
			Agents: []AgentConfig{{ID: "a", Queues: []string{"nope"}}},
		},
		{
			Queues: []QueueConfig{{Name: "q", Capacity: 1, WaitThreshold: time.Second}},
			Agents: []AgentConfig{{ID: "a"}, {ID: "a"}},
		},
	}
	for i, cfg := range bad {
		if _, err := NewEngine(cfg); err == nil {
			t.Fatalf("bad config %d accepted", i)
		}
	}

	eng, err := NewEngine(Config{
		Queues: []QueueConfig{
			{Name: "q", Capacity: 2, WaitThreshold: time.Second},
			{Name: "q2", Capacity: 2, WaitThreshold: time.Second},
		},
		Agents: []AgentConfig{{ID: "a", Queues: []string{"q"}}},
	})
	if err != nil {
		t.Fatalf("NewEngine: %v", err)
	}
	if err := eng.Enqueue("c", "nope", 5); err != ErrUnknownQueue {
		t.Fatalf("Enqueue unknown queue = %v", err)
	}
	if err := eng.Enqueue("c", "q", 0); err != ErrInvalidPriority {
		t.Fatalf("Enqueue bad priority = %v", err)
	}
	if err := eng.Enqueue("c", "q", 11); err != ErrInvalidPriority {
		t.Fatalf("Enqueue bad priority = %v", err)
	}
	mustEnqueue(t, eng, "c", "q2", 5)
	if err := eng.Enqueue("c", "q", 5); err != ErrDuplicateCall {
		t.Fatalf("Enqueue duplicate = %v", err)
	}
	if err := eng.Abandon("nope"); err != ErrUnknownCall {
		t.Fatalf("Abandon unknown = %v", err)
	}
	if err := eng.EndCall("nope"); err != ErrUnknownCall {
		t.Fatalf("EndCall unknown = %v", err)
	}
	if err := eng.EndCall("c"); err != ErrCallNotConnected {
		t.Fatalf("EndCall waiting = %v", err)
	}
	if err := eng.SetAgentState("nope", AgentIdle); err != ErrUnknownAgent {
		t.Fatalf("SetAgentState unknown = %v", err)
	}
	if err := eng.SetAgentState("a", AgentTalking); err == nil {
		t.Fatal("SetAgentState(talking) accepted")
	}
	if err := eng.Advance(-time.Second); err != ErrInvalidDuration {
		t.Fatalf("Advance negative = %v", err)
	}
	if _, err := eng.CallStatus("nope"); err != ErrUnknownCall {
		t.Fatalf("CallStatus unknown = %v", err)
	}
	if _, err := eng.AgentStatus("nope"); err != ErrUnknownAgent {
		t.Fatalf("AgentStatus unknown = %v", err)
	}
}
