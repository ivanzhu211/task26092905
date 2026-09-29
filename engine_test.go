package callcenter

import (
	"testing"
	"time"
)

// 同一 Advance 内，某来电到达溢出阈值的时刻恰有坐席话后结束：
// 话后结束先处理，该来电必须被这名坐席接通而不是被溢出。
func TestAdvanceACWEndBeforeOverflow(t *testing.T) {
	eng, err := NewEngine(Config{
		Queues: []QueueConfig{{
			Name:          "q1",
			Capacity:      5,
			WaitThreshold: 10 * time.Second,
			AfterCallWork: 10 * time.Second,
			Overflow:      OverflowRule{Kind: OverflowVoicemail},
		}},
		Agents: []AgentConfig{{ID: "a1", Queues: []string{"q1"}}},
	})
	if err != nil {
		t.Fatalf("NewEngine: %v", err)
	}
	// c0 立即接通 a1，随后挂机，a1 进入话后直到 t=10s。
	mustEnqueue(t, eng, "c0", "q1", 5)
	mustEndCall(t, eng, "c0")
	// c1 无人可接，等待，溢出时刻同样为 t=10s。
	mustEnqueue(t, eng, "c1", "q1", 5)

	if err := eng.Advance(10 * time.Second); err != nil {
		t.Fatalf("Advance: %v", err)
	}

	info, err := eng.CallStatus("c1")
	if err != nil {
		t.Fatalf("CallStatus: %v", err)
	}
	if info.State != CallConnected || info.Agent != "a1" {
		t.Fatalf("c1 = %v (agent %q), want connected to a1", info.State, info.Agent)
	}
	if st := mustAgentState(t, eng, "a1"); st != AgentTalking {
		t.Fatalf("a1 state = %v, want talking", st)
	}
	// 话后结束应先于溢出：c1 的溢出事件必须已被作废而不是触发。
	if err := eng.Advance(time.Second); err != nil {
		t.Fatalf("Advance: %v", err)
	}
	info, _ = eng.CallStatus("c1")
	if info.State != CallConnected {
		t.Fatalf("c1 changed to %v after extra advance", info.State)
	}
}

// 分配顺序：优先级高者优先；同级先到先服务；多个空闲坐席时给空闲最久的。
func TestAllocationOrder(t *testing.T) {
	eng, err := NewEngine(Config{
		Queues: []QueueConfig{{
			Name:          "q",
			Capacity:      10,
			WaitThreshold: time.Minute,
			AfterCallWork: time.Second,
			Overflow:      OverflowRule{Kind: OverflowDisconnect},
		}},
		Agents: []AgentConfig{
			{ID: "a1", Queues: []string{"q"}},
			{ID: "a2", Queues: []string{"q"}},
		},
	})
	if err != nil {
		t.Fatalf("NewEngine: %v", err)
	}
	// 两个坐席都忙起来。
	mustEnqueue(t, eng, "busy1", "q", 1)
	mustEnqueue(t, eng, "busy2", "q", 1)
	// 三个等待者：低优先级早到，高优先级晚到。
	mustEnqueue(t, eng, "low", "q", 3)
	mustEnqueue(t, eng, "high1", "q", 9)
	mustEnqueue(t, eng, "high2", "q", 9)

	// a1 先挂机（t=5s），话后 1s 后于 t=6s 空闲，应接 high1（优先级最高、同级最早）。
	advance(t, eng, 5*time.Second)
	mustEndCall(t, eng, "busy1")
	advance(t, eng, time.Second) // t=6s，a1 话后结束
	assertConnected(t, eng, "high1", "a1")
	// a2 后挂机，话后结束后应接 high2。
	mustEndCall(t, eng, "busy2")
	advance(t, eng, time.Second) // t=7s，a2 话后结束
	assertConnected(t, eng, "high2", "a2")
	// 空闲最久：a1 话后结束（t=8s）时接走仍在等待的 low；
	// a2 于 t=9s 空闲；a1 再次挂机后于 t=11s 空闲。
	mustEndCall(t, eng, "high1") // a1 进入话后
	advance(t, eng, time.Second) // t=8s：a1 话后结束，接走 low
	assertConnected(t, eng, "low", "a1")
	mustEndCall(t, eng, "high2") // a2 进入话后
	advance(t, eng, time.Second) // t=9s：a2 话后结束，空闲（idleSince=9）
	mustEndCall(t, eng, "low")   // a1 再次进入话后
	advance(t, eng, time.Second) // t=10s：a1 话后结束，空闲（idleSince=10）
	mustEnqueue(t, eng, "late", "q", 1)
	assertConnected(t, eng, "late", "a2") // a2 空闲更久，尽管 ID 更大
}

// 队列满时立即执行自己的溢出规则；连续转移绕回本轮已过的队列时断开。
func TestCapacityOverflowCascadeLoop(t *testing.T) {
	eng, err := NewEngine(Config{
		Queues: []QueueConfig{
			{Name: "qa", Capacity: 1, WaitThreshold: time.Minute, Overflow: OverflowRule{Kind: OverflowTransfer, Target: "qb"}},
			{Name: "qb", Capacity: 1, WaitThreshold: time.Minute, Overflow: OverflowRule{Kind: OverflowTransfer, Target: "qa"}},
		},
	})
	if err != nil {
		t.Fatalf("NewEngine: %v", err)
	}
	mustEnqueue(t, eng, "x", "qa", 5) // 占满 qa
	mustEnqueue(t, eng, "y", "qb", 5) // 占满 qb
	// qa 已满 → 转 qb；qb 已满 → 转 qa；qa 本轮已经过 → 断开。
	mustEnqueue(t, eng, "z", "qa", 5)
	assertState(t, eng, "z", CallDisconnected)
	// 未构成回路的满队列转移应成功进入目标队列。
	eng2, err := NewEngine(Config{
		Queues: []QueueConfig{
			{Name: "qa", Capacity: 1, WaitThreshold: time.Minute, Overflow: OverflowRule{Kind: OverflowTransfer, Target: "qb"}},
			{Name: "qb", Capacity: 2, WaitThreshold: time.Minute, Overflow: OverflowRule{Kind: OverflowVoicemail}},
		},
	})
	if err != nil {
		t.Fatalf("NewEngine: %v", err)
	}
	mustEnqueue(t, eng2, "x", "qa", 5)
	mustEnqueue(t, eng2, "z", "qa", 5) // qa 满 → 转入 qb 等待
	assertState(t, eng2, "z", CallWaiting)
	info, _ := eng2.CallStatus("z")
	if info.Queue != "qb" {
		t.Fatalf("z queue = %q, want qb", info.Queue)
	}
}

// 等待超时触发溢出：转入语音信箱；放弃与终态的不可变性。
func TestTimeoutOverflowAndTerminalStates(t *testing.T) {
	eng, err := NewEngine(Config{
		Queues: []QueueConfig{
			{Name: "q1", Capacity: 3, WaitThreshold: 5 * time.Second, Overflow: OverflowRule{Kind: OverflowVoicemail}},
			{Name: "q2", Capacity: 3, WaitThreshold: 5 * time.Second, Overflow: OverflowRule{Kind: OverflowDisconnect}},
		},
	})
	if err != nil {
		t.Fatalf("NewEngine: %v", err)
	}
	mustEnqueue(t, eng, "c1", "q1", 5)
	mustEnqueue(t, eng, "c2", "q2", 5)
	mustEnqueue(t, eng, "c3", "q1", 5)
	if err := eng.Abandon("c3"); err != nil {
		t.Fatalf("Abandon: %v", err)
	}
	advance(t, eng, 5*time.Second)
	assertState(t, eng, "c1", CallVoicemail)
	assertState(t, eng, "c2", CallDisconnected)
	assertState(t, eng, "c3", CallAbandoned)
	// 终态不可改变。
	if err := eng.Abandon("c1"); err != ErrCallNotWaiting {
		t.Fatalf("Abandon terminal = %v, want ErrCallNotWaiting", err)
	}
	advance(t, eng, time.Minute)
	assertState(t, eng, "c1", CallVoicemail)
}

// 通话/话后期间的状态申请在话后结束时才生效。
func TestPendingAgentState(t *testing.T) {
	eng, err := NewEngine(Config{
		Queues: []QueueConfig{{
			Name:          "q",
			Capacity:      5,
			WaitThreshold: time.Minute,
			AfterCallWork: 10 * time.Second,
			Overflow:      OverflowRule{Kind: OverflowDisconnect},
		}},
		Agents: []AgentConfig{{ID: "a", Queues: []string{"q"}}},
	})
	if err != nil {
		t.Fatalf("NewEngine: %v", err)
	}
	mustEnqueue(t, eng, "c", "q", 5)
	mustSetState(t, eng, "a", AgentBreak) // 通话中申请小休
	if st := mustAgentState(t, eng, "a"); st != AgentTalking {
		t.Fatalf("state = %v, want talking", st)
	}
	mustEndCall(t, eng, "c")
	if st := mustAgentState(t, eng, "a"); st != AgentAfterCallWork {
		t.Fatalf("state = %v, want after_call_work", st)
	}
	advance(t, eng, 10*time.Second)
	if st := mustAgentState(t, eng, "a"); st != AgentBreak {
		t.Fatalf("state = %v, want break", st)
	}
	// 小休中不得被分配。
	mustEnqueue(t, eng, "c2", "q", 5)
	assertState(t, eng, "c2", CallWaiting)
	mustSetState(t, eng, "a", AgentIdle)
	assertConnected(t, eng, "c2", "a")
}

func mustEnqueue(t *testing.T, e *Engine, id, q string, p int) {
	t.Helper()
	if err := e.Enqueue(id, q, p); err != nil {
		t.Fatalf("Enqueue(%s): %v", id, err)
	}
}

func mustEndCall(t *testing.T, e *Engine, id string) {
	t.Helper()
	if err := e.EndCall(id); err != nil {
		t.Fatalf("EndCall(%s): %v", id, err)
	}
}

func mustSetState(t *testing.T, e *Engine, id string, s AgentState) {
	t.Helper()
	if err := e.SetAgentState(id, s); err != nil {
		t.Fatalf("SetAgentState(%s): %v", id, err)
	}
}

func advance(t *testing.T, e *Engine, d time.Duration) {
	t.Helper()
	if err := e.Advance(d); err != nil {
		t.Fatalf("Advance: %v", err)
	}
}

func mustAgentState(t *testing.T, e *Engine, id string) AgentState {
	t.Helper()
	info, err := e.AgentStatus(id)
	if err != nil {
		t.Fatalf("AgentStatus(%s): %v", id, err)
	}
	return info.State
}

func assertState(t *testing.T, e *Engine, id string, want CallState) {
	t.Helper()
	info, err := e.CallStatus(id)
	if err != nil {
		t.Fatalf("CallStatus(%s): %v", id, err)
	}
	if info.State != want {
		t.Fatalf("call %s state = %v, want %v", id, info.State, want)
	}
}

func assertConnected(t *testing.T, e *Engine, id, agent string) {
	t.Helper()
	info, err := e.CallStatus(id)
	if err != nil {
		t.Fatalf("CallStatus(%s): %v", id, err)
	}
	if info.State != CallConnected || info.Agent != agent {
		t.Fatalf("call %s = %v (agent %q), want connected to %s", id, info.State, info.Agent, agent)
	}
}
