package callcenter

import (
	"fmt"
	"math/rand"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// 压力测试：8 个队列、30 个坐席（各服务 2-3 个队列）、4000 通来电，
// 多 goroutine 并发入队/放弃/挂机/切换状态，另有 goroutine 推进时间。
// 运行中检测坐席双绑定与队列超容；结束后校验每通来电恰好一个终态、
// 不存在闲置配对。
func TestStress(t *testing.T) {
	seed := int64(42)
	if s := os.Getenv("STRESS_SEED"); s != "" {
		if v, err := strconv.ParseInt(s, 10, 64); err == nil {
			seed = v
		}
	}
	rng := rand.New(rand.NewSource(seed))

	const (
		numQueues = 8
		numAgents = 30
		numCalls  = 4000
	)
	qcfgs := make([]QueueConfig, numQueues)
	for i := range qcfgs {
		var rule OverflowRule
		switch i % 3 {
		case 0: // 转入下一队列（构成环，考验连续转移回环断开）
			rule = OverflowRule{Kind: OverflowTransfer, Target: fmt.Sprintf("q%d", (i+1)%numQueues)}
		case 1:
			rule = OverflowRule{Kind: OverflowVoicemail}
		case 2:
			rule = OverflowRule{Kind: OverflowDisconnect}
		}
		qcfgs[i] = QueueConfig{
			Name:          fmt.Sprintf("q%d", i),
			Capacity:      20 + rng.Intn(80),
			WaitThreshold: time.Duration(5+rng.Intn(45)) * time.Millisecond,
			AfterCallWork: time.Duration(1+rng.Intn(20)) * time.Millisecond,
			Overflow:      rule,
		}
	}
	acfgs := make([]AgentConfig, numAgents)
	for i := range acfgs {
		n := 2 + rng.Intn(2)
		perm := rng.Perm(numQueues)[:n]
		queues := make([]string, n)
		for j, qi := range perm {
			queues[j] = fmt.Sprintf("q%d", qi)
		}
		acfgs[i] = AgentConfig{ID: fmt.Sprintf("a%d", i), Queues: queues}
	}
	eng, err := NewEngine(Config{Queues: qcfgs, Agents: acfgs})
	if err != nil {
		t.Fatalf("NewEngine: %v", err)
	}

	var stop atomic.Bool
	var enqueued atomic.Int64
	var wg sync.WaitGroup

	// 入队 worker。
	for w := 0; w < 4; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			r := rand.New(rand.NewSource(int64(w)))
			for i := w; i < numCalls; i += 4 {
				id := fmt.Sprintf("call-%d", i)
				q := fmt.Sprintf("q%d", r.Intn(numQueues))
				if err := eng.Enqueue(id, q, 1+r.Intn(10)); err != nil {
					t.Errorf("Enqueue(%s): %v", id, err)
					return
				}
				enqueued.Add(1)
			}
		}(w)
	}
	// 放弃 worker。
	for w := 0; w < 2; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			r := rand.New(rand.NewSource(int64(100 + w)))
			for !stop.Load() {
				n := enqueued.Load()
				if n > 0 {
					_ = eng.Abandon(fmt.Sprintf("call-%d", r.Int63n(n)))
				}
			}
		}(w)
	}
	// 挂机 worker。
	for w := 0; w < 2; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			r := rand.New(rand.NewSource(int64(200 + w)))
			for !stop.Load() {
				n := enqueued.Load()
				if n > 0 {
					_ = eng.EndCall(fmt.Sprintf("call-%d", r.Int63n(n)))
				}
			}
		}(w)
	}
	// 坐席状态切换 worker。
	wg.Add(1)
	go func() {
		defer wg.Done()
		r := rand.New(rand.NewSource(300))
		states := []AgentState{AgentIdle, AgentBreak, AgentOffline}
		for !stop.Load() {
			a := fmt.Sprintf("a%d", r.Intn(numAgents))
			_ = eng.SetAgentState(a, states[r.Intn(len(states))])
		}
	}()
	// 时间推进 worker。
	wg.Add(1)
	go func() {
		defer wg.Done()
		r := rand.New(rand.NewSource(400))
		for !stop.Load() {
			if err := eng.Advance(time.Duration(1+r.Intn(5)) * time.Millisecond); err != nil {
				t.Errorf("Advance: %v", err)
				return
			}
			time.Sleep(50 * time.Microsecond)
		}
	}()
	// 运行中不变量检查：坐席双绑定、队列超容、终态不可变。
	for w := 0; w < 2; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			r := rand.New(rand.NewSource(int64(500 + w)))
			terminal := map[string]CallState{}
			for !stop.Load() {
				checkStructure(t, eng)
				n := enqueued.Load()
				if n > 0 {
					id := fmt.Sprintf("call-%d", r.Int63n(n))
					info, err := eng.CallStatus(id)
					if err == nil {
						if prev, ok := terminal[id]; ok && prev != info.State {
							t.Errorf("call %s terminal state changed: %v -> %v", id, prev, info.State)
						}
						if info.State.Terminal() {
							terminal[id] = info.State
						}
					}
				}
			}
		}(w)
	}

	// 等入队完成后再让其余 worker 跑一小段时间。
	for enqueued.Load() < numCalls {
		time.Sleep(time.Millisecond)
	}
	time.Sleep(50 * time.Millisecond)
	stop.Store(true)
	wg.Wait()

	// 静止点：所有并发调用已返回，不得存在闲置配对。
	checkStructure(t, eng)
	checkNoIdlePairing(t, eng)

	// 排空：所有坐席恢复空闲，推进时间直到系统静止。
	for i := 0; i < numAgents; i++ {
		_ = eng.SetAgentState(fmt.Sprintf("a%d", i), AgentIdle)
	}
	quiescent := false
	for i := 0; i < 10000 && !quiescent; i++ {
		eng.calls.Range(func(_, v any) bool {
			c := v.(*call)
			if c.st() == CallConnected {
				_ = eng.EndCall(c.id)
			}
			return true
		})
		if err := eng.Advance(100 * time.Millisecond); err != nil {
			t.Fatalf("Advance: %v", err)
		}
		quiescent = isQuiescent(eng)
	}
	if !quiescent {
		t.Fatal("system did not quiesce")
	}

	// 最终校验：每通来电恰好一个终态；无闲置配对。
	counts := map[CallState]int{}
	for i := 0; i < numCalls; i++ {
		info, err := eng.CallStatus(fmt.Sprintf("call-%d", i))
		if err != nil {
			t.Fatalf("CallStatus(call-%d): %v", i, err)
		}
		if !info.State.Terminal() {
			t.Fatalf("call-%d not terminal after drain: %v", i, info.State)
		}
		if info.State == CallConnected && info.Agent == "" {
			t.Fatalf("call-%d connected but no agent", i)
		}
		counts[info.State]++
	}
	checkStructure(t, eng)
	checkNoIdlePairing(t, eng)
	t.Logf("terminal states: %v", counts)
}

// checkStructure 在白盒下校验：队列不超容、等待计数一致、
// 坐席至多绑定一通来电且绑定关系互不相同。
func checkStructure(t *testing.T, eng *Engine) {
	t.Helper()
	for _, comp := range eng.comps {
		comp.mu.Lock()
		for _, q := range comp.queues {
			if q.waiting > q.capacity {
				t.Errorf("queue %s over capacity: %d > %d", q.name, q.waiting, q.capacity)
			}
			if q.waiting != len(q.members) || q.waiting != len(q.heap) {
				t.Errorf("queue %s inconsistent: waiting=%d members=%d heap=%d",
					q.name, q.waiting, len(q.members), len(q.heap))
			}
		}
		bound := map[*call]*agent{}
		for _, a := range comp.agents {
			if a.cur == nil {
				if a.state == AgentTalking {
					t.Errorf("agent %s talking without call", a.id)
				}
				continue
			}
			if a.state != AgentTalking {
				t.Errorf("agent %s has call but state %v", a.id, a.state)
			}
			if prev, dup := bound[a.cur]; dup {
				t.Errorf("call %s bound to both %s and %s", a.cur.id, prev.id, a.id)
			}
			bound[a.cur] = a
			if a.cur.st() != CallConnected {
				t.Errorf("agent %s bound to non-connected call %s (%v)", a.id, a.cur.id, a.cur.st())
			}
		}
		comp.mu.Unlock()
	}
}

// checkNoIdlePairing 校验不存在“队列有人等待且服务它的坐席空闲”。
func checkNoIdlePairing(t *testing.T, eng *Engine) {
	t.Helper()
	for _, comp := range eng.comps {
		comp.mu.Lock()
		for _, q := range comp.queues {
			if q.waiting == 0 {
				continue
			}
			for _, a := range q.agents {
				if a.state == AgentIdle {
					t.Errorf("queue %s has %d waiting but agent %s idle", q.name, q.waiting, a.id)
				}
			}
		}
		comp.mu.Unlock()
	}
}

// isQuiescent 报告系统是否静止：无等待、无通话/话后、无待处理事件。
func isQuiescent(eng *Engine) bool {
	for _, comp := range eng.comps {
		comp.mu.Lock()
		busy := len(comp.events) > 0
		for _, q := range comp.queues {
			busy = busy || q.waiting > 0
		}
		for _, a := range comp.agents {
			busy = busy || a.state == AgentTalking || a.state == AgentAfterCallWork
		}
		comp.mu.Unlock()
		if busy {
			return false
		}
	}
	return true
}
