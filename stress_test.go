package callcenter

import (
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

const (
	stressQueues = 8
	stressAgents = 30
	stressCalls  = 4000
)

func buildStressConfig() (Config, map[string][]string) {
	cfg := Config{}
	queueAgents := map[string][]string{}
	for i := 0; i < stressQueues; i++ {
		rule := OverflowRule{Action: OverflowVoicemail}
		switch i % 3 {
		case 0:
			if i+1 < stressQueues {
				rule = OverflowRule{Action: OverflowTransfer, Target: fmt.Sprintf("q%d", i+1)}
			}
		case 1:
			rule = OverflowRule{Action: OverflowDisconnect}
		}
		cfg.Queues = append(cfg.Queues, QueueConfig{
			Name:      fmt.Sprintf("q%d", i),
			Capacity:  5 + i*2,
			Threshold: time.Duration(20+i*5) * time.Millisecond,
			Wrapup:    time.Duration(1+i%3) * time.Millisecond,
			Overflow:  rule,
		})
	}
	rng := rand.New(rand.NewSource(42))
	for i := 0; i < stressAgents; i++ {
		id := fmt.Sprintf("a%d", i)
		k := 2 + rng.Intn(2) // 2 or 3 queues
		perm := rng.Perm(stressQueues)[:k]
		var qs []string
		for _, qi := range perm {
			qn := fmt.Sprintf("q%d", qi)
			qs = append(qs, qn)
			queueAgents[qn] = append(queueAgents[qn], id)
		}
		cfg.Agents = append(cfg.Agents, AgentConfig{ID: id, Queues: qs})
	}
	return cfg, queueAgents
}

func TestStress(t *testing.T) {
	cfg, queueAgents := buildStressConfig()
	e := New(cfg)

	var idsMu sync.Mutex
	var ids []string
	addID := func(id string) {
		idsMu.Lock()
		ids = append(ids, id)
		idsMu.Unlock()
	}
	pickID := func(r *rand.Rand) (string, bool) {
		idsMu.Lock()
		defer idsMu.Unlock()
		if len(ids) == 0 {
			return "", false
		}
		return ids[r.Intn(len(ids))], true
	}

	var stop atomic.Bool
	var monErrsMu sync.Mutex
	var monErrs []string
	report := func(format string, args ...any) {
		monErrsMu.Lock()
		if len(monErrs) < 50 {
			monErrs = append(monErrs, fmt.Sprintf(format, args...))
		}
		monErrsMu.Unlock()
	}

	// Monitor: continuously check for double-bound agents and over-capacity
	// queues while the system is running.
	var monWG sync.WaitGroup
	monWG.Add(1)
	go func() {
		defer monWG.Done()
		for !stop.Load() {
			bound := map[string]string{}
			for _, ac := range cfg.Agents {
				info := e.AgentStatus(ac.ID)
				if info.Call == "" {
					continue
				}
				if prev, dup := bound[info.Call]; dup {
					report("call %s bound to both %s and %s", info.Call, prev, ac.ID)
				}
				bound[info.Call] = ac.ID
				cs := e.CallStatus(info.Call)
				if cs.Exists && cs.State == CallAnswered && cs.Agent != ac.ID {
					report("agent %s bound to %s but call says agent %q",
						ac.ID, info.Call, cs.Agent)
				}
			}
			for _, qc := range cfg.Queues {
				if n := e.QueueLen(qc.Name); n > qc.Capacity {
					report("queue %s over capacity: %d > %d", qc.Name, n, qc.Capacity)
				}
			}
		}
	}()

	var enqWG, bgWG sync.WaitGroup

	// Enqueuers: thousands of calls across all queues.
	for w := 0; w < 4; w++ {
		enqWG.Add(1)
		go func(w int) {
			defer enqWG.Done()
			r := rand.New(rand.NewSource(int64(1000 + w)))
			for i := 0; i < stressCalls/4; i++ {
				id := fmt.Sprintf("c-%d-%d", w, i)
				addID(id)
				e.Enqueue(id, fmt.Sprintf("q%d", r.Intn(stressQueues)), 1+r.Intn(10))
			}
		}(w)
	}

	// Abandoners.
	for w := 0; w < 2; w++ {
		bgWG.Add(1)
		go func(w int) {
			defer bgWG.Done()
			r := rand.New(rand.NewSource(int64(2000 + w)))
			for !stop.Load() {
				if id, ok := pickID(r); ok {
					e.Abandon(id)
				}
			}
		}(w)
	}

	// Call enders.
	for w := 0; w < 2; w++ {
		bgWG.Add(1)
		go func(w int) {
			defer bgWG.Done()
			r := rand.New(rand.NewSource(int64(3000 + w)))
			for !stop.Load() {
				if id, ok := pickID(r); ok {
					e.EndCall(id)
				}
			}
		}(w)
	}

	// State flippers.
	states := []AgentState{AgentIdle, AgentOnBreak, AgentOffline}
	for w := 0; w < 2; w++ {
		bgWG.Add(1)
		go func(w int) {
			defer bgWG.Done()
			r := rand.New(rand.NewSource(int64(4000 + w)))
			for !stop.Load() {
				a := cfg.Agents[r.Intn(len(cfg.Agents))]
				e.SetAgentState(a.ID, states[r.Intn(len(states))])
			}
		}(w)
	}

	// Time advancer: drives overflows and wrapup ends.
	bgWG.Add(1)
	go func() {
		defer bgWG.Done()
		for !stop.Load() {
			e.Advance(time.Millisecond)
		}
	}()

	enqWG.Wait()
	stop.Store(true)
	bgWG.Wait()
	monWG.Wait()

	monErrsMu.Lock()
	for _, m := range monErrs {
		t.Error(m)
	}
	monErrsMu.Unlock()
	if t.Failed() {
		return
	}

	// Quiesced: no queue may have a waiting call while an agent serving it
	// is idle.
	for _, qc := range cfg.Queues {
		if e.QueueLen(qc.Name) == 0 {
			continue
		}
		for _, aid := range queueAgents[qc.Name] {
			if info := e.AgentStatus(aid); info.State == AgentIdle {
				t.Errorf("queue %s has %d waiting but agent %s is idle",
					qc.Name, e.QueueLen(qc.Name), aid)
			}
		}
	}

	// Drain: free all agents and advance until every call reaches a
	// terminal state.
	for _, ac := range cfg.Agents {
		e.SetAgentState(ac.ID, AgentIdle)
	}
	drained := false
	for i := 0; i < 5000; i++ {
		e.Advance(100 * time.Millisecond)
		for _, ac := range cfg.Agents {
			info := e.AgentStatus(ac.ID)
			if info.State == AgentInCall {
				e.EndCall(info.Call)
			}
		}
		all := true
		for _, id := range ids {
			s := e.CallStatus(id).State
			if s == CallWaiting {
				all = false
				break
			}
		}
		if all {
			drained = true
			break
		}
	}
	if !drained {
		t.Fatal("calls did not drain to terminal states")
	}

	// Every call has exactly one terminal state.
	counts := map[CallState]int{}
	for _, id := range ids {
		cs := e.CallStatus(id)
		if !cs.Exists {
			t.Errorf("call %s missing", id)
			continue
		}
		switch cs.State {
		case CallAnswered:
			if cs.Agent == "" {
				t.Errorf("call %s answered but no agent recorded", id)
			}
		case CallVoicemail, CallDisconnected, CallAbandoned:
		default:
			t.Errorf("call %s in non-terminal state %v after drain", id, cs.State)
		}
		counts[cs.State]++
	}
	t.Logf("terminal states: answered=%d voicemail=%d disconnected=%d abandoned=%d",
		counts[CallAnswered], counts[CallVoicemail], counts[CallDisconnected], counts[CallAbandoned])
}
