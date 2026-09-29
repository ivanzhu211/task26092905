package callcenter

import (
	"sync"
	"sync/atomic"
	"time"
)

// Engine is the in-memory call center queueing and dispatch engine.
type Engine struct {
	queues map[string]*queue
	agents map[string]*agent

	callsMu sync.RWMutex
	calls   map[string]*call

	advanceMu sync.Mutex // serializes Advance only
	now       atomic.Int64
}

// New builds an Engine from the given configuration.
func New(cfg Config) *Engine {
	e := &Engine{
		queues: make(map[string]*queue, len(cfg.Queues)),
		agents: make(map[string]*agent, len(cfg.Agents)),
		calls:  make(map[string]*call),
	}
	for _, qc := range cfg.Queues {
		cap := qc.Capacity
		if cap <= 0 {
			cap = 1
		}
		if cap > 150 {
			cap = 150
		}
		e.queues[qc.Name] = &queue{
			name:      qc.Name,
			capacity:  cap,
			threshold: qc.Threshold,
			wrapup:    qc.Wrapup,
			overflow:  qc.Overflow,
		}
	}
	for _, ac := range cfg.Agents {
		a := &agent{id: ac.ID, state: AgentIdle}
		for _, qn := range ac.Queues {
			if q, ok := e.queues[qn]; ok {
				a.queues = append(a.queues, q)
				q.agents = append(q.agents, a)
			}
		}
		e.agents[ac.ID] = a
	}
	return e
}

func (e *Engine) clock() time.Duration { return time.Duration(e.now.Load()) }

// Enqueue adds a call to a queue.
func (e *Engine) Enqueue(callID, queueName string, priority int) {
	q, ok := e.queues[queueName]
	if !ok {
		return
	}
	if priority < 1 {
		priority = 1
	}
	if priority > 10 {
		priority = 10
	}
	now := e.clock()
	c := &call{
		id:           callID,
		queue:        queueName,
		priority:     priority,
		firstArrival: now,
		enteredAt:    now,
		state:        CallWaiting,
	}
	e.callsMu.Lock()
	if _, dup := e.calls[callID]; dup {
		e.callsMu.Unlock()
		return
	}
	e.calls[callID] = c
	e.callsMu.Unlock()

	q.mu.Lock()
	if len(q.waiting) >= q.capacity {
		q.mu.Unlock()
		e.overflowCall(c, q, nil, now)
		return
	}
	q.waiting = append(q.waiting, c)
	q.mu.Unlock()
	e.dispatchFromQueue(q)
}

// Abandon removes a waiting call (caller hung up while waiting).
func (e *Engine) Abandon(callID string) {
	e.callsMu.RLock()
	c, ok := e.calls[callID]
	e.callsMu.RUnlock()
	if !ok {
		return
	}
	for {
		c.mu.Lock()
		if c.state != CallWaiting {
			c.mu.Unlock()
			return
		}
		q := e.queues[c.queue]
		c.mu.Unlock()

		q.mu.Lock()
		c.mu.Lock()
		if c.state == CallWaiting && c.queue == q.name && q.stillWaitingLocked(c) {
			q.removeLocked(c)
			c.state = CallAbandoned
			c.mu.Unlock()
			q.mu.Unlock()
			return
		}
		c.mu.Unlock()
		q.mu.Unlock()
		// Call moved concurrently (overflow transfer or dispatch); retry.
	}
}

// CallStatus returns a snapshot of a call.
func (e *Engine) CallStatus(callID string) CallInfo {
	e.callsMu.RLock()
	c, ok := e.calls[callID]
	e.callsMu.RUnlock()
	if !ok {
		return CallInfo{ID: callID}
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return CallInfo{
		ID:     c.id,
		State:  c.state,
		Queue:  c.queue,
		Agent:  c.agent,
		Exists: true,
	}
}

// AgentStatus returns a snapshot of an agent.
func (e *Engine) AgentStatus(agentID string) AgentInfo {
	a, ok := e.agents[agentID]
	if !ok {
		return AgentInfo{ID: agentID}
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	info := AgentInfo{ID: a.id, State: a.state, Exists: true}
	if a.call != nil {
		info.Call = a.call.id
	}
	return info
}

// QueueLen returns the number of calls currently waiting in the named
// queue, or -1 if the queue does not exist.
func (e *Engine) QueueLen(name string) int {
	q, ok := e.queues[name]
	if !ok {
		return -1
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	return len(q.waiting)
}
