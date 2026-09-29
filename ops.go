package callcenter

import "time"

// EndCall ends the call an agent is on; the agent enters wrapup for the
// queue the call was answered from, then automatically becomes available.
func (e *Engine) EndCall(callID string) {
	e.callsMu.RLock()
	c, ok := e.calls[callID]
	e.callsMu.RUnlock()
	if !ok {
		return
	}
	c.mu.Lock()
	if c.state != CallAnswered {
		c.mu.Unlock()
		return
	}
	agentID := c.agent
	queueName := c.queue
	c.mu.Unlock()

	a := e.agents[agentID]
	if a == nil {
		return
	}
	wrapup := e.queues[queueName].wrapup
	now := e.clock()

	a.mu.Lock()
	if a.state != AgentInCall || a.call != c {
		a.mu.Unlock()
		return
	}
	if wrapup <= 0 {
		a.mu.Unlock()
		e.finishWrapup(a, now)
		return
	}
	a.state = AgentWrapup
	a.wrapupEnd = now + wrapup
	a.mu.Unlock()
}

// SetAgentState requests an agent state change. Requests made while the
// agent is in a call or wrapup take effect when wrapup ends.
func (e *Engine) SetAgentState(agentID string, state AgentState) {
	a, ok := e.agents[agentID]
	if !ok {
		return
	}
	switch state {
	case AgentIdle, AgentOnBreak, AgentOffline:
	default:
		return
	}
	now := e.clock()

	a.mu.Lock()
	if a.state == AgentInCall || a.state == AgentWrapup {
		a.pendingState = state
		a.hasPending = true
		a.mu.Unlock()
		return
	}
	if state == AgentIdle && a.state != AgentIdle {
		a.state = AgentIdle
		a.idleSince = now
		a.mu.Unlock()
		e.pullForAgent(a)
		return
	}
	a.state = state
	a.mu.Unlock()
}

// finishWrapup completes an agent's wrapup: unbinds the call, applies any
// pending state request, and if the agent becomes idle, pulls a call.
// Must not be called with any lock held.
func (e *Engine) finishWrapup(a *agent, now time.Duration) {
	a.mu.Lock()
	if a.state != AgentWrapup {
		a.mu.Unlock()
		return
	}
	a.call = nil
	next := AgentIdle
	if a.hasPending {
		next = a.pendingState
		a.hasPending = false
	}
	a.state = next
	if next == AgentIdle {
		a.idleSince = now
	}
	a.mu.Unlock()
	if next == AgentIdle {
		e.pullForAgent(a)
	}
}
