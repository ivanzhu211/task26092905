package callcenter

import "runtime"

// bestCallLocked returns the highest-priority, then earliest-first-arrival
// waiting call. Caller must hold q.mu.
func (q *queue) bestCallLocked() *call {
	var best *call
	for _, c := range q.waiting {
		if best == nil || c.priority > best.priority ||
			(c.priority == best.priority && c.firstArrival < best.firstArrival) {
			best = c
		}
	}
	return best
}

// removeLocked removes c from the waiting list. Caller must hold q.mu.
func (q *queue) removeLocked(c *call) {
	for i, w := range q.waiting {
		if w == c {
			q.waiting = append(q.waiting[:i], q.waiting[i+1:]...)
			return
		}
	}
}

// stillWaitingLocked reports whether c is still in the waiting list.
// Caller must hold q.mu.
func (q *queue) stillWaitingLocked(c *call) bool {
	for _, w := range q.waiting {
		if w == c {
			return true
		}
	}
	return false
}

// bindLocked assigns c to a. Caller must hold a.mu and c's queue mu.
func bindLocked(a *agent, c *call) {
	c.mu.Lock()
	c.state = CallAnswered
	c.agent = a.id
	c.mu.Unlock()
	a.call = c
	a.state = AgentInCall
}

// dispatchFromQueue tries to assign the best waiting call of q to an idle
// agent serving q, preferring the longest-idle agent. Called after a call
// enters q. Must not be called with any queue or agent lock held.
func (e *Engine) dispatchFromQueue(q *queue) {
	q.mu.Lock()
	empty := len(q.waiting) == 0
	q.mu.Unlock()
	if empty {
		return
	}
	a := e.longestIdleAgent(q)
	if a == nil {
		return
	}
	// a.mu is held here and a is idle.
	q.mu.Lock()
	c := q.bestCallLocked()
	if c == nil {
		q.mu.Unlock()
		a.mu.Unlock()
		return
	}
	q.removeLocked(c)
	bindLocked(a, c)
	q.mu.Unlock()
	a.mu.Unlock()
}

// longestIdleAgent finds and locks an idle agent serving q, preferring the
// longest-idle one. Returns nil if no idle agent is available. Agents whose
// locks are contended are skipped; if any were skipped and no idle agent was
// found, the scan retries so that a concurrently released agent is not
// missed (preserving the no-idle-pairing invariant).
func (e *Engine) longestIdleAgent(q *queue) *agent {
	for {
		var best *agent
		contended := false
		for _, a := range q.agents {
			if !a.mu.TryLock() {
				contended = true
				continue
			}
			if a.state == AgentIdle && (best == nil || a.idleSince < best.idleSince) {
				if best != nil {
					best.mu.Unlock()
				}
				best = a
			} else {
				a.mu.Unlock()
			}
		}
		if best != nil {
			return best
		}
		if !contended {
			return nil
		}
		runtime.Gosched()
	}
}

// pullForAgent lets an idle agent take the best call across all its queues.
// Must not be called with any lock held.
func (e *Engine) pullForAgent(a *agent) {
	a.mu.Lock()
	if a.state != AgentIdle {
		a.mu.Unlock()
		return
	}
	defer a.mu.Unlock()

	for {
		var bestQ *queue
		var best *call
		for _, q := range a.queues {
			q.mu.Lock()
			c := q.bestCallLocked()
			q.mu.Unlock()
			if c != nil && (best == nil || c.priority > best.priority ||
				(c.priority == best.priority && c.firstArrival < best.firstArrival)) {
				best = c
				bestQ = q
			}
		}
		if best == nil {
			return
		}
		bestQ.mu.Lock()
		if bestQ.stillWaitingLocked(best) {
			bestQ.removeLocked(best)
			bindLocked(a, best)
			bestQ.mu.Unlock()
			return
		}
		bestQ.mu.Unlock()
		// Call was taken concurrently; rescan.
	}
}
