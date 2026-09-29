package callcenter

import "time"

// overflowCall applies fromQ's overflow rule to c. visited holds the queues
// already passed in this capacity-driven transfer chain (including fromQ).
// Must not be called with any lock held.
func (e *Engine) overflowCall(c *call, fromQ *queue, visited map[string]bool, now time.Duration) {
	if visited == nil {
		visited = map[string]bool{}
	}
	visited[fromQ.name] = true
	rule := fromQ.overflow
	switch rule.Action {
	case OverflowVoicemail:
		e.terminateCall(c, CallVoicemail)
	case OverflowDisconnect:
		e.terminateCall(c, CallDisconnected)
	case OverflowTransfer:
		target, ok := e.queues[rule.Target]
		if !ok || visited[target.name] {
			e.terminateCall(c, CallDisconnected)
			return
		}
		target.mu.Lock()
		if len(target.waiting) >= target.capacity {
			target.mu.Unlock()
			e.overflowCall(c, target, visited, now)
			return
		}
		c.mu.Lock()
		c.queue = target.name
		c.enteredAt = now
		c.mu.Unlock()
		target.waiting = append(target.waiting, c)
		target.mu.Unlock()
		e.dispatchFromQueue(target)
	}
}

// terminateCall sets a terminal state on a waiting call.
func (e *Engine) terminateCall(c *call, state CallState) {
	c.mu.Lock()
	if c.state == CallWaiting {
		c.state = state
	}
	c.mu.Unlock()
}
