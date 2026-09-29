package callcenter

import "time"

const maxTime = time.Duration(1<<62 - 1)

// Advance moves the engine clock forward by d, processing wrapup ends and
// queue overflows in chronological order. At the same instant, wrapup ends
// are processed before overflows.
func (e *Engine) Advance(d time.Duration) {
	if d < 0 {
		return
	}
	e.advanceMu.Lock()
	defer e.advanceMu.Unlock()

	target := e.clock() + d
	overflowCount := map[*call]int{}
	for {
		wrapAt, ofAt := e.nextEventTimes()
		t := wrapAt
		if ofAt < t {
			t = ofAt
		}
		if t > target {
			break
		}
		e.now.Store(int64(t))

		// Wrapup ends first at this instant.
		for _, a := range e.agents {
			a.mu.Lock()
			due := a.state == AgentWrapup && a.wrapupEnd <= t
			a.mu.Unlock()
			if due {
				e.finishWrapup(a, t)
			}
		}
		// Then overflows due at this instant.
		for _, q := range e.queues {
			var expired []*call
			q.mu.Lock()
			remaining := q.waiting[:0]
			for _, c := range q.waiting {
				c.mu.Lock()
				deadline := c.enteredAt + q.threshold
				c.mu.Unlock()
				if deadline <= t {
					expired = append(expired, c)
				} else {
					remaining = append(remaining, c)
				}
			}
			q.waiting = remaining
			q.mu.Unlock()
			for _, c := range expired {
				overflowCount[c]++
				if overflowCount[c] > len(e.queues) {
					// Safety valve against zero-threshold transfer loops.
					e.terminateCall(c, CallDisconnected)
					continue
				}
				e.overflowCall(c, q, nil, t)
			}
		}
	}
	e.now.Store(int64(target))
}

// nextEventTimes returns the earliest pending wrapup-end time and the
// earliest overflow deadline across all queues (maxTime if none).
func (e *Engine) nextEventTimes() (wrapAt, overflowAt time.Duration) {
	wrapAt, overflowAt = maxTime, maxTime
	for _, a := range e.agents {
		a.mu.Lock()
		if a.state == AgentWrapup && a.wrapupEnd < wrapAt {
			wrapAt = a.wrapupEnd
		}
		a.mu.Unlock()
	}
	for _, q := range e.queues {
		q.mu.Lock()
		for _, c := range q.waiting {
			c.mu.Lock()
			if d := c.enteredAt + q.threshold; d < overflowAt {
				overflowAt = d
			}
			c.mu.Unlock()
		}
		q.mu.Unlock()
	}
	return wrapAt, overflowAt
}
