package callcenter

import (
	"container/heap"
	"time"
)

// Advance 推进时间。一次跨过多个到期时刻时按先后逐个处理；
// 同一时刻的话后结束先于溢出。事件处理只持有对应组件锁，
// 不同组件的事件处理互不阻塞。
func (e *Engine) Advance(d time.Duration) error {
	if d < 0 {
		return ErrInvalidDuration
	}
	e.advMu.Lock()
	defer e.advMu.Unlock()
	target := e.now.Add(int64(d))
	for {
		ev, comp := e.earliestEvent()
		if ev == nil || ev.at > target {
			return nil
		}
		comp.mu.Lock()
		if len(comp.events) == 0 || comp.events[0] != ev {
			// 堆顶被并发插入改变（新事件只会更晚，防御性重试）。
			comp.mu.Unlock()
			continue
		}
		heap.Pop(&comp.events)
		var overflow *call
		var overflowQ *queue
		switch ev.kind {
		case evACWEnd:
			a := ev.ag
			if a.state == AgentAfterCallWork && a.acwGen == ev.acwGen {
				e.finishACWLocked(a, ev.at)
			}
		case evOverflow:
			c := ev.cl
			if c.st() == CallWaiting && c.queue.Load() == ev.callQ && c.gen == ev.callGen {
				ev.callQ.removeLocked(c)
				overflow = c
				overflowQ = ev.callQ
			}
		}
		comp.mu.Unlock()
		if overflow != nil {
			e.applyOverflow(overflow, overflowQ, ev.at, map[*queue]bool{overflowQ: true})
		}
	}
}

// earliestEvent 扫描各组件的事件堆，返回全局最早的到期事件。
func (e *Engine) earliestEvent() (*event, *component) {
	var best *event
	var bestComp *component
	for _, c := range e.comps {
		c.mu.Lock()
		if len(c.events) > 0 {
			ev := c.events[0]
			if best == nil || eventLess(ev, best) {
				best = ev
				bestComp = c
			}
		}
		c.mu.Unlock()
	}
	return best, bestComp
}

// eventLess 与 eventHeap 的排序一致：时间升序，同刻话后结束先于溢出，再按序号。
func eventLess(a, b *event) bool {
	if a.at != b.at {
		return a.at < b.at
	}
	if a.kind != b.kind {
		return a.kind < b.kind
	}
	return a.seq < b.seq
}
