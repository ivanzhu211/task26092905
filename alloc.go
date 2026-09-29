package callcenter

import "container/heap"

// 本文件的函数均要求在调用方持有相应组件锁的前提下执行。

// insertLocked 把来电放入队列等待堆，等待数加一。
func (q *queue) insertLocked(c *call, seq uint64) {
	e := &callEntry{c: c, seq: seq}
	c.entry = e
	q.members[c] = struct{}{}
	heap.Push(&q.heap, e)
	q.waiting++
}

// removeLocked 把来电移出队列，幂等：返回是否真正移除。
func (q *queue) removeLocked(c *call) bool {
	if _, ok := q.members[c]; !ok {
		return false
	}
	delete(q.members, c)
	heap.Remove(&q.heap, c.entry.idx)
	c.entry = nil
	q.waiting--
	return true
}

// topLocked 返回堆顶来电；调用方需自行确认其状态仍有效。
func (q *queue) topLocked() *call {
	if len(q.heap) == 0 {
		return nil
	}
	return q.heap[0].c
}

// betterCall 报告 a 是否应排在 b 之前（与堆序一致）。
func betterCall(a, b *call, aSeq, bSeq uint64) bool {
	if a.priority != b.priority {
		return a.priority > b.priority
	}
	if a.firstArrive != b.firstArrive {
		return a.firstArrive < b.firstArrive
	}
	return aSeq < bSeq
}

// assignLocked 建立坐席与来电的绑定。调用方必须已通过 CAS
// 将来电状态从等待改为接通。
func (a *agent) assignLocked(c *call) {
	c.agentID.Store(a.id)
	c.connQueue = c.queue.Load()
	a.state = AgentTalking
	a.cur = c
}

// allocateForQueue 在来电进入队列后调用：若有可服务该队列的
// 空闲坐席，把队列中最优的来电分配给空闲最久的坐席（并列取 ID 小者）。
// 返回是否发生分配。需在 q.comp 锁内调用。
func (e *Engine) allocateForQueue(q *queue) bool {
	var best *agent
	for _, a := range q.agents {
		if a.state != AgentIdle {
			continue
		}
		if best == nil || a.idleSince < best.idleSince ||
			(a.idleSince == best.idleSince && a.id < best.id) {
			best = a
		}
	}
	if best == nil {
		return false
	}
	for len(q.heap) > 0 {
		c := q.topLocked()
		if c.state.CompareAndSwap(int32(CallWaiting), int32(CallConnected)) {
			q.removeLocked(c)
			best.assignLocked(c)
			return true
		}
		// 与 Abandon 竞争失败：该来电已终态，丢弃后继续看下一个。
		q.removeLocked(c)
	}
	return false
}

// allocateForAgent 在坐席变为空闲后调用：从其服务的全部队列中
// 取优先级最高、同级中首次进入系统最早的来电分配给它。
// 返回是否发生分配。需在 a.comp 锁内调用。
func (e *Engine) allocateForAgent(a *agent) bool {
	for {
		var bestQ *queue
		var bestC *call
		var bestSeq uint64
		for _, q := range a.queues {
			c := q.topLocked()
			if c == nil || c.st() != CallWaiting {
				continue
			}
			if bestC == nil || betterCall(c, bestC, c.entry.seq, bestSeq) {
				bestQ, bestC, bestSeq = q, c, c.entry.seq
			}
		}
		if bestC == nil {
			return false
		}
		if bestC.state.CompareAndSwap(int32(CallWaiting), int32(CallConnected)) {
			bestQ.removeLocked(bestC)
			a.assignLocked(bestC)
			return true
		}
		// 与 Abandon 竞争失败：丢弃该来电后重试。
		bestQ.removeLocked(bestC)
	}
}
