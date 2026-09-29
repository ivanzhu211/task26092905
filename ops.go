package callcenter

import "fmt"

// Enqueue 来电入队。priority 取值 1 到 10，大者优先。
// 队列已满时立即执行该队列自己的溢出规则。
func (e *Engine) Enqueue(callID, queueName string, priority int) error {
	q, ok := e.queues[queueName]
	if !ok {
		return ErrUnknownQueue
	}
	if priority < 1 || priority > 10 {
		return ErrInvalidPriority
	}
	now := e.nowTime()
	c := &call{
		id:          callID,
		priority:    priority,
		firstArrive: now,
		entryTime:   now,
	}
	c.state.Store(int32(CallWaiting))
	c.queue.Store(q)
	if _, loaded := e.calls.LoadOrStore(callID, c); loaded {
		return ErrDuplicateCall
	}
	e.enterQueue(c, q, now)
	return nil
}

// enterQueue 尝试让来电进入队列：若队列已满则执行该队列的溢出规则，
// 否则入队、登记溢出定时事件并尝试分配。调用时不持有任何组件锁。
func (e *Engine) enterQueue(c *call, q *queue, at int64) {
	c.queue.Store(q)
	comp := q.comp
	comp.mu.Lock()
	if c.st() != CallWaiting {
		// 入队前已被放弃。
		comp.mu.Unlock()
		return
	}
	if q.waiting >= q.capacity {
		comp.mu.Unlock()
		e.applyOverflow(c, q, at, map[*queue]bool{q: true})
		return
	}
	c.entryTime = at
	c.gen++
	e.insertLocked(c, q)
	comp.pushEvent(&event{
		at:      at + q.threshold,
		kind:    evOverflow,
		seq:     e.seq.Add(1),
		cl:      c,
		callQ:   q,
		callGen: c.gen,
	})
	comp.mu.Unlock()

	comp.mu.Lock()
	e.allocateForQueue(q)
	comp.mu.Unlock()
}

// insertLocked 是 queue.insertLocked 的包装，统一序号来源。
func (e *Engine) insertLocked(c *call, q *queue) {
	q.insertLocked(c, e.seq.Add(1))
}

// applyOverflow 对来电执行队列 q 的溢出规则；不持有任何组件锁。
// visited 记录本轮连续转移已经过的队列，绕回时以断开结束。
// at 是溢出发生的逻辑时间。
func (e *Engine) applyOverflow(c *call, q *queue, at int64, visited map[*queue]bool) {
	for {
		rule := q.overflow
		switch rule.Kind {
		case OverflowVoicemail:
			c.state.CompareAndSwap(int32(CallWaiting), int32(CallVoicemail))
			return
		case OverflowDisconnect:
			c.state.CompareAndSwap(int32(CallWaiting), int32(CallDisconnected))
			return
		case OverflowTransfer:
			target := e.queues[rule.Target]
			if visited[target] {
				c.state.CompareAndSwap(int32(CallWaiting), int32(CallDisconnected))
				return
			}
			visited[target] = true
			e.transfer(c, target, at, visited)
			return
		}
	}
}

// transfer 把来电转入目标队列；若目标队列已满，继续执行目标队列
// 自己的溢出规则（同一轮连续转移，共享 visited）。
func (e *Engine) transfer(c *call, target *queue, at int64, visited map[*queue]bool) {
	c.queue.Store(target)
	comp := target.comp
	comp.mu.Lock()
	if c.st() != CallWaiting {
		comp.mu.Unlock()
		return
	}
	if target.waiting >= target.capacity {
		comp.mu.Unlock()
		e.applyOverflow(c, target, at, visited)
		return
	}
	c.entryTime = at
	c.gen++
	e.insertLocked(c, target)
	comp.pushEvent(&event{
		at:      at + target.threshold,
		kind:    evOverflow,
		seq:     e.seq.Add(1),
		cl:      c,
		callQ:   target,
		callGen: c.gen,
	})
	comp.mu.Unlock()

	comp.mu.Lock()
	e.allocateForQueue(target)
	comp.mu.Unlock()
}

// Abandon 来电方在等待中挂断。与分配竞争时只能一方生效。
func (e *Engine) Abandon(callID string) error {
	v, ok := e.calls.Load(callID)
	if !ok {
		return ErrUnknownCall
	}
	c := v.(*call)
	if !c.state.CompareAndSwap(int32(CallWaiting), int32(CallAbandoned)) {
		return ErrCallNotWaiting
	}
	// 从当前所在队列移除；若来电正处于两次入队之间的转移窗口，
	// 移除会落空，由转移方在入队前检查状态并中止。
	q := c.queue.Load()
	q.comp.mu.Lock()
	q.removeLocked(c)
	q.comp.mu.Unlock()
	return nil
}

// EndCall 坐席挂机，进入接通所在队列的话后时长。
func (e *Engine) EndCall(callID string) error {
	v, ok := e.calls.Load(callID)
	if !ok {
		return ErrUnknownCall
	}
	c := v.(*call)
	if c.st() != CallConnected {
		return ErrCallNotConnected
	}
	agentID, _ := c.agentID.Load().(string)
	a := e.agents[agentID]
	if a == nil {
		return ErrCallNotConnected
	}
	comp := a.comp
	comp.mu.Lock()
	defer comp.mu.Unlock()
	if a.state != AgentTalking || a.cur != c {
		return ErrCallNotConnected
	}
	a.cur = nil
	now := e.nowTime()
	a.state = AgentAfterCallWork
	a.acwGen++
	a.acwEnd = now + int64(c.connQueue.acw)
	if a.acwEnd <= now {
		// 话后时长为 0：立即到期。
		e.finishACWLocked(a, now)
		return nil
	}
	comp.pushEvent(&event{
		at:     a.acwEnd,
		kind:   evACWEnd,
		seq:    e.seq.Add(1),
		ag:     a,
		acwGen: a.acwGen,
	})
	return nil
}

// finishACWLocked 在组件锁内结束坐席的话后状态：
// 应用挂起的状态申请（若有），否则恢复空闲并尝试分配。
func (e *Engine) finishACWLocked(a *agent, at int64) {
	if a.pending != nil {
		a.state = *a.pending
		a.pending = nil
	} else {
		a.state = AgentIdle
	}
	if a.state == AgentIdle {
		a.idleSince = at
		e.allocateForAgent(a)
	}
}

// SetAgentState 申请切换坐席状态（空闲/小休/离线）。
// 通话或话后期间的申请到话后结束时才生效。
func (e *Engine) SetAgentState(agentID string, state AgentState) error {
	a, ok := e.agents[agentID]
	if !ok {
		return ErrUnknownAgent
	}
	switch state {
	case AgentIdle, AgentBreak, AgentOffline:
	default:
		return fmt.Errorf("callcenter: cannot set agent state to %s", state)
	}
	comp := a.comp
	comp.mu.Lock()
	defer comp.mu.Unlock()
	switch a.state {
	case AgentTalking, AgentAfterCallWork:
		s := state
		a.pending = &s
	default:
		if a.state == state {
			return nil
		}
		a.state = state
		if state == AgentIdle {
			a.idleSince = e.nowTime()
			e.allocateForAgent(a)
		}
	}
	return nil
}

// CallStatus 查询来电状态。
func (e *Engine) CallStatus(callID string) (CallInfo, error) {
	v, ok := e.calls.Load(callID)
	if !ok {
		return CallInfo{}, ErrUnknownCall
	}
	c := v.(*call)
	info := CallInfo{State: c.st()}
	if q := c.queue.Load(); q != nil {
		info.Queue = q.name
	}
	if id, _ := c.agentID.Load().(string); info.State == CallConnected {
		info.Agent = id
	}
	return info, nil
}

// AgentStatus 查询坐席状态。
func (e *Engine) AgentStatus(agentID string) (AgentInfo, error) {
	a, ok := e.agents[agentID]
	if !ok {
		return AgentInfo{}, ErrUnknownAgent
	}
	a.comp.mu.Lock()
	defer a.comp.mu.Unlock()
	info := AgentInfo{State: a.state}
	if a.state == AgentTalking && a.cur != nil {
		info.Call = a.cur.id
	}
	info.Pending = a.pending
	return info, nil
}
