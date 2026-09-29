package callcenter

import (
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
)

// Engine 是呼叫中心排队与分配引擎。所有状态在内存中，
// 时间只由 Advance 推进。所有公开方法可并发调用。
type Engine struct {
	queues map[string]*queue
	agents map[string]*agent
	comps  []*component

	calls sync.Map // callID -> *call
	now   atomic.Int64
	seq   atomic.Uint64 // 全局序号，用于分配与事件的确定性次序
	advMu sync.Mutex    // 仅串行化 Advance 之间的事件处理，不参与分配决策
}

// NewEngine 按配置构造引擎。
func NewEngine(cfg Config) (*Engine, error) {
	if len(cfg.Queues) == 0 {
		return nil, errors.New("callcenter: at least one queue is required")
	}
	e := &Engine{
		queues: make(map[string]*queue, len(cfg.Queues)),
		agents: make(map[string]*agent, len(cfg.Agents)),
	}
	for _, qc := range cfg.Queues {
		if qc.Name == "" {
			return nil, errors.New("callcenter: queue name must not be empty")
		}
		if _, dup := e.queues[qc.Name]; dup {
			return nil, fmt.Errorf("callcenter: duplicate queue %q", qc.Name)
		}
		if qc.Capacity < 1 || qc.Capacity > 150 {
			return nil, fmt.Errorf("callcenter: queue %q capacity must be between 1 and 150", qc.Name)
		}
		if qc.WaitThreshold <= 0 {
			return nil, fmt.Errorf("callcenter: queue %q wait threshold must be positive", qc.Name)
		}
		if qc.AfterCallWork < 0 {
			return nil, fmt.Errorf("callcenter: queue %q after-call work must be non-negative", qc.Name)
		}
		if qc.Overflow.Kind == OverflowTransfer {
			if qc.Overflow.Target == "" {
				return nil, fmt.Errorf("callcenter: queue %q overflow target must not be empty", qc.Name)
			}
		}
		e.queues[qc.Name] = &queue{
			name:      qc.Name,
			capacity:  qc.Capacity,
			threshold: int64(qc.WaitThreshold),
			acw:       qc.AfterCallWork,
			overflow:  qc.Overflow,
			members:   make(map[*call]struct{}),
		}
	}
	for _, qc := range cfg.Queues {
		if qc.Overflow.Kind == OverflowTransfer {
			if _, ok := e.queues[qc.Overflow.Target]; !ok {
				return nil, fmt.Errorf("callcenter: queue %q overflow target %q: %w", qc.Name, qc.Overflow.Target, ErrUnknownQueue)
			}
		}
	}
	for _, ac := range cfg.Agents {
		if ac.ID == "" {
			return nil, errors.New("callcenter: agent ID must not be empty")
		}
		if _, dup := e.agents[ac.ID]; dup {
			return nil, fmt.Errorf("callcenter: duplicate agent %q", ac.ID)
		}
		a := &agent{id: ac.ID, state: AgentIdle}
		for _, qn := range ac.Queues {
			q, ok := e.queues[qn]
			if !ok {
				return nil, fmt.Errorf("callcenter: agent %q serves %q: %w", ac.ID, qn, ErrUnknownQueue)
			}
			a.queues = append(a.queues, q)
			q.agents = append(q.agents, a)
		}
		e.agents[ac.ID] = a
	}
	e.buildComponents()
	return e, nil
}

// buildComponents 用并查集把“共享坐席”的队列与坐席连成组件，
// 每个组件一把锁；不共享坐席的队列分属不同组件，可并行分配。
func (e *Engine) buildComponents() {
	parent := map[*queue]*queue{}
	var find func(q *queue) *queue
	find = func(q *queue) *queue {
		if parent[q] != q {
			parent[q] = find(parent[q])
		}
		return parent[q]
	}
	for _, q := range e.queues {
		parent[q] = q
	}
	union := func(a, b *queue) {
		ra, rb := find(a), find(b)
		if ra != rb {
			parent[rb] = ra
		}
	}
	for _, a := range e.agents {
		for i := 1; i < len(a.queues); i++ {
			union(a.queues[0], a.queues[i])
		}
	}
	compOf := map[*queue]*component{}
	for _, q := range e.queues {
		root := find(q)
		c := compOf[root]
		if c == nil {
			c = &component{}
			compOf[root] = c
			e.comps = append(e.comps, c)
		}
		q.comp = c
		c.queues = append(c.queues, q)
	}
	for _, a := range e.agents {
		if len(a.queues) == 0 {
			// 不服务任何队列的坐席独占一个组件。
			c := &component{}
			e.comps = append(e.comps, c)
			a.comp = c
			c.agents = append(c.agents, a)
			continue
		}
		c := a.queues[0].comp
		a.comp = c
		c.agents = append(c.agents, a)
	}
}

// nowTime 返回引擎当前时间。
func (e *Engine) nowTime() int64 { return e.now.Load() }
