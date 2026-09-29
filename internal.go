package callcenter

import (
	"container/heap"
	"sync"
	"sync/atomic"
	"time"
)

// call 是一通来电的内部表示。
// state/queue/agentID 为原子字段，可在任意锁外读取；
// 其余字段只在“当前所在队列的组件锁”内访问。
type call struct {
	id          string
	priority    int
	firstArrive int64 // 首次进入系统的时间（纳秒刻度），转移时保留
	state       atomic.Int32
	queue       atomic.Pointer[queue]
	agentID     atomic.Value // string，接通后有效

	entryTime int64      // 进入当前队列的时间
	gen       uint64     // 每次入队递增，用于识别过期事件
	entry     *callEntry // 在当前队列堆中的位置
	connQueue *queue     // 接通时所在队列
}

func (c *call) st() CallState { return CallState(c.state.Load()) }

// callEntry 是队列等待堆中的一项。
type callEntry struct {
	c   *call
	seq uint64 // 入队序号，同级同刻的确定性次序
	idx int
}

// callHeap 是按（优先级降序, 首次到达升序, 序号升序）排序的最小堆。
type callHeap []*callEntry

func (h callHeap) Len() int { return len(h) }
func (h callHeap) Less(i, j int) bool {
	a, b := h[i], h[j]
	if a.c.priority != b.c.priority {
		return a.c.priority > b.c.priority
	}
	if a.c.firstArrive != b.c.firstArrive {
		return a.c.firstArrive < b.c.firstArrive
	}
	return a.seq < b.seq
}
func (h callHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
	h[i].idx = i
	h[j].idx = j
}
func (h *callHeap) Push(x any) {
	e := x.(*callEntry)
	e.idx = len(*h)
	*h = append(*h, e)
}
func (h *callHeap) Pop() any {
	old := *h
	n := len(old)
	e := old[n-1]
	old[n-1] = nil
	*h = old[:n-1]
	return e
}

// queue 是一个等待队列。字段只在 comp.mu 内访问。
type queue struct {
	name      string
	capacity  int
	threshold int64
	acw       time.Duration
	overflow  OverflowRule

	comp   *component
	agents []*agent // 可服务本队列的坐席

	waiting int
	members map[*call]struct{}
	heap    callHeap
}

// agent 是一个坐席。字段只在 comp.mu 内访问。
type agent struct {
	id     string
	comp   *component
	queues []*queue

	state     AgentState
	idleSince int64
	cur       *call
	acwEnd    int64
	acwGen    uint64
	pending   *AgentState // 通话/话后期间申请的状态
}

// eventKind 区分定时事件种类；同刻事件按种类排序，话后结束先于溢出。
type eventKind int

const (
	evACWEnd eventKind = iota
	evOverflow
)

// event 是一个定时事件。
type event struct {
	at   int64
	kind eventKind
	seq  uint64

	ag     *agent // evACWEnd
	acwGen uint64

	cl      *call // evOverflow
	callQ   *queue
	callGen uint64
}

// eventHeap 按（时间升序, 种类升序, 序号升序）排序。
type eventHeap []*event

func (h eventHeap) Len() int { return len(h) }
func (h eventHeap) Less(i, j int) bool {
	a, b := h[i], h[j]
	if a.at != b.at {
		return a.at < b.at
	}
	if a.kind != b.kind {
		return a.kind < b.kind
	}
	return a.seq < b.seq
}
func (h eventHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }
func (h *eventHeap) Push(x any)   { *h = append(*h, x.(*event)) }
func (h *eventHeap) Pop() any {
	old := *h
	n := len(old)
	e := old[n-1]
	old[n-1] = nil
	*h = old[:n-1]
	return e
}

// component 是通过“共享坐席”连通的一组队列与坐席，
// 是分配决策的并行粒度：不同组件之间无需互斥。
type component struct {
	mu     sync.Mutex
	queues []*queue
	agents []*agent
	events eventHeap
}

// pushEvent 在组件锁内向事件堆加入事件。
func (c *component) pushEvent(ev *event) { heap.Push(&c.events, ev) }
