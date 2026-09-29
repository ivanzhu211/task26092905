# task26092905

呼叫中心来电排队与坐席分配引擎（Go 库，纯内存，无持久化）。

## 用法

```go
e := callcenter.New(callcenter.Config{
    Queues: []callcenter.QueueConfig{{
        Name:      "support",
        Capacity:  100,               // 上限 150
        Threshold: 60 * time.Second,  // 等待超时即溢出
        Wrapup:    10 * time.Second,  // 话后时长
        Overflow:  callcenter.OverflowRule{
            Action: callcenter.OverflowTransfer, // 或 OverflowVoicemail / OverflowDisconnect
            Target: "backup",
        },
    }},
    Agents: []callcenter.AgentConfig{{ID: "a1", Queues: []string{"support", "backup"}}},
})

e.Enqueue("call-1", "support", 5)   // 优先级 1-10，大者优先
e.Abandon("call-1")                 // 等待中挂断
e.EndCall("call-1")                 // 坐席挂机，进入话后
e.SetAgentState("a1", callcenter.AgentOnBreak) // 通话/话后期间的申请在话后结束时生效
e.Advance(time.Second)              // 时间只由 Advance 推进
info := e.CallStatus("call-1")      // 查询
```

## 设计要点

- 每队列一把锁、每坐席一把锁，锁序固定为 坐席 → 队列 → 来电；坐席查找用 `TryLock` 避免循环等待。不共享坐席的队列可并行分配。
- 分配在触发它的调用内同步完成（入队、坐席变闲、话后结束、溢出转入），无后台扫描。
- `Advance` 按时间先后逐个处理到期事件，同一时刻话后结束先于溢出；容量溢出链带环路检测，绕回即断开。

## 测试

```sh
go test -race ./...
```

含确定性测试（同一 `Advance` 内话后结束与溢出阈值同时到期时来电被接通）与压力测试
（8 队列、30 坐席、4000 来电、多 goroutine 并发 + 运行期不变量监控）。
