# gitops-drift-reconciler-go

一套**本地可运行**的期望状态收敛控制循环。用本地内存存储模拟期望声明与实际资源状态，
使差异计算、收敛执行以及并发触发下的结论一致、可解释且可复现。
支持**多个彼此独立的收敛执行者**同时工作在同一个存储上，并允许调用方选择漂移处置方式、
主动取消或为单资源执行设置超时。整个验证过程不依赖任何外部服务、集群或凭据。

## 结构

```
internal/decl       期望声明的解析、校验与拓扑排序
internal/store      本地实际状态存储（乐观并发版本控制 + 原子创建）
internal/plan       差异计算与有序变更计划
internal/reconcile  收敛控制循环（执行、失败分类、漂移处置、取消/超时、并发串行化）
cmd/reconciler      本地演示入口（单实例幂等 + 多实例并发）
```

## 计划与顺序语义

- **差异分类**：每个资源被判定为 `create`（实际缺失）、`update`（字段漂移，逐字段给出
  `旧值 -> 新值`）、`delete`（多余资源，期望声明中不存在）之一；无差异则不产生步骤。
- **执行顺序**：`create`/`update` 按依赖拓扑序排列（依赖在前），同层资源按 `kind/name`
  字典序；`delete` 一律排在最后，按 key 字典序。
- **可复现性**：同一份期望声明 + 同一份实际状态快照，必然得到完全相同的计划与顺序
  （所有迭代均先排序，无 map 遍历随机性）。实例数量、资源集规模、依赖链长度
  都不影响这一性质。
- **整体拒绝**：声明不合法（缺 kind/name、重复声明、依赖不存在的资源、依赖自身、依赖环）
  时，`Run` 直接返回 `validation` 错误，**不执行任何变更**，不会按部分内容继续。

## 多实例并发语义

多个 `Reconciler` 实例（各自 `reconcile.New(sharedStore)`）可以同时处理同一份期望声明
与同一份实际状态：

- **存储层原子性**：存储的每次写入都是原子操作，并基于全局单调递增版本号做乐观并发
  控制。多个执行者同时写入时，版本不匹配的写入被明确拒绝（`store.ErrConflict`），
  不会出现只生效一半的交错状态。`store.CreateIfAbsent` 进一步保证：多个执行者同时
  创建同一资源时**只有一个成功**，其余全部收到 `store.ErrAlreadyExists`，
  不会静默覆盖，也不会各自都以为自己成功。
- **恰好一次下发**：同一资源在一次并发风暴里最多被**成功**下发一次。其他实例的重复
  尝试会被存储冲突拒绝；采用 `replan` 策略的实例随后基于最新快照重算计划，
  发现该资源已收敛，不再重复下发。测试通过存储全局版本号严格断言：
  成功写入总数恰好等于资源数。
- **结论收敛**：所有实例最终都收敛到同一个结论——实际状态恰好等于期望集合，
  风暴过后任意实例再次执行都得到相同的空计划。
- 单个 `Reconciler` 实例内部的并发 `Run` 依旧在控制器层面串行化（互斥锁），
  与上一轮行为一致。

## 漂移处置：两种方式怎么选

从算出计划到真正执行之间，实际状态可能被外部改动。调用方通过
`RunWithOptions(ctx, d, Options{DriftPolicy: ...})` 选择处置方式：

| 策略 | 行为 | 适用场景 |
|---|---|---|
| `abort`（默认） | 拒绝过期计划：本次执行立即中止，`Result.Drift=true`、失败原因 `drift_detected`，由下一次 `Run` 重新计算 | 希望先暴露漂移、由外层节奏控制重试；与上一轮既有行为完全一致 |
| `replan` | 拿最新实际状态重新计算计划，接着往下收敛；已完成的步骤不会重复下发；重算次数有上限（默认 16，可配 `MaxReplans`），超限按漂移中止 | 多实例并发、外部变更频繁，希望单次调用内自愈收敛 |

两种方式都保证**不会把过期计划套到变化后的资源上**：`abort` 直接拒绝；
`replan` 丢弃旧计划、基于最新快照重算后再执行。报告中通过
`Result.Policy`（本次采用的方式）、`Result.Replans`（重算次数）与
`Result.DriftLog`（每次漂移/重算的判定依据）可以看出这次用的是哪种方式、为什么这么判。

默认行为不变：`Run(d)` 等价于 `RunWithOptions(context.Background(), d, Options{})`，
即 `abort` 策略，既有调用的含义不受影响。

## 取消与超时的终态保证

- **主动取消**：`RunWithOptions` 接受 `context.Context`。取消后当前资源及其下游
  停在原地（下游标记 `dependency_not_ready`），失败原因 `canceled`。
- **单资源超时**：`SetStepTimeout` 限制单个步骤的执行时长，超时失败原因 `timeout`。
- **终态一致性**：不管是取消还是超时，一旦本次执行被判定为失败，之后就**不会再出现
  这次执行带来的写入**——存储写入只发生在主流程中、且写入前再次确认未取消，
  被遗弃的 Hook goroutine 不触碰存储。因此结论必然等于最终实际状态，
  不存在“报告说失败、过一会儿资源又被改掉了”的窗口。
- 已完成的部分保留在存储中，可通过 `store.Keys()` / `Snapshot()` 观察、解释；
  重试时重新计算计划，只补齐剩余变更，最终收敛到与一次性成功**相同的结论**。

## 失败分类

| 类别 | 含义 |
|---|---|
| `validation` | 声明不合法 / 互相矛盾 / 依赖不存在，整体拒绝 |
| `dependency_not_ready` | 上游步骤失败/中断，下游未就绪 |
| `conflict` | 写入时版本冲突（执行冲突） |
| `timeout` | 单步骤执行超过时限 |
| `drift_detected` | 计划生成与执行之间实际状态被外部改动，过期计划被拒绝 |
| `canceled` | 调用方通过 context 主动取消 |
| `execution` | 其他执行错误 |

## 本地验证

```bash
# 全部单元测试（含竞态检测）
go test -race ./...

# 查看测试日志中的变更计划、执行顺序、中间/最终状态与判定依据
go test ./internal/reconcile/ -v

# 并发用例反复执行，验证稳定性
go test -race ./internal/reconcile/ -count=20

# 本地演示：单实例幂等 + 4 个独立执行者并发收敛
go run ./cmd/reconciler
```

测试覆盖场景：

- `TestRepeatedRunIdempotent`：重复执行幂等，依赖顺序正确；
- `TestPartialFailureThenRetry`：部分失败后停留、下游标记 `dependency_not_ready`、
  重试收敛到同一结论且每个资源只下发一次；
- `TestConcurrentRuns`：单实例内 8 个并发循环串行化，每个资源全程只下发一次；
- `TestMultiInstanceConcurrentReconcile`：6 个独立执行者并发处理 12 个资源
  （8 级依赖链 + 4 个独立分支），计划稳定可复现、冲突被明确报出并重算、
  成功写入总数恰好等于资源数、所有实例收敛到同一结论；
- `TestDriftBetweenPlanAndApply` / `TestDriftPolicyAbortExplicit`：默认及显式
  `abort` 策略下过期计划被拒绝（`drift_detected`），报告标明策略与判定依据；
- `TestDriftPolicyReplan`：`replan` 策略下基于最新实际状态重算计划继续收敛，
  被篡改的资源按新计划恢复、多余资源被删除，最终状态等于期望；
- `TestCancelStopsAndNoLateWrites`：调用方取消后下游停在原地、
  判定失败后无迟到写入、重试收敛到同一结论；
- `TestTimeoutNoLateWrites` / `TestFailureKindsDistinguishable`：超时判定后
  无迟到写入，失败原因可区分，非法声明不产生任何变更；
- `TestConcurrentCreateIfAbsent`（store）：16 路并发创建同一资源，
  恰好 1 个成功、其余全部收到 `ErrAlreadyExists`，已存在资源不被静默覆盖。
