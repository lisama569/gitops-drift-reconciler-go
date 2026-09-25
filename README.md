# gitops-drift-reconciler-go

一套**纯本地运行**的期望状态收敛控制循环（GitOps drift reconciler）：
用内存存储模拟“期望声明”与“实际资源状态”，实现差异计算、按依赖顺序的
收敛执行、幂等重试、并发触发与执行前漂移检测。不依赖任何外部服务、集群或凭据。

## 结构

| 包 | 职责 |
|---|---|
| `internal/spec` | 期望声明（JSON）的解析与校验，非法声明整体拒绝 |
| `internal/plan` | 期望 vs 实际的差异计算，产出确定顺序的变更计划 |
| `internal/store` | 并发安全的内存资源存储，版本化 + CAS 写 |
| `internal/reconcile` | 控制循环：快照 → 计划 → 执行 → 漂移检测与重试 |
| `cmd/reconciler` | 本地演示入口 |

## 期望声明格式

```json
{
  "resources": [
    {"name": "namespace", "kind": "Namespace", "spec": {"tier": "backend"}},
    {"name": "config", "kind": "ConfigMap", "dependsOn": ["namespace"], "spec": {"logLevel": "info"}}
  ]
}
```

**校验规则**（任一不满足即整体拒绝，不会按部分内容继续执行）：
至少一个资源；`name`/`kind` 非空且 `name` 唯一（重名即自相矛盾）；
`dependsOn` 必须引用同一份声明中存在的资源（悬空依赖拒绝）；
依赖图无环、无自依赖；不允许未知字段。

## 计划与顺序语义

`plan.Diff(desired, snapshot, managedBy)` 对同一 `(desired, snapshot)` 组合
必然得到**稳定且可复现**的计划：

- **变更分类可区分**：
  - `create` — 资源缺失（实际状态中不存在）；
  - `update` — 字段漂移（`kind` 或 `spec` 与期望不一致）；
  - `delete` — 多余资源（由本控制器 `managedBy` 管理但不在期望声明中；
    他人管理的资源绝不触碰）；
  - `noop` — 已一致。
- **执行顺序**：对期望资源做拓扑排序（被依赖者在前），同层按名称字典序
  打破平局（Kahn 算法 + 有序就绪集）；`delete` 排在所有 create/update 之后，
  多余资源之间按名称字典序。
- 每个变更项记录 `expectedVersion`（计划生成时观察到的实际版本），
  供执行期 CAS 校验。

## 收敛执行语义

- **依赖顺序**：严格按计划顺序逐个执行。
- **幂等**：存储层 `Apply` 在内容一致时不产生任何变更（版本号、代际均不变）；
  已收敛状态下重复执行得到全 `noop` 的空计划，无额外副作用。
- **失败停驻**：任一资源失败即整体停驻——该资源标记为 `failed`，
  其传递下游标记为 `skipped` + `dependency-not-ready`，其余未执行项标记为
  `skipped`；已完成资源保持 `applied`/`noop` 状态可观察。
- **重试收敛**：修复故障后再次 `Reconcile`，基于最新快照重新计划，
  只补做未完成的部分，最终收敛到与一次性成功相同的结论。

### 失败分类（可区分）

| Reason | 含义 |
|---|---|
| `validation` | 声明非法或执行期校验（如准入）失败 |
| `dependency-not-ready` | 上游依赖失败/未就绪，本资源被跳过 |
| `conflict` | 执行冲突（注入的冲突类错误） |
| `timeout` | 上下文截止/取消 |
| `stale-plan` | 执行前漂移：计划生成后实际状态已被改动 |

错误以 `*reconcile.Failure` 承载，可用 `errors.As` 提取分类。

## 漂移与并发处理规则

- **执行前漂移检测**：每个变更以 `expectedVersion` 做 CAS 下发。
  若计划生成与执行之间实际状态被外部改动，版本必然不匹配
  （`ErrVersionConflict`/`ErrAlreadyExists`/`ErrNotFound`），
  本次执行立即停驻并判定为 `stale-plan`，**过期计划不会被套用**。
- **漂移策略**：
  - `Recompute`（默认）：丢弃旧计划，基于最新快照重新计算并重试，
    直到收敛或超过 `maxAttempts`；
  - `Reject`：直接拒绝，返回 `stale-plan` 错误，外部改动保持原样。
- **并发触发**：同一 `Reconciler` 实例上的 `Reconcile` 调用被互斥串行化；
  跨实例并发由存储层 CAS 兜底——同一资源的并发创建只有一个赢家，
  其余触发漂移检测并重算，因此不会重复下发，也不会出现只生效一半的状态
  （每次 `Apply`/`Delete` 在存储锁内原子完成）。

## 本地验证

```bash
# 全部单元测试（含竞态检测）
go test ./... -race -count=1

# 查看测试日志中的变更计划、依赖顺序、状态快照与判定依据
go test ./internal/reconcile/ -v

# 运行演示：对 examples/desired.json 连续收敛两次（第二次应全部 noop）
go run ./cmd/reconciler examples/desired.json
```

测试覆盖场景：

- `TestConvergeThenIdempotentRerun` — 重复执行幂等（代际不变证明无副作用）；
- `TestPartialFailureThenRetryConverges` — 部分失败后停驻、失败原因可区分、
  重试收敛到同一结论；
- `TestConcurrentReconcileSameConclusion` — 8 个控制器并发收敛同一资源集，
  存储代际恰好为 3 证明每个资源只被实际变更一次；
- `TestPreExecutionDriftRecompute` / `TestPreExecutionDriftReject` —
  计划生成后外部改动被 CAS 检测，分别按重算/拒绝策略处理；
- `TestTimeoutClassification`、`TestInvalidDesiredRejectedWithoutSideEffects`、
  `TestExtraResourceDeleted` — 超时分类、非法声明零副作用、多余资源回收。
