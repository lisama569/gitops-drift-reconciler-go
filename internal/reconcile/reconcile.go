// Package reconcile 实现期望状态收敛控制循环。
//
// 语义：
//   - 每次 Run 先基于实际状态快照计算计划，再按依赖顺序执行；
//   - 执行通过存储的乐观并发版本号推进，计划生成与执行之间
//     实际状态若被外部改动，写入会被拒绝（冲突）。调用方可通过
//     Options.DriftPolicy 选择处置方式：DriftAbort（默认，中止本次
//     执行并标记 Drift=true）或 DriftReplan（用最新实际状态重新
//     计算计划后继续收敛）；两种方式都不会把过期计划套到已变化的
//     资源上；
//   - 某一步失败时，执行停留在该资源及其下游，不再推进其余依赖链；
//   - 调用方可通过 RunContext 取消一次收敛，或为单步骤设置超时；
//     一旦本次执行被判定为失败，之后不会再产生本次执行的任何写入
//     （存储写入只发生在运行 goroutine 内、且写入前确认未取消）；
//   - 同一 Reconciler 的多个 Run 并发触发时在控制器层面串行化；
//     多个独立 Reconciler 实例可共享同一存储并发收敛，靠存储的
//     乐观并发版本号保证同一资源最多被成功下发一次，冲突方得到
//     明确的冲突错误而非静默覆盖。
package reconcile

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/lisama569/gitops-drift-reconciler-go/internal/decl"
	"github.com/lisama569/gitops-drift-reconciler-go/internal/plan"
	"github.com/lisama569/gitops-drift-reconciler-go/internal/store"
)

// FailureKind 区分失败原因。
type FailureKind string

const (
	FailValidation         FailureKind = "validation"           // 声明不合法
	FailDependencyNotReady FailureKind = "dependency_not_ready" // 上游失败导致未就绪
	FailConflict           FailureKind = "conflict"             // 执行冲突（版本竞争）
	FailTimeout            FailureKind = "timeout"              // 步骤执行超时
	FailDrift              FailureKind = "drift_detected"       // 计划与执行之间发生漂移
	FailCanceled           FailureKind = "canceled"             // 调用方取消本次收敛
	FailExecution          FailureKind = "execution"            // 其他执行错误
)

// StepError 携带可区分失败原因的错误。
type StepError struct {
	Kind   FailureKind
	Detail string
}

func (e *StepError) Error() string { return fmt.Sprintf("%s: %s", e.Kind, e.Detail) }

// DriftPolicy 决定“计划生成后、执行期间实际状态被外部改动”时的处置方式。
type DriftPolicy string

const (
	// DriftAbort 是默认策略：拒绝过期计划，中止本次执行，
	// Result.Drift=true，失败原因 drift_detected，由下一次 Run 重新收敛。
	DriftAbort DriftPolicy = "abort"
	// DriftReplan 用最新实际状态重新计算计划，接着往下收敛；
	// 已完成的步骤不会重复下发（新计划中不再包含它们）。
	DriftReplan DriftPolicy = "replan"
)

// Options 是单次收敛的可选参数；nil 等价于全部默认（与上一轮行为一致）。
type Options struct {
	DriftPolicy DriftPolicy // 漂移处置策略，空串等价于 DriftAbort
}

// maxReplans 限制单次 Run 内的重规划次数，防止持续争抢下无限循环。
const maxReplans = 32

// ReplanEvent 记录一次重规划（仅 DriftReplan 策略下产生）。
type ReplanEvent struct {
	AtKey   string     // 发生冲突、触发重规划的步骤
	OldBase uint64     // 旧计划的 baseVersion
	NewBase uint64     // 重新快照后的 baseVersion
	Reason  string     // 触发原因（冲突详情）
	Plan    *plan.Plan // 基于最新实际状态重新计算的计划
}

// StepResult 记录单个步骤的执行结果。
type StepResult struct {
	Key     string
	Action  plan.ChangeKind
	Failure FailureKind // 空串表示成功
	Detail  string
}

// Result 是一次收敛的结论。
type Result struct {
	Plan     *plan.Plan    // 本次执行的初始计划（含判定依据）
	Policy   DriftPolicy   // 本次使用的漂移处置策略
	Replans  []ReplanEvent // 执行期间发生的重规划（仅 DriftReplan）
	Applied  []StepResult  // 已成功下发的步骤
	Failed   *StepResult   // 失败的步骤（若有）
	Skipped  []StepResult  // 未推进的步骤及其原因
	Drift    bool          // 是否因执行前漂移而中止
	Canceled bool          // 是否因调用方取消而中止
}

// Converged 报告本次收敛是否完全成功。
func (r *Result) Converged() bool {
	return r.Failed == nil && len(r.Skipped) == 0 && !r.Drift
}

// String 以稳定格式输出结论，便于日志与测试比对。
func (r *Result) String() string {
	var b strings.Builder
	b.WriteString(r.Plan.String())
	fmt.Fprintf(&b, "driftPolicy=%s\n", r.Policy)
	for _, rp := range r.Replans {
		fmt.Fprintf(&b, "replan: at=%s base %d -> %d (%s)\n%s", rp.AtKey, rp.OldBase, rp.NewBase, rp.Reason, rp.Plan.String())
	}
	for _, a := range r.Applied {
		fmt.Fprintf(&b, "applied: %s %s\n", a.Action, a.Key)
	}
	if r.Failed != nil {
		fmt.Fprintf(&b, "failed: %s %s [%s] %s\n", r.Failed.Action, r.Failed.Key, r.Failed.Failure, r.Failed.Detail)
	}
	for _, s := range r.Skipped {
		fmt.Fprintf(&b, "skipped: %s %s [%s] %s\n", s.Action, s.Key, s.Failure, s.Detail)
	}
	fmt.Fprintf(&b, "converged=%v drift=%v canceled=%v\n", r.Converged(), r.Drift, r.Canceled)
	return b.String()
}

// StepHook 在步骤真正写入存储前调用，可用于注入失败（测试用）。
type StepHook func(step plan.Step) error

// Reconciler 是收敛控制器。
type Reconciler struct {
	store   *store.Store
	mu      sync.Mutex // 串行化并发触发的收敛循环
	timeout time.Duration

	// Hook 若非 nil，在每个步骤写入前调用；返回错误则该步骤失败。
	Hook StepHook
}

// New 创建收敛控制器。
func New(s *store.Store) *Reconciler {
	return &Reconciler{store: s, timeout: 5 * time.Second}
}

// SetStepTimeout 设置单步骤超时。
func (r *Reconciler) SetStepTimeout(d time.Duration) { r.timeout = d }

// Run 执行一次收敛循环（默认行为：不可取消、漂移即中止）。
// 声明不合法时整体拒绝，返回 error，不执行任何变更。
func (r *Reconciler) Run(d *decl.Declaration) (*Result, error) {
	return r.RunContext(context.Background(), d, nil)
}

// RunContext 执行一次收敛循环，支持调用方取消与漂移处置策略。
// opts 为 nil 时等价于默认行为（DriftAbort），与 Run 完全一致。
func (r *Reconciler) RunContext(ctx context.Context, d *decl.Declaration, opts *Options) (*Result, error) {
	if err := decl.Validate(d); err != nil {
		return nil, fmt.Errorf("reconcile: %w", &StepError{Kind: FailValidation, Detail: err.Error()})
	}
	policy := DriftAbort
	if opts != nil && opts.DriftPolicy != "" {
		policy = opts.DriftPolicy
	}
	r.mu.Lock()
	defer r.mu.Unlock()

	res := &Result{Policy: policy}
	downstream := transitiveDependents(d)

	p, version, err := r.computePlan(d)
	if err != nil {
		return nil, fmt.Errorf("reconcile: %w", &StepError{Kind: FailValidation, Detail: err.Error()})
	}
	res.Plan = p

	replans := 0
	for {
		failedIdx := -1
		var failErr error
		for i, step := range p.Steps {
			// 步骤之间检查取消：被中断的资源停在原地，不再推进。
			if ctx.Err() != nil {
				failedIdx, failErr = i, &StepError{Kind: FailCanceled, Detail: "调用方取消了本次收敛"}
				break
			}
			if err := r.executeStep(ctx, step, &version); err != nil {
				failedIdx, failErr = i, err
				break
			}
			res.Applied = append(res.Applied, StepResult{Key: step.Resource.Key(), Action: step.Kind})
		}
		if failedIdx < 0 {
			return res, nil
		}

		step := p.Steps[failedIdx]
		kind := classify(failErr)
		if kind == FailConflict && policy == DriftReplan && replans < maxReplans {
			// 重规划：用最新实际状态重新计算计划，接着往下收敛。
			// 已完成的步骤不会出现在新计划中，不会重复下发。
			np, nver, err := r.computePlan(d)
			if err != nil {
				return nil, fmt.Errorf("reconcile: %w", &StepError{Kind: FailValidation, Detail: err.Error()})
			}
			replans++
			res.Replans = append(res.Replans, ReplanEvent{
				AtKey:   step.Resource.Key(),
				OldBase: version,
				NewBase: nver,
				Reason:  "执行期间实际状态被修改，按 replan 策略重新计算计划: " + failErr.Error(),
				Plan:    np,
			})
			p, version = np, nver
			continue
		}

		sr := StepResult{Key: step.Resource.Key(), Action: step.Kind, Failure: kind, Detail: failErr.Error()}
		switch kind {
		case FailConflict:
			// 既定策略（默认）：计划已过期，拒绝继续套用，标记漂移。
			res.Drift = true
			sr.Failure = FailDrift
			sr.Detail = "计划生成后实际状态已被修改，拒绝套用过期计划: " + failErr.Error()
		case FailCanceled:
			res.Canceled = true
		}
		res.Failed = &sr
		// 停留在该资源及其下游：其余步骤不再推进。
		for _, rest := range p.Steps[failedIdx+1:] {
			skip := StepResult{Key: rest.Resource.Key(), Action: rest.Kind, Failure: FailExecution, Detail: "前序步骤失败，未推进"}
			if downstream[step.Resource.Key()][rest.Resource.Key()] {
				skip.Failure = FailDependencyNotReady
				skip.Detail = fmt.Sprintf("上游 %s 未就绪", step.Resource.Key())
			}
			res.Skipped = append(res.Skipped, skip)
		}
		return res, nil
	}
}

// computePlan 基于当前实际状态快照计算计划，返回计划及其 baseVersion。
func (r *Reconciler) computePlan(d *decl.Declaration) (*plan.Plan, uint64, error) {
	records, base := r.store.Snapshot()
	actual := make(map[string]decl.Resource, len(records))
	for k, rec := range records {
		actual[k] = rec.Resource
	}
	p, err := plan.Compute(d, actual, base)
	if err != nil {
		return nil, base, err
	}
	return p, base, nil
}

// executeStep 执行单个步骤并推进版本号。
// 可能阻塞的 Hook（模拟外部系统耗时）在独立 goroutine 中执行；
// 存储写入只发生在当前 goroutine、且写入前确认未取消未超时，
// 因此一旦本次执行被判定为失败，之后不会再产生本次执行的任何写入。
func (r *Reconciler) executeStep(ctx context.Context, step plan.Step, version *uint64) error {
	// 在当前 goroutine（持有控制器锁）内先取走 Hook 与超时配置，
	// 避免超时返回后后台 goroutine 再读取字段产生数据竞争。
	hook := r.Hook
	timeout := r.timeout
	stepCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	done := make(chan error, 1)
	go func() {
		if hook != nil {
			done <- hook(step)
			return
		}
		done <- nil
	}()
	select {
	case err := <-done:
		if err != nil {
			return err
		}
	case <-stepCtx.Done():
		if ctx.Err() != nil {
			return &StepError{Kind: FailCanceled, Detail: fmt.Sprintf("步骤 %s 执行期间被调用方取消", step.Resource.Key())}
		}
		return &StepError{Kind: FailTimeout, Detail: fmt.Sprintf("步骤 %s 超过 %s 未完成", step.Resource.Key(), timeout)}
	}
	// 写入前再次确认未取消：取消/超时判定后不得再产生写入。
	if ctx.Err() != nil {
		return &StepError{Kind: FailCanceled, Detail: fmt.Sprintf("步骤 %s 写入前被取消", step.Resource.Key())}
	}
	var v uint64
	var err error
	switch step.Kind {
	case plan.Create, plan.Update:
		v, err = r.store.Apply(step.Resource, *version)
	case plan.Delete:
		v, err = r.store.Delete(step.Resource.Key(), *version)
	}
	if err != nil {
		return err
	}
	*version = v
	return nil
}

// classify 将错误归类为可区分的失败原因。
func classify(err error) FailureKind {
	var se *StepError
	if errors.As(err, &se) {
		return se.Kind
	}
	if errors.Is(err, store.ErrConflict) {
		return FailConflict
	}
	if errors.Is(err, context.Canceled) {
		return FailCanceled
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return FailTimeout
	}
	return FailExecution
}

// transitiveDependents 计算每个资源的全部传递下游（依赖它的资源）。
func transitiveDependents(d *decl.Declaration) map[string]map[string]bool {
	children := map[string][]string{}
	for _, r := range d.Resources {
		for _, dep := range r.DependsOn {
			children[dep] = append(children[dep], r.Key())
		}
	}
	out := map[string]map[string]bool{}
	for _, r := range d.Resources {
		key := r.Key()
		seen := map[string]bool{}
		stack := append([]string{}, children[key]...)
		for len(stack) > 0 {
			c := stack[len(stack)-1]
			stack = stack[:len(stack)-1]
			if seen[c] {
				continue
			}
			seen[c] = true
			stack = append(stack, children[c]...)
		}
		out[key] = seen
	}
	return out
}

// SortedKeys 辅助函数：返回排序后的 key 列表（供日志使用）。
func SortedKeys(m map[string]bool) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}
