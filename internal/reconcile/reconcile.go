// Package reconcile 实现期望状态收敛控制循环。
//
// 语义：
//   - 每次 Run 先基于实际状态快照计算计划，再按依赖顺序执行；
//   - 执行通过存储的乐观并发版本号推进，计划生成与执行之间
//     实际状态若被外部改动，写入会被拒绝，本次执行中止并标记
//     Drift=true（既定策略：拒绝过期计划，由下一次 Run 重新计算）；
//   - 某一步失败时，执行停留在该资源及其下游，不再推进其余依赖链；
//   - 多个 Run 并发触发时在控制器层面串行化，同一资源不会被重复下发。
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
	FailCanceled           FailureKind = "canceled"             // 调用方主动取消
	FailExecution          FailureKind = "execution"            // 其他执行错误
)

// DriftPolicy 决定“计划生成后实际状态被外部改动”时的处置方式。
type DriftPolicy string

const (
	// DriftAbort 既定策略（默认）：拒绝过期计划，本次执行中止并标记漂移，
	// 由下一次 Run 重新计算。与上一轮的既有行为完全一致。
	DriftAbort DriftPolicy = "abort"
	// DriftReplan 拿最新实际状态重新计算计划，接着往下收敛。
	// 已完成的步骤不会重复下发（重算后的计划中不再出现）。
	DriftReplan DriftPolicy = "replan"
)

// DefaultMaxReplans 是 DriftReplan 策略下允许的最大重算次数上限，
// 防止外部持续改动时无限重算。
const DefaultMaxReplans = 16

// Options 是一次收敛的可选参数。零值等价于上一轮的默认行为。
type Options struct {
	// DriftPolicy 漂移处置方式；零值（空串）按 DriftAbort 处理。
	DriftPolicy DriftPolicy
	// MaxReplans DriftReplan 下的最大重算次数；<=0 时使用 DefaultMaxReplans。
	MaxReplans int
}

func (o Options) policy() DriftPolicy {
	if o.DriftPolicy == "" {
		return DriftAbort
	}
	return o.DriftPolicy
}

func (o Options) maxReplans() int {
	if o.MaxReplans <= 0 {
		return DefaultMaxReplans
	}
	return o.MaxReplans
}

// StepError 携带可区分失败原因的错误。
type StepError struct {
	Kind   FailureKind
	Detail string
}

func (e *StepError) Error() string { return fmt.Sprintf("%s: %s", e.Kind, e.Detail) }

// StepResult 记录单个步骤的执行结果。
type StepResult struct {
	Key     string
	Action  plan.ChangeKind
	Failure FailureKind // 空串表示成功
	Detail  string
}

// Result 是一次收敛的结论。
type Result struct {
	Plan    *plan.Plan   // 本次执行的计划（含判定依据）；重算时为最后一份计划
	Applied []StepResult // 已成功下发的步骤
	Failed  *StepResult  // 失败的步骤（若有）
	Skipped []StepResult // 未推进的步骤及其原因
	Drift   bool         // 是否因执行前漂移而中止

	// Policy 是本次实际采用的漂移处置方式（abort/replan），报告中可据此
	// 看出这次用的是哪种方式。
	Policy DriftPolicy
	// Replans 是 DriftReplan 策略下实际发生的重算次数。
	Replans int
	// DriftLog 记录每次漂移/重算的判定依据（为什么这么判）。
	DriftLog []string
}

// Converged 报告本次收敛是否完全成功。
func (r *Result) Converged() bool {
	return r.Failed == nil && len(r.Skipped) == 0 && !r.Drift
}

// String 以稳定格式输出结论，便于日志与测试比对。
func (r *Result) String() string {
	var b strings.Builder
	fmt.Fprintf(&b, "driftPolicy=%s replans=%d\n", r.Policy, r.Replans)
	for _, d := range r.DriftLog {
		fmt.Fprintf(&b, "drift: %s\n", d)
	}
	b.WriteString(r.Plan.String())
	for _, a := range r.Applied {
		fmt.Fprintf(&b, "applied: %s %s\n", a.Action, a.Key)
	}
	if r.Failed != nil {
		fmt.Fprintf(&b, "failed: %s %s [%s] %s\n", r.Failed.Action, r.Failed.Key, r.Failed.Failure, r.Failed.Detail)
	}
	for _, s := range r.Skipped {
		fmt.Fprintf(&b, "skipped: %s %s [%s] %s\n", s.Action, s.Key, s.Failure, s.Detail)
	}
	fmt.Fprintf(&b, "converged=%v drift=%v\n", r.Converged(), r.Drift)
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

// Run 执行一次收敛循环，使用默认选项（漂移即中止、不可取消），
// 与上一轮的既有调用含义完全一致。
// 声明不合法时整体拒绝，返回 error，不执行任何变更。
func (r *Reconciler) Run(d *decl.Declaration) (*Result, error) {
	return r.RunWithOptions(context.Background(), d, Options{})
}

// RunWithOptions 执行一次收敛循环：
//   - ctx 取消时，当前资源及其下游停在原地，已完成的写入保持可观察，
//     本次执行被判定为失败后不会再产生任何属于本次的写入；
//   - 计划与执行之间发生漂移时，按 opts.DriftPolicy 处置（见 DriftPolicy）。
func (r *Reconciler) RunWithOptions(ctx context.Context, d *decl.Declaration, opts Options) (*Result, error) {
	if err := decl.Validate(d); err != nil {
		return nil, fmt.Errorf("reconcile: %w", &StepError{Kind: FailValidation, Detail: err.Error()})
	}
	r.mu.Lock()
	defer r.mu.Unlock()

	res := &Result{Policy: opts.policy()}
	downstream := transitiveDependents(d)

	var p *plan.Plan
	var version uint64
	replan := func() error {
		records, base := r.store.Snapshot()
		actual := make(map[string]decl.Resource, len(records))
		for k, rec := range records {
			actual[k] = rec.Resource
		}
		np, err := plan.Compute(d, actual, base)
		if err != nil {
			return err
		}
		p, version = np, base
		res.Plan = p
		return nil
	}
	if err := replan(); err != nil {
		return nil, fmt.Errorf("reconcile: %w", &StepError{Kind: FailValidation, Detail: err.Error()})
	}

	replans := 0
	for !p.Empty() {
		replanned := false
		for i, step := range p.Steps {
			// 取消检查：进入每一步之前确认调用方未取消。
			if err := ctx.Err(); err != nil {
				sr := StepResult{Key: step.Resource.Key(), Action: step.Kind,
					Failure: FailCanceled, Detail: "调用方取消，停止推进: " + err.Error()}
				res.Failed = &sr
				skipRemaining(res, downstream, p, i, step.Resource.Key())
				return res, nil
			}
			sr := StepResult{Key: step.Resource.Key(), Action: step.Kind}
			err := r.executeStep(ctx, step, &version)
			if err == nil {
				res.Applied = append(res.Applied, sr)
				continue
			}
			kind := classify(err)
			if kind == FailConflict && opts.policy() == DriftReplan {
				if replans < opts.maxReplans() {
					replans++
					res.Replans = replans
					res.DriftLog = append(res.DriftLog, fmt.Sprintf(
						"步骤 %s 写入冲突（%v）；按 replan 策略基于最新实际状态重算计划（第 %d 次）",
						step.Resource.Key(), err, replans))
					if rerr := replan(); rerr != nil {
						return nil, fmt.Errorf("reconcile: %w", &StepError{Kind: FailValidation, Detail: rerr.Error()})
					}
					replanned = true
					break
				}
				res.DriftLog = append(res.DriftLog, fmt.Sprintf(
					"步骤 %s 写入冲突且重算次数已达上限 %d，按漂移中止",
					step.Resource.Key(), opts.maxReplans()))
			}
			if kind == FailConflict {
				// 既定策略：计划已过期，拒绝继续套用，标记漂移。
				if opts.policy() == DriftAbort {
					res.DriftLog = append(res.DriftLog, fmt.Sprintf(
						"步骤 %s 写入冲突（%v）；按 abort 策略拒绝过期计划，中止本次执行",
						step.Resource.Key(), err))
				}
				res.Drift = true
				sr.Failure = FailDrift
				sr.Detail = "计划生成后实际状态已被修改，拒绝套用过期计划: " + err.Error()
			} else {
				sr.Failure = kind
				sr.Detail = err.Error()
			}
			res.Failed = &sr
			// 停留在该资源及其下游：其余步骤不再推进。
			skipRemaining(res, downstream, p, i, step.Resource.Key())
			return res, nil
		}
		if !replanned {
			// 当前计划全部步骤执行完毕。
			return res, nil
		}
	}
	return res, nil
}

// skipRemaining 将计划中下标 i 之后的所有步骤标记为未推进；
// 属于 failedKey 下游的步骤标记为 dependency_not_ready。
func skipRemaining(res *Result, downstream map[string]map[string]bool, p *plan.Plan, i int, failedKey string) {
	for _, rest := range p.Steps[i+1:] {
		skip := StepResult{Key: rest.Resource.Key(), Action: rest.Kind, Failure: FailExecution, Detail: "前序步骤失败，未推进"}
		if downstream[failedKey][rest.Resource.Key()] {
			skip.Failure = FailDependencyNotReady
			skip.Detail = fmt.Sprintf("上游 %s 未就绪", failedKey)
		}
		res.Skipped = append(res.Skipped, skip)
	}
}

// executeStep 执行单个步骤并推进版本号。
// 存储写入只发生在当前 goroutine 内、且写入前再次确认未取消：
// 一旦本次执行被判定为失败/取消/超时，之后不会再出现这次执行带来的写入，
// 即结论必然等于最终实际状态。
func (r *Reconciler) executeStep(ctx context.Context, step plan.Step, version *uint64) error {
	if err := ctx.Err(); err != nil {
		return &StepError{Kind: FailCanceled, Detail: fmt.Sprintf("步骤 %s 在写入前被取消", step.Resource.Key())}
	}
	if r.Hook != nil {
		if err := r.runHook(ctx, step); err != nil {
			return err
		}
	}
	// Hook 完成后、真正写入前再次确认未取消/未超时。
	if err := ctx.Err(); err != nil {
		return &StepError{Kind: FailCanceled, Detail: fmt.Sprintf("步骤 %s 在写入前被取消", step.Resource.Key())}
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

// runHook 在带超时的窗口内执行 Hook。
// 超时时直接返回错误，Hook goroutine 本身不触碰存储，
// 因此不会产生“报告失败后资源又被改掉”的迟到写入。
func (r *Reconciler) runHook(ctx context.Context, step plan.Step) error {
	hookCtx, cancel := context.WithTimeout(ctx, r.timeout)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- r.Hook(step) }()
	select {
	case err := <-done:
		return err
	case <-hookCtx.Done():
		if ctx.Err() != nil {
			return &StepError{Kind: FailCanceled, Detail: fmt.Sprintf("步骤 %s 执行期间被取消", step.Resource.Key())}
		}
		return &StepError{Kind: FailTimeout, Detail: fmt.Sprintf("步骤 %s 超过 %s 未完成", step.Resource.Key(), r.timeout)}
	}
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
