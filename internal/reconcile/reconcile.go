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
	FailExecution          FailureKind = "execution"            // 其他执行错误
)

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
	Plan    *plan.Plan   // 本次执行的计划（含判定依据）
	Applied []StepResult // 已成功下发的步骤
	Failed  *StepResult  // 失败的步骤（若有）
	Skipped []StepResult // 未推进的步骤及其原因
	Drift   bool         // 是否因执行前漂移而中止
}

// Converged 报告本次收敛是否完全成功。
func (r *Result) Converged() bool {
	return r.Failed == nil && len(r.Skipped) == 0 && !r.Drift
}

// String 以稳定格式输出结论，便于日志与测试比对。
func (r *Result) String() string {
	var b strings.Builder
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

// Run 执行一次收敛循环。
// 声明不合法时整体拒绝，返回 error，不执行任何变更。
func (r *Reconciler) Run(d *decl.Declaration) (*Result, error) {
	if err := decl.Validate(d); err != nil {
		return nil, fmt.Errorf("reconcile: %w", &StepError{Kind: FailValidation, Detail: err.Error()})
	}
	r.mu.Lock()
	defer r.mu.Unlock()

	records, base := r.store.Snapshot()
	actual := make(map[string]decl.Resource, len(records))
	for k, rec := range records {
		actual[k] = rec.Resource
	}
	p, err := plan.Compute(d, actual, base)
	if err != nil {
		return nil, fmt.Errorf("reconcile: %w", &StepError{Kind: FailValidation, Detail: err.Error()})
	}
	res := &Result{Plan: p}
	if p.Empty() {
		return res, nil
	}

	downstream := transitiveDependents(d)
	version := base
	for i, step := range p.Steps {
		sr := StepResult{Key: step.Resource.Key(), Action: step.Kind}
		if err := r.executeStep(step, &version); err != nil {
			kind := classify(err)
			sr.Failure = kind
			sr.Detail = err.Error()
			if kind == FailConflict {
				// 既定策略：计划已过期，拒绝继续套用，标记漂移。
				res.Drift = true
				sr.Failure = FailDrift
				sr.Detail = "计划生成后实际状态已被修改，拒绝套用过期计划: " + err.Error()
			}
			res.Failed = &sr
			// 停留在该资源及其下游：其余步骤不再推进。
			for _, rest := range p.Steps[i+1:] {
				skip := StepResult{Key: rest.Resource.Key(), Action: rest.Kind, Failure: FailExecution, Detail: "前序步骤失败，未推进"}
				if downstream[step.Resource.Key()][rest.Resource.Key()] {
					skip.Failure = FailDependencyNotReady
					skip.Detail = fmt.Sprintf("上游 %s 未就绪", step.Resource.Key())
				}
				res.Skipped = append(res.Skipped, skip)
			}
			return res, nil
		}
		res.Applied = append(res.Applied, sr)
	}
	return res, nil
}

// executeStep 执行单个步骤并推进版本号。
func (r *Reconciler) executeStep(step plan.Step, version *uint64) error {
	ctx, cancel := context.WithTimeout(context.Background(), r.timeout)
	defer cancel()
	type outcome struct {
		v   uint64
		err error
	}
	done := make(chan outcome, 1)
	go func() {
		var o outcome
		if r.Hook != nil {
			if err := r.Hook(step); err != nil {
				o.err = err
				done <- o
				return
			}
		}
		switch step.Kind {
		case plan.Create, plan.Update:
			o.v, o.err = r.store.Apply(step.Resource, *version)
		case plan.Delete:
			o.v, o.err = r.store.Delete(step.Resource.Key(), *version)
		}
		done <- o
	}()
	select {
	case o := <-done:
		if o.err != nil {
			return o.err
		}
		*version = o.v
		return nil
	case <-ctx.Done():
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
