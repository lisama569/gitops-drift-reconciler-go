// Package reconcile 实现期望状态收敛控制循环：
// 校验 -> 快照 -> 计划 -> 按依赖顺序执行 -> 漂移检测与重试。
//
// 语义要点：
//   - 同一 Reconciler 实例上的 Reconcile 调用被互斥串行化；
//     跨实例的并发由存储层 CAS 兜底，版本冲突即视为执行前漂移。
//   - 执行严格按计划顺序推进；任一资源失败即整体停驻，
//     该资源的下游标记为 dependency-not-ready，其余未执行的标记为 skipped，
//     已完成资源的状态保持可观察。
//   - 重复执行幂等：已一致的资源在计划阶段即为 noop，不会重复下发。
package reconcile

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/lisama569/gitops-drift-reconciler-go/internal/plan"
	"github.com/lisama569/gitops-drift-reconciler-go/internal/spec"
	"github.com/lisama569/gitops-drift-reconciler-go/internal/store"
)

// FailureReason 是可区分的失败分类。
type FailureReason string

const (
	ReasonValidation         FailureReason = "validation"           // 校验失败
	ReasonDependencyNotReady FailureReason = "dependency-not-ready" // 依赖未就绪
	ReasonConflict           FailureReason = "conflict"             // 执行冲突
	ReasonTimeout            FailureReason = "timeout"              // 超时
	ReasonStalePlan          FailureReason = "stale-plan"           // 计划已过期（执行前漂移）
)

// 供故障注入与错误分类使用的哨兵错误。
var (
	ErrValidation = errors.New("reconcile: validation failed")
	ErrConflict   = errors.New("reconcile: execution conflict")
	ErrTimeout    = errors.New("reconcile: timeout")
)

// Failure 是携带分类信息的错误，可用 errors.As 提取。
type Failure struct {
	Reason   FailureReason
	Resource string
	Err      error
}

func (f *Failure) Error() string {
	return fmt.Sprintf("reconcile: %s on resource %q: %v", f.Reason, f.Resource, f.Err)
}

func (f *Failure) Unwrap() error { return f.Err }

// ResourceStatus 记录单个资源在本轮中的结局。
type ResourceStatus struct {
	Name   string
	Action plan.ChangeType
	Result string // "applied" | "deleted" | "noop" | "failed" | "skipped"
	Reason FailureReason
	Detail string
}

// Result 是一轮收敛的完整结论，可观察、可解释。
type Result struct {
	Converged bool
	Plan      plan.Plan
	Statuses  []ResourceStatus
	Attempts  int
	Err       error
}

// Status 按名称查询单个资源的结局。
func (r Result) Status(name string) (ResourceStatus, bool) {
	for _, s := range r.Statuses {
		if s.Name == name {
			return s, true
		}
	}
	return ResourceStatus{}, false
}

// DriftPolicy 决定执行前检测到漂移（计划过期）时的行为。
type DriftPolicy int

const (
	// Recompute 重新计算计划并重试（有次数上限）。
	Recompute DriftPolicy = iota
	// Reject 直接拒绝，返回 ReasonStalePlan。
	Reject
)

// Reconciler 是收敛控制器。
type Reconciler struct {
	st          *store.Store
	managedBy   string
	policy      DriftPolicy
	maxAttempts int
	mu          sync.Mutex // 串行化本实例上的 Reconcile 调用
}

// New 创建控制器。managedBy 标识本控制器管理的资源集合。
// maxAttempts <= 0 时取默认值 3。
func New(st *store.Store, managedBy string, policy DriftPolicy, maxAttempts int) *Reconciler {
	if maxAttempts <= 0 {
		maxAttempts = 3
	}
	return &Reconciler{st: st, managedBy: managedBy, policy: policy, maxAttempts: maxAttempts}
}

// Reconcile 执行收敛循环，直到收敛、失败或超过重试上限。
// 结论（含变更计划、逐资源状态、尝试次数）全部体现在返回值中。
func (r *Reconciler) Reconcile(ctx context.Context, d spec.DesiredState) Result {
	r.mu.Lock()
	defer r.mu.Unlock()

	if err := spec.Validate(d); err != nil {
		return Result{Err: &Failure{Reason: ReasonValidation, Err: err}}
	}

	var last Result
	for attempt := 1; attempt <= r.maxAttempts; attempt++ {
		res, drift := r.attempt(ctx, d)
		res.Attempts = attempt
		if !drift {
			return res
		}
		last = res
		if r.policy == Reject {
			last.Err = &Failure{Reason: ReasonStalePlan, Err: errors.New("plan rejected: actual state changed after planning")}
			return last
		}
		// Recompute：进入下一次尝试，基于最新快照重新计划。
	}
	last.Err = &Failure{Reason: ReasonStalePlan, Err: fmt.Errorf("plan still stale after %d attempts", r.maxAttempts)}
	return last
}

// attempt 执行单次“快照-计划-执行”。drift 为 true 表示检测到执行前漂移，
// 调用方应按策略重试或拒绝。
func (r *Reconciler) attempt(ctx context.Context, d spec.DesiredState) (Result, bool) {
	snap := r.st.Snapshot()
	p, err := plan.Diff(d, snap, r.managedBy)
	if err != nil {
		return Result{Err: &Failure{Reason: ReasonValidation, Err: err}}, false
	}

	res := Result{Plan: p}
	if p.Empty() {
		res.Converged = true
		for _, c := range p.Changes {
			res.Statuses = append(res.Statuses, ResourceStatus{
				Name: c.Resource.Name, Action: c.Type, Result: "noop",
				Detail: "already matches desired state",
			})
		}
		return res, false
	}

	// 记录期望图中每个资源的传递下游，用于失败时标记 dependency-not-ready。
	downstream := transitiveDependents(d)
	isDownstream := func(name, failed string) bool {
		for _, d := range downstream[failed] {
			if d == name {
				return true
			}
		}
		return false
	}

	failed := ""      // 首个失败的资源名
	halted := false   // 是否已停驻
	drift := false    // 是否检测到执行前漂移
	var failErr error // 首个失败的分类错误

	for _, c := range p.Changes {
		name := c.Resource.Name

		if halted {
			st := ResourceStatus{Name: name, Action: c.Type, Result: "skipped",
				Detail: fmt.Sprintf("not attempted: execution halted after failure of %q", failed)}
			if isDownstream(name, failed) {
				st.Reason = ReasonDependencyNotReady
				st.Detail = fmt.Sprintf("dependency %q failed", failed)
			}
			res.Statuses = append(res.Statuses, st)
			continue
		}

		if err := ctx.Err(); err != nil {
			halted, failed = true, name
			failErr = &Failure{Reason: ReasonTimeout, Resource: name, Err: err}
			res.Statuses = append(res.Statuses, ResourceStatus{
				Name: name, Action: c.Type, Result: "failed",
				Reason: ReasonTimeout, Detail: err.Error(),
			})
			continue
		}

		st, ferr, isDrift := r.execute(ctx, c)
		res.Statuses = append(res.Statuses, st)
		switch {
		case isDrift:
			// 执行前漂移：计划基于的实际版本已变化，不得继续套用旧计划。
			drift = true
			halted, failed = true, name
			failErr = ferr
		case ferr != nil:
			halted, failed = true, name
			failErr = ferr
		}
	}
	if drift {
		res.Err = failErr
		return res, true
	}
	if failErr != nil {
		res.Err = failErr
		return res, false
	}
	res.Converged = true
	return res, false
}

// execute 应用单个变更，返回资源状态、分类错误与是否漂移。
func (r *Reconciler) execute(ctx context.Context, c plan.Change) (ResourceStatus, *Failure, bool) {
	name := c.Resource.Name
	st := ResourceStatus{Name: name, Action: c.Type}

	if c.Type == plan.NoOp {
		st.Result = "noop"
		st.Detail = "already matches desired state"
		return st, nil, false
	}

	var err error
	if c.Type == plan.Delete {
		err = r.st.Delete(name, c.ExpectedVersion)
	} else {
		_, err = r.st.Apply(name, c.ExpectedVersion, store.Resource{
			Kind:      c.Resource.Kind,
			Spec:      c.Resource.Spec,
			ManagedBy: r.managedBy,
		})
	}

	if err == nil {
		if c.Type == plan.Delete {
			st.Result = "deleted"
		} else {
			st.Result = "applied"
		}
		st.Detail = "change applied"
		return st, nil, false
	}

	st.Result = "failed"
	// 版本冲突类错误 = 计划生成后实际状态被改动 = 执行前漂移。
	if errors.Is(err, store.ErrVersionConflict) || errors.Is(err, store.ErrAlreadyExists) ||
		errors.Is(err, store.ErrNotFound) {
		st.Reason = ReasonStalePlan
		st.Detail = fmt.Sprintf("actual state changed after planning: %v", err)
		return st, &Failure{Reason: ReasonStalePlan, Resource: name, Err: err}, true
	}
	switch {
	case errors.Is(err, ErrValidation):
		st.Reason = ReasonValidation
	case errors.Is(err, ErrConflict):
		st.Reason = ReasonConflict
	case errors.Is(err, ErrTimeout), errors.Is(err, context.DeadlineExceeded):
		st.Reason = ReasonTimeout
	default:
		st.Reason = ReasonConflict
	}
	st.Detail = err.Error()
	return st, &Failure{Reason: st.Reason, Resource: name, Err: err}, false
}

// dependsOn 报告 name 是否传递依赖于 target。
func dependsOn(d spec.DesiredState, name, target string) bool {
	deps := map[string][]string{}
	for _, r := range d.Resources {
		deps[r.Name] = r.DependsOn
	}
	seen := map[string]bool{}
	var walk func(n string) bool
	walk = func(n string) bool {
		for _, dep := range deps[n] {
			if dep == target {
				return true
			}
			if !seen[dep] {
				seen[dep] = true
				if walk(dep) {
					return true
				}
			}
		}
		return false
	}
	return walk(name)
}

// transitiveDependents 计算每个资源的全部传递下游（保留供日志/解释使用）。
func transitiveDependents(d spec.DesiredState) map[string][]string {
	out := map[string][]string{}
	for _, r := range d.Resources {
		for _, other := range d.Resources {
			if other.Name != r.Name && dependsOn(d, other.Name, r.Name) {
				out[r.Name] = append(out[r.Name], other.Name)
			}
		}
	}
	return out
}
