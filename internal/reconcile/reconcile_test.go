package reconcile

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/lisama569/gitops-drift-reconciler-go/internal/spec"
	"github.com/lisama569/gitops-drift-reconciler-go/internal/store"
)

func mustParse(t *testing.T, doc string) spec.DesiredState {
	t.Helper()
	d, err := spec.Parse([]byte(doc))
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	return d
}

// logResult 打印变更计划、依赖顺序、状态快照与判定依据，保证结论可解释。
func logResult(t *testing.T, label string, res Result, st *store.Store) {
	t.Helper()
	t.Logf("[%s] converged=%v attempts=%d err=%v", label, res.Converged, res.Attempts, res.Err)
	t.Logf("[%s] plan (dependency execution order, generation=%d):", label, res.Plan.Generation)
	for i, c := range res.Plan.Changes {
		t.Logf("  %d. %-6s %s expectedVersion=%d", i+1, c.Type, c.Resource.Name, c.ExpectedVersion)
	}
	for _, s := range res.Statuses {
		t.Logf("[%s] status: %-8s %-8s %s reason=%q detail=%q",
			label, s.Action, s.Result, s.Name, s.Reason, s.Detail)
	}
	snap := st.Snapshot()
	names := make([]string, 0, len(snap.Resources))
	for n := range snap.Resources {
		names = append(names, n)
	}
	sort.Strings(names)
	t.Logf("[%s] state snapshot (generation=%d):", label, snap.Generation)
	for _, n := range names {
		r := snap.Resources[n]
		t.Logf("  %s kind=%s version=%d managedBy=%q spec=%v", r.Name, r.Kind, r.Version, r.ManagedBy, r.Spec)
	}
}

const chainDoc = `{"resources": [
	{"name": "a-base", "kind": "Namespace", "spec": {"tier": "backend"}},
	{"name": "b-config", "kind": "ConfigMap", "dependsOn": ["a-base"], "spec": {"logLevel": "info"}},
	{"name": "c-app", "kind": "Deployment", "dependsOn": ["b-config"], "spec": {"replicas": 2}}
]}`

func TestConvergeThenIdempotentRerun(t *testing.T) {
	st := store.New()
	rec := New(st, "test", Recompute, 3)
	d := mustParse(t, chainDoc)

	r1 := rec.Reconcile(context.Background(), d)
	logResult(t, "run-1", r1, st)
	if !r1.Converged || r1.Err != nil {
		t.Fatalf("run 1 should converge: %+v", r1)
	}
	for _, name := range []string{"a-base", "b-config", "c-app"} {
		s, ok := r1.Status(name)
		if !ok || s.Result != "applied" {
			t.Fatalf("resource %s: want applied, got %+v", name, s)
		}
	}
	genAfterRun1 := st.Snapshot().Generation

	// 重复执行：不得产生额外变更或副作用。
	r2 := rec.Reconcile(context.Background(), d)
	logResult(t, "run-2", r2, st)
	if !r2.Converged || !r2.Plan.Empty() {
		t.Fatalf("run 2 should be a no-op convergence")
	}
	for _, s := range r2.Statuses {
		if s.Result != "noop" {
			t.Fatalf("run 2 must be all noop, got %+v", s)
		}
	}
	if gen := st.Snapshot().Generation; gen != genAfterRun1 {
		t.Fatalf("idempotent re-run mutated state: generation %d -> %d", genAfterRun1, gen)
	}
	t.Logf("judgment: generation unchanged (%d) after re-run => no side effects", genAfterRun1)
}

func TestPartialFailureThenRetryConverges(t *testing.T) {
	st := store.New()
	rec := New(st, "test", Recompute, 3)
	d := mustParse(t, chainDoc)

	// 注入一次性故障：b-config 首次 apply 时校验失败。
	var failArmed atomic.Bool
	failArmed.Store(true)
	st.SetHook(func(op store.Op, r *store.Resource) error {
		if op.Kind == "apply" && op.Name == "b-config" &&
			failArmed.CompareAndSwap(true, false) {
			return fmt.Errorf("admission rejected spec: %w", ErrValidation)
		}
		return nil
	})

	r1 := rec.Reconcile(context.Background(), d)
	logResult(t, "failing-run", r1, st)
	if r1.Converged || r1.Err == nil {
		t.Fatalf("expected failure, got %+v", r1)
	}
	var f *Failure
	if !errors.As(r1.Err, &f) || f.Reason != ReasonValidation {
		t.Fatalf("want validation failure, got %v", r1.Err)
	}
	// 失败必须停留在该资源及其下游。
	assertStatus(t, r1, "a-base", "applied", "")
	assertStatus(t, r1, "b-config", "failed", ReasonValidation)
	assertStatus(t, r1, "c-app", "skipped", ReasonDependencyNotReady)
	// 已完成资源的状态保持可观察。
	if _, err := st.Get("a-base"); err != nil {
		t.Fatalf("a-base should remain observable: %v", err)
	}
	if _, err := st.Get("c-app"); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("c-app must not be created after upstream failure")
	}

	// 撤掉故障后重试：应收敛到同一结论，且不重复下发 a-base。
	st.SetHook(nil)
	r2 := rec.Reconcile(context.Background(), d)
	logResult(t, "retry-run", r2, st)
	if !r2.Converged || r2.Err != nil {
		t.Fatalf("retry should converge: %+v", r2)
	}
	assertStatus(t, r2, "a-base", "noop", "")
	assertStatus(t, r2, "b-config", "applied", "")
	assertStatus(t, r2, "c-app", "applied", "")

	// 再次重试：完全幂等。
	r3 := rec.Reconcile(context.Background(), d)
	logResult(t, "stable-run", r3, st)
	if !r3.Plan.Empty() {
		t.Fatalf("third run must be empty plan")
	}
}

func TestConcurrentReconcileSameConclusion(t *testing.T) {
	st := store.New()
	d := mustParse(t, chainDoc)

	// 多个独立控制器实例并发作用于同一资源集。
	const n = 8
	results := make([]Result, n)
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			rec := New(st, "test", Recompute, 10)
			results[i] = rec.Reconcile(context.Background(), d)
		}(i)
	}
	wg.Wait()

	for i, r := range results {
		logResult(t, fmt.Sprintf("worker-%d", i), r, st)
		if !r.Converged || r.Err != nil {
			t.Fatalf("worker %d did not converge: err=%v", i, r.Err)
		}
	}
	// 判定依据：3 个资源各只发生一次实际变更（创建），
	// 存储代际必须恰好为 3 —— 证明没有重复下发、没有半生效状态。
	if gen := st.Snapshot().Generation; gen != 3 {
		t.Fatalf("generation = %d, want exactly 3 (one mutation per resource)", gen)
	}
	// 最终结论一致：再跑一次必须全部 noop。
	final := New(st, "test", Recompute, 3).Reconcile(context.Background(), d)
	if !final.Converged || !final.Plan.Empty() {
		t.Fatalf("post-concurrency state not converged: %+v", final)
	}
	t.Logf("judgment: generation=3 after %d concurrent loops => each resource mutated exactly once", n)
}

func TestPreExecutionDriftRecompute(t *testing.T) {
	st := store.New()
	// 预置资源 a-drifted（version=1），期望声明将更新它。
	if _, err := st.Apply("a-drifted", 0, store.Resource{Kind: "ConfigMap", Spec: map[string]any{"v": "old"}, ManagedBy: "test"}); err != nil {
		t.Fatal(err)
	}
	d := mustParse(t, `{"resources": [
		{"name": "a-drifted", "kind": "ConfigMap", "spec": {"v": "new"}},
		{"name": "b-later", "kind": "Deployment", "dependsOn": ["a-drifted"]}
	]}`)

	// 模拟外部 actor：在计划生成后、a-drifted 执行前改动它。
	// 注意用 atomic.Bool 而非 sync.Once：hook 内递归 Apply 会重入 hook，
	// Once.Do 不可重入会导致自死锁。
	var driftArmed atomic.Bool
	driftArmed.Store(true)
	st.SetHook(func(op store.Op, r *store.Resource) error {
		if op.Kind == "apply" && op.Name == "a-drifted" &&
			driftArmed.CompareAndSwap(true, false) {
			if _, err := st.Apply("a-drifted", 1, store.Resource{
				Kind: "ConfigMap", Spec: map[string]any{"v": "external"}, ManagedBy: "test",
			}); err != nil {
				t.Errorf("external drift injection failed: %v", err)
			}
			t.Logf("external actor mutated a-drifted between plan and execute")
		}
		return nil
	})

	rec := New(st, "test", Recompute, 5)
	res := rec.Reconcile(context.Background(), d)
	st.SetHook(nil)
	logResult(t, "drift-recompute", res, st)

	if !res.Converged || res.Err != nil {
		t.Fatalf("should converge after recompute: %+v", res)
	}
	if res.Attempts < 2 {
		t.Fatalf("expected at least 2 attempts (stale plan detected), got %d", res.Attempts)
	}
	// 过期计划不得被套用：最终状态必须等于期望，而不是外部值或混合值。
	got, _ := st.Get("a-drifted")
	if got.Spec["v"] != "new" {
		t.Fatalf("stale plan must not win: spec.v = %v, want new", got.Spec["v"])
	}
	t.Logf("judgment: drift detected, plan recomputed at attempt %d, desired state wins", res.Attempts)
}

func TestPreExecutionDriftReject(t *testing.T) {
	st := store.New()
	if _, err := st.Apply("a-drifted", 0, store.Resource{Kind: "ConfigMap", Spec: map[string]any{"v": "old"}, ManagedBy: "test"}); err != nil {
		t.Fatal(err)
	}
	d := mustParse(t, `{"resources": [{"name": "a-drifted", "kind": "ConfigMap", "spec": {"v": "new"}}]}`)

	var driftArmed atomic.Bool
	driftArmed.Store(true)
	st.SetHook(func(op store.Op, r *store.Resource) error {
		if op.Kind == "apply" && op.Name == "a-drifted" &&
			driftArmed.CompareAndSwap(true, false) {
			_, _ = st.Apply("a-drifted", 1, store.Resource{
				Kind: "ConfigMap", Spec: map[string]any{"v": "external"}, ManagedBy: "test",
			})
		}
		return nil
	})

	rec := New(st, "test", Reject, 3)
	res := rec.Reconcile(context.Background(), d)
	st.SetHook(nil)
	logResult(t, "drift-reject", res, st)

	if res.Converged || res.Err == nil {
		t.Fatalf("reject policy must refuse stale plan: %+v", res)
	}
	var f *Failure
	if !errors.As(res.Err, &f) || f.Reason != ReasonStalePlan {
		t.Fatalf("want stale-plan rejection, got %v", res.Err)
	}
	// 过期计划未被套用：外部改动保持原样。
	got, _ := st.Get("a-drifted")
	if got.Spec["v"] != "external" {
		t.Fatalf("rejected plan must not be applied: spec.v = %v", got.Spec["v"])
	}
	t.Logf("judgment: stale plan rejected, external state untouched")
}

func TestTimeoutClassification(t *testing.T) {
	st := store.New()
	rec := New(st, "test", Recompute, 3)
	d := mustParse(t, chainDoc)

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // 立即超时
	res := rec.Reconcile(ctx, d)
	logResult(t, "timeout", res, st)

	var f *Failure
	if !errors.As(res.Err, &f) || f.Reason != ReasonTimeout {
		t.Fatalf("want timeout failure, got %v", res.Err)
	}
	assertStatus(t, res, "a-base", "failed", ReasonTimeout)
	assertStatus(t, res, "b-config", "skipped", ReasonDependencyNotReady)
}

func TestInvalidDesiredRejectedWithoutSideEffects(t *testing.T) {
	st := store.New()
	rec := New(st, "test", Recompute, 3)
	// 引用不存在的依赖，必须整体拒绝。
	d := spec.DesiredState{Resources: []spec.Resource{
		{Name: "a", Kind: "K"},
		{Name: "b", Kind: "K", DependsOn: []string{"ghost"}},
	}}
	res := rec.Reconcile(context.Background(), d)
	logResult(t, "invalid", res, st)

	var f *Failure
	if !errors.As(res.Err, &f) || f.Reason != ReasonValidation {
		t.Fatalf("want validation failure, got %v", res.Err)
	}
	if gen := st.Snapshot().Generation; gen != 0 {
		t.Fatalf("invalid desired must not mutate store, generation=%d", gen)
	}
}

func TestExtraResourceDeleted(t *testing.T) {
	st := store.New()
	if _, err := st.Apply("obsolete", 0, store.Resource{Kind: "K", ManagedBy: "test"}); err != nil {
		t.Fatal(err)
	}
	if _, err := st.Apply("foreign", 0, store.Resource{Kind: "K", ManagedBy: "other"}); err != nil {
		t.Fatal(err)
	}
	d := mustParse(t, `{"resources": [{"name": "wanted", "kind": "K"}]}`)

	rec := New(st, "test", Recompute, 3)
	res := rec.Reconcile(context.Background(), d)
	logResult(t, "gc", res, st)

	if !res.Converged {
		t.Fatalf("should converge: %+v", res)
	}
	assertStatus(t, res, "obsolete", "deleted", "")
	if _, err := st.Get("obsolete"); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("obsolete resource should be deleted")
	}
	if _, err := st.Get("foreign"); err != nil {
		t.Fatalf("foreign-managed resource must not be touched: %v", err)
	}
}

func assertStatus(t *testing.T, res Result, name, result string, reason FailureReason) {
	t.Helper()
	s, ok := res.Status(name)
	if !ok {
		t.Fatalf("no status for %s", name)
	}
	if s.Result != result || s.Reason != reason {
		t.Fatalf("status %s: want (%s, %q), got (%s, %q)", name, result, reason, s.Result, s.Reason)
	}
}
