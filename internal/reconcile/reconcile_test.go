package reconcile

import (
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lisama569/gitops-drift-reconciler-go/internal/decl"
	"github.com/lisama569/gitops-drift-reconciler-go/internal/plan"
	"github.com/lisama569/gitops-drift-reconciler-go/internal/store"
)

func chainDecl() *decl.Declaration {
	return &decl.Declaration{Resources: []decl.Resource{
		{Kind: "App", Name: "a", Spec: map[string]string{"v": "1"}},
		{Kind: "App", Name: "b", DependsOn: []string{"App/a"}, Spec: map[string]string{"v": "1"}},
		{Kind: "App", Name: "c", DependsOn: []string{"App/b"}, Spec: map[string]string{"v": "1"}},
	}}
}

func snapshotLog(t *testing.T, s *store.Store) string {
	t.Helper()
	records, ver := s.Snapshot()
	var b strings.Builder
	fmt.Fprintf(&b, "version=%d ", ver)
	for _, k := range s.Keys() {
		fmt.Fprintf(&b, "%s=%v ", k, records[k].Resource.Spec)
	}
	return b.String()
}

// 重复执行：第二次不得产生任何额外变更。
func TestRepeatedRunIdempotent(t *testing.T) {
	s := store.New()
	r := New(s)
	d := chainDecl()

	res1, err := r.Run(d)
	if err != nil {
		t.Fatalf("第一次收敛失败: %v", err)
	}
	t.Logf("第一次收敛:\n%s", res1)
	t.Logf("状态快照: %s", snapshotLog(t, s))
	if !res1.Converged() || len(res1.Applied) != 3 {
		t.Fatalf("第一次应收敛且应用 3 步: %s", res1)
	}
	// 依赖顺序：a 先于 b 先于 c。
	for i, want := range []string{"App/a", "App/b", "App/c"} {
		if res1.Applied[i].Key != want {
			t.Fatalf("依赖顺序错误，第 %d 步应为 %s: %v", i+1, want, res1.Applied)
		}
	}

	res2, err := r.Run(d)
	if err != nil {
		t.Fatalf("第二次收敛失败: %v", err)
	}
	t.Logf("第二次收敛:\n%s", res2)
	if !res2.Plan.Empty() || len(res2.Applied) != 0 {
		t.Fatalf("重复执行不得产生额外变更: %s", res2)
	}
}

// 部分失败后重试：停留在失败资源及其下游，重试收敛到同一结论，不重复下发。
func TestPartialFailureThenRetry(t *testing.T) {
	s := store.New()
	r := New(s)
	d := chainDecl()

	var applyCount sync.Map // key -> 实际写入次数
	failB := true
	r.Hook = func(step plan.Step) error {
		if failB && step.Resource.Key() == "App/b" {
			return &StepError{Kind: FailExecution, Detail: "模拟执行错误"}
		}
		n, _ := applyCount.LoadOrStore(step.Resource.Key(), new(int32))
		atomic.AddInt32(n.(*int32), 1)
		return nil
	}

	res1, err := r.Run(d)
	if err != nil {
		t.Fatalf("第一次收敛返回错误: %v", err)
	}
	t.Logf("第一次收敛（部分失败）:\n%s", res1)
	t.Logf("状态快照: %s", snapshotLog(t, s))
	if res1.Failed == nil || res1.Failed.Key != "App/b" {
		t.Fatalf("应停留在 App/b: %+v", res1.Failed)
	}
	// 下游 App/c 必须标记为依赖未就绪。
	if len(res1.Skipped) != 1 || res1.Skipped[0].Key != "App/c" ||
		res1.Skipped[0].Failure != FailDependencyNotReady {
		t.Fatalf("下游应标记为 dependency_not_ready: %+v", res1.Skipped)
	}
	// 已完成的 App/a 状态保持可观察。
	if got := s.Keys(); len(got) != 1 || got[0] != "App/a" {
		t.Fatalf("失败后只有 App/a 应存在: %v", got)
	}

	failB = false
	res2, err := r.Run(d)
	if err != nil {
		t.Fatalf("重试收敛失败: %v", err)
	}
	t.Logf("重试收敛:\n%s", res2)
	t.Logf("状态快照: %s", snapshotLog(t, s))
	if !res2.Converged() {
		t.Fatalf("重试应收敛: %s", res2)
	}
	// 每个资源只被实际下发一次，重试不重复下发已完成的 App/a。
	for _, k := range []string{"App/a", "App/b", "App/c"} {
		n, _ := applyCount.Load(k)
		if n == nil || *n.(*int32) != 1 {
			t.Fatalf("资源 %s 被下发 %v 次，期望恰好 1 次", k, n)
		}
	}
	// 再次执行应为空计划（收敛到同一结论）。
	res3, _ := r.Run(d)
	if !res3.Plan.Empty() {
		t.Fatalf("收敛后再次执行应无变更: %s", res3)
	}
}

// 并发触发：同一资源不得被重复下发，最终状态一致。
func TestConcurrentRuns(t *testing.T) {
	s := store.New()
	r := New(s)
	d := chainDecl()

	var applies sync.Map
	r.Hook = func(step plan.Step) error {
		n, _ := applies.LoadOrStore(step.Resource.Key(), new(int32))
		atomic.AddInt32(n.(*int32), 1)
		return nil
	}

	const loops = 8
	var wg sync.WaitGroup
	results := make([]*Result, loops)
	for i := 0; i < loops; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			res, err := r.Run(d)
			if err != nil {
				t.Errorf("并发收敛失败: %v", err)
				return
			}
			results[i] = res
		}(i)
	}
	wg.Wait()

	for i, res := range results {
		t.Logf("循环 %d: applied=%d converged=%v drift=%v", i, len(res.Applied), res.Converged(), res.Drift)
		if !res.Converged() {
			t.Fatalf("循环 %d 未收敛: %s", i, res)
		}
	}
	// 全部循环结束后，每个资源在整个过程中只被下发一次。
	for _, k := range []string{"App/a", "App/b", "App/c"} {
		n, _ := applies.Load(k)
		if n == nil || *n.(*int32) != 1 {
			t.Fatalf("并发下资源 %s 被下发 %v 次，期望恰好 1 次", k, n)
		}
	}
	t.Logf("最终状态快照: %s", snapshotLog(t, s))
}

// 执行前漂移：计划生成后实际状态被外部改动，必须拒绝过期计划，重试后收敛。
func TestDriftBetweenPlanAndApply(t *testing.T) {
	s := store.New()
	r := New(s)
	d := chainDecl()

	injected := false
	r.Hook = func(step plan.Step) error {
		if !injected && step.Resource.Key() == "App/b" {
			injected = true
			// 模拟外部 actor 在计划与执行之间修改实际状态。
			_, ver := s.Snapshot()
			if _, err := s.Apply(decl.Resource{Kind: "External", Name: "actor"}, ver); err != nil {
				t.Fatalf("注入漂移失败: %v", err)
			}
			t.Logf("已在计划与执行之间注入外部变更（App/b 之前）")
		}
		return nil
	}

	res1, err := r.Run(d)
	if err != nil {
		t.Fatalf("第一次收敛返回错误: %v", err)
	}
	t.Logf("第一次收敛（漂移）:\n%s", res1)
	t.Logf("状态快照: %s", snapshotLog(t, s))
	if !res1.Drift {
		t.Fatalf("必须检测到执行前漂移: %s", res1)
	}
	if res1.Failed == nil || res1.Failed.Failure != FailDrift {
		t.Fatalf("失败原因应为 drift_detected: %+v", res1.Failed)
	}
	// 过期计划不得继续套用：App/b、App/c 不得被写入。
	for _, k := range s.Keys() {
		if k == "App/b" || k == "App/c" {
			t.Fatalf("过期计划被套用到 %s", k)
		}
	}

	// 重试：重新计算计划，收敛到同一结论。
	res2, err := r.Run(d)
	if err != nil {
		t.Fatalf("重试收敛失败: %v", err)
	}
	t.Logf("重试收敛:\n%s", res2)
	if !res2.Converged() {
		t.Fatalf("重试应收敛: %s", res2)
	}
	res3, _ := r.Run(d)
	if !res3.Plan.Empty() {
		t.Fatalf("收敛后再次执行应无变更: %s", res3)
	}
}

// 失败分类可区分：校验失败 / 超时。
func TestFailureKindsDistinguishable(t *testing.T) {
	// 校验失败：整体拒绝，不执行任何变更。
	s := store.New()
	r := New(s)
	bad := &decl.Declaration{Resources: []decl.Resource{
		{Kind: "App", Name: "a", DependsOn: []string{"App/ghost"}},
	}}
	_, err := r.Run(bad)
	var se *StepError
	if !errors.As(err, &se) || se.Kind != FailValidation {
		t.Fatalf("应返回 validation 失败: %v", err)
	}
	t.Logf("校验失败判定依据: %v", err)
	if len(s.Keys()) != 0 {
		t.Fatalf("非法声明不得产生任何变更: %v", s.Keys())
	}

	// 超时：步骤执行超过时限。
	s2 := store.New()
	r2 := New(s2)
	r2.SetStepTimeout(5 * time.Millisecond)
	r2.Hook = func(step plan.Step) error {
		time.Sleep(50 * time.Millisecond)
		return nil
	}
	res, err := r2.Run(chainDecl())
	if err != nil {
		t.Fatalf("超时场景不应返回错误: %v", err)
	}
	t.Logf("超时场景:\n%s", res)
	if res.Failed == nil || res.Failed.Failure != FailTimeout {
		t.Fatalf("失败原因应为 timeout: %+v", res.Failed)
	}
}
