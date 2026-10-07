package reconcile

import (
	"context"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/lisama569/gitops-drift-reconciler-go/internal/decl"
	"github.com/lisama569/gitops-drift-reconciler-go/internal/plan"
	"github.com/lisama569/gitops-drift-reconciler-go/internal/store"
)

// wideDecl 构造资源集更大、依赖链更长的声明：
// 1 条 5 级长链 + 1 个 root 带 3 个中间层、每个中间层带 3 个叶子，共 18 个资源。
func wideDecl() *decl.Declaration {
	d := &decl.Declaration{Resources: []decl.Resource{
		{Kind: "App", Name: "chain-0", Spec: map[string]string{"v": "1"}},
	}}
	for i := 1; i < 5; i++ {
		d.Resources = append(d.Resources, decl.Resource{
			Kind: "App", Name: fmt.Sprintf("chain-%d", i),
			DependsOn: []string{fmt.Sprintf("App/chain-%d", i-1)},
			Spec:      map[string]string{"v": "1"},
		})
	}
	d.Resources = append(d.Resources, decl.Resource{Kind: "App", Name: "root", Spec: map[string]string{"v": "1"}})
	for i := 0; i < 3; i++ {
		mid := fmt.Sprintf("mid-%d", i)
		d.Resources = append(d.Resources, decl.Resource{
			Kind: "App", Name: mid, DependsOn: []string{"App/root"},
			Spec: map[string]string{"v": "1"},
		})
		for j := 0; j < 3; j++ {
			d.Resources = append(d.Resources, decl.Resource{
				Kind: "App", Name: fmt.Sprintf("leaf-%d-%d", i, j),
				DependsOn: []string{"App/" + mid},
				Spec:      map[string]string{"v": "1"},
			})
		}
	}
	return d
}

// assertTopoOrder 校验已应用步骤序列满足依赖在前。
// 实例可能只应用了剩余步骤（其余已被其他实例完成），
// 因此只约束双方都出现在本次序列中的依赖对。
func assertTopoOrder(t *testing.T, d *decl.Declaration, applied []StepResult) {
	t.Helper()
	pos := make(map[string]int, len(applied))
	for i, a := range applied {
		pos[a.Key] = i
	}
	for _, res := range d.Resources {
		for _, dep := range res.DependsOn {
			depPos, depOK := pos[dep]
			resPos, resOK := pos[res.Key()]
			if depOK && resOK && depPos >= resPos {
				t.Fatalf("执行顺序违反依赖: %s 应先于 %s: %+v", dep, res.Key(), applied)
			}
		}
	}
}

// actualSpecs 返回当前实际状态 key->spec 的拷贝。
func actualSpecs(s *store.Store) map[string]map[string]string {
	records, _ := s.Snapshot()
	out := make(map[string]map[string]string, len(records))
	for k, rec := range records {
		out[k] = rec.Resource.Spec
	}
	return out
}

// 多实例并发风暴：8 个彼此独立的 Reconciler 共享同一存储、同一份期望，
// 全部使用 replan 策略并发收敛。同一资源全程最多被成功下发一次，
// 执行顺序满足依赖约束，所有实例收敛到同一结论；两轮风暴结果完全一致。
func TestMultiInstanceConcurrentStorm(t *testing.T) {
	const instances = 8
	d := wideDecl()
	wantSpecs := map[string]map[string]string{}
	for _, res := range d.Resources {
		wantSpecs[res.Key()] = res.Spec
	}

	runStorm := func(round int) (map[string]map[string]string, uint64) {
		s := store.New()
		results := make([]*Result, instances)
		var wg sync.WaitGroup
		start := make(chan struct{})
		for i := 0; i < instances; i++ {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				<-start
				// 每个实例是独立的 Reconciler，共享同一存储。
				res, err := New(s).RunContext(context.Background(), d, &Options{DriftPolicy: DriftReplan})
				if err != nil {
					t.Errorf("实例 %d 收敛返回错误: %v", i, err)
					return
				}
				results[i] = res
			}(i)
		}
		close(start)
		wg.Wait()

		var totalReplans int
		for i, res := range results {
			if res == nil {
				t.Fatalf("实例 %d 无结果", i)
			}
			totalReplans += len(res.Replans)
			t.Logf("第 %d 轮 实例 %d: applied=%d replans=%d converged=%v drift=%v",
				round, i, len(res.Applied), len(res.Replans), res.Converged(), res.Drift)
			if !res.Converged() {
				t.Fatalf("第 %d 轮 实例 %d 未收敛:\n%s", round, i, res)
			}
			if res.Drift || res.Canceled {
				t.Fatalf("第 %d 轮 实例 %d 不应漂移或取消:\n%s", round, i, res)
			}
			// 每个实例的执行顺序都必须满足依赖约束。
			assertTopoOrder(t, d, res.Applied)
		}
		t.Logf("第 %d 轮: 共发生 %d 次重规划", round, totalReplans)

		records, ver := s.Snapshot()
		// 存储全局版本 == 成功写入次数；期望资源数 == 写入次数
		// => 每个资源在整个风暴中恰好被成功下发一次，无重复、无遗漏。
		if int(ver) != len(d.Resources) {
			t.Fatalf("第 %d 轮: 成功写入 %d 次，期望恰好 %d 次（每资源一次）", round, ver, len(d.Resources))
		}
		if len(records) != len(d.Resources) {
			t.Fatalf("第 %d 轮: 资源数 %d，期望 %d", round, len(records), len(d.Resources))
		}
		return actualSpecs(s), ver
	}

	specs1, ver1 := runStorm(1)
	specs2, ver2 := runStorm(2)
	// 最终结论必须收敛到同一个结果：终态与期望一致，且两轮完全一致。
	if !reflect.DeepEqual(specs1, wantSpecs) {
		t.Fatalf("第 1 轮终态与期望不一致: %v", specs1)
	}
	if !reflect.DeepEqual(specs1, specs2) || ver1 != ver2 {
		t.Fatalf("两轮风暴结论不一致: %v vs %v (version %d vs %d)", specs1, specs2, ver1, ver2)
	}
	t.Logf("两轮风暴终态一致: version=%d 资源数=%d", ver1, len(specs1))
}

// 漂移处置方式一（默认 abort）：拒绝过期计划、明确报告漂移。
// 显式传入 DriftAbort 与默认 Run 行为一致。
func TestDriftPolicyAbortExplicit(t *testing.T) {
	s := store.New()
	r := New(s)
	d := chainDecl()

	injected := false
	r.Hook = func(step plan.Step) error {
		if !injected && step.Resource.Key() == "App/b" {
			injected = true
			_, ver := s.Snapshot()
			if _, err := s.Apply(decl.Resource{Kind: "External", Name: "actor"}, ver); err != nil {
				t.Fatalf("注入漂移失败: %v", err)
			}
			t.Logf("已在计划与执行之间注入外部变更（App/b 之前）")
		}
		return nil
	}

	res, err := r.RunContext(context.Background(), d, &Options{DriftPolicy: DriftAbort})
	if err != nil {
		t.Fatalf("收敛返回错误: %v", err)
	}
	t.Logf("abort 策略结论:\n%s", res)
	if !res.Drift || res.Failed == nil || res.Failed.Failure != FailDrift {
		t.Fatalf("abort 策略应中止并报告 drift_detected: %+v", res.Failed)
	}
	if res.Policy != DriftAbort {
		t.Fatalf("报告应记录本次策略为 abort: %s", res.Policy)
	}
	if !strings.Contains(res.String(), "driftPolicy=abort") {
		t.Fatalf("报告应可看出使用了 abort 策略:\n%s", res)
	}
	// 过期计划不得被套用：App/b、App/c 不得写入。
	for _, k := range s.Keys() {
		if k == "App/b" || k == "App/c" {
			t.Fatalf("过期计划被套用到 %s", k)
		}
	}
	t.Logf("漂移后状态快照: %s", snapshotLog(t, s))
}

// 漂移处置方式二（replan）：用最新实际状态重新计算计划，接着往下收敛。
func TestDriftPolicyReplan(t *testing.T) {
	s := store.New()
	r := New(s)
	d := chainDecl()

	injected := false
	r.Hook = func(step plan.Step) error {
		if !injected && step.Resource.Key() == "App/b" {
			injected = true
			_, ver := s.Snapshot()
			if _, err := s.Apply(decl.Resource{Kind: "External", Name: "actor"}, ver); err != nil {
				t.Fatalf("注入漂移失败: %v", err)
			}
			t.Logf("已在计划与执行之间注入外部变更（App/b 之前）")
		}
		return nil
	}

	res, err := r.RunContext(context.Background(), d, &Options{DriftPolicy: DriftReplan})
	if err != nil {
		t.Fatalf("收敛返回错误: %v", err)
	}
	t.Logf("replan 策略结论:\n%s", res)
	t.Logf("最终状态快照: %s", snapshotLog(t, s))

	if !res.Converged() {
		t.Fatalf("replan 策略应当场收敛: %s", res)
	}
	if res.Drift {
		t.Fatalf("replan 策略不应标记漂移中止: %s", res)
	}
	if len(res.Replans) != 1 || res.Replans[0].AtKey != "App/b" {
		t.Fatalf("应记录 1 次发生在 App/b 的重规划: %+v", res.Replans)
	}
	if res.Replans[0].NewBase <= res.Replans[0].OldBase {
		t.Fatalf("重规划后版本应推进: %+v", res.Replans[0])
	}
	// 报告中能看出使用了哪种方式、为什么这么判。
	if !strings.Contains(res.String(), "driftPolicy=replan") || !strings.Contains(res.String(), "replan: at=App/b") {
		t.Fatalf("报告应可看出 replan 策略及触发原因:\n%s", res)
	}
	// 最终收敛到期望状态：外部资源作为多余资源被清理，期望资源全部就位。
	if got := s.Keys(); !reflect.DeepEqual(got, []string{"App/a", "App/b", "App/c"}) {
		t.Fatalf("终态应为期望资源集合: %v", got)
	}
	// 每个期望资源恰好下发一次：写入次数 = 3 资源 + 1 外部注入 + 1 次清理。
	_, ver := s.Snapshot()
	if ver != 5 {
		t.Fatalf("成功写入应为 5 次（3 创建 + 1 外部 + 1 删除），实际 %d", ver)
	}
	// 再次执行为空计划（收敛到同一结论）。
	res2, err := r.Run(d)
	if err != nil || !res2.Plan.Empty() {
		t.Fatalf("收敛后再次执行应无变更: %s", res2)
	}
}

// 取消：调用方取消后，被中断资源及其下游停在原地，
// 判定失败后不再产生本次执行的任何写入，重试收敛到同一结论。
func TestCancelStopsRunAndNoLateWrites(t *testing.T) {
	s := store.New()
	r := New(s)
	d := chainDecl()

	ctx, cancel := context.WithCancel(context.Background())
	r.Hook = func(step plan.Step) error {
		if step.Resource.Key() == "App/b" {
			cancel()                          // 调用方在 App/b 执行期间取消
			time.Sleep(20 * time.Millisecond) // 模拟取消传播前的耗时操作
		}
		return nil
	}

	res, err := r.RunContext(ctx, d, nil)
	if err != nil {
		t.Fatalf("取消场景不应返回错误: %v", err)
	}
	t.Logf("取消场景结论:\n%s", res)
	t.Logf("取消后状态快照: %s", snapshotLog(t, s))

	if !res.Canceled || res.Failed == nil || res.Failed.Failure != FailCanceled {
		t.Fatalf("应报告 canceled: %+v", res.Failed)
	}
	if res.Failed.Key != "App/b" {
		t.Fatalf("应停留在 App/b: %+v", res.Failed)
	}
	// 下游 App/c 未推进。
	if len(res.Skipped) != 1 || res.Skipped[0].Key != "App/c" ||
		res.Skipped[0].Failure != FailDependencyNotReady {
		t.Fatalf("下游应标记为 dependency_not_ready: %+v", res.Skipped)
	}
	// 已完成部分保持可观察：只有 App/a 被写入。
	if got := s.Keys(); !reflect.DeepEqual(got, []string{"App/a"}) {
		t.Fatalf("取消后只有 App/a 应存在: %v", got)
	}
	// 终态一致性：判定失败之后，不能再出现本次执行带来的写入。
	_, v1 := s.Snapshot()
	time.Sleep(100 * time.Millisecond) // 等待任何可能的迟到写入
	_, v2 := s.Snapshot()
	if v1 != v2 {
		t.Fatalf("取消判定后出现了迟到写入: version %d -> %d", v1, v2)
	}

	// 重试（新的上下文）收敛到同一结论。
	r.Hook = nil
	res2, err := r.RunContext(context.Background(), d, nil)
	if err != nil || !res2.Converged() {
		t.Fatalf("重试应收敛: %v\n%s", err, res2)
	}
	t.Logf("重试结论:\n%s", res2)
	res3, _ := r.Run(d)
	if !res3.Plan.Empty() {
		t.Fatalf("收敛后再次执行应无变更: %s", res3)
	}
}

// 超时：单步骤超时判定失败后，同样不得出现迟到写入，重试收敛到同一结论。
func TestTimeoutNoLateWrites(t *testing.T) {
	s := store.New()
	r := New(s)
	r.SetStepTimeout(5 * time.Millisecond)
	d := chainDecl()

	r.Hook = func(step plan.Step) error {
		if step.Resource.Key() == "App/b" {
			time.Sleep(50 * time.Millisecond) // 模拟外部系统缓慢
		}
		return nil
	}

	res, err := r.Run(d)
	if err != nil {
		t.Fatalf("超时场景不应返回错误: %v", err)
	}
	t.Logf("超时场景结论:\n%s", res)
	if res.Failed == nil || res.Failed.Failure != FailTimeout || res.Failed.Key != "App/b" {
		t.Fatalf("应停留在 App/b 且原因 timeout: %+v", res.Failed)
	}
	if got := s.Keys(); !reflect.DeepEqual(got, []string{"App/a"}) {
		t.Fatalf("超时后只有 App/a 应存在: %v", got)
	}
	// 终态一致性：等待缓慢的 hook goroutine 结束后，不得出现迟到写入。
	_, v1 := s.Snapshot()
	time.Sleep(100 * time.Millisecond)
	_, v2 := s.Snapshot()
	if v1 != v2 {
		t.Fatalf("超时判定后出现了迟到写入: version %d -> %d", v1, v2)
	}
	t.Logf("超时判定后状态保持稳定: %s", snapshotLog(t, s))

	// 放宽超时重试，收敛到同一结论。
	r.SetStepTimeout(time.Second)
	r.Hook = nil
	res2, err := r.Run(d)
	if err != nil || !res2.Converged() {
		t.Fatalf("重试应收敛: %v\n%s", err, res2)
	}
	t.Logf("重试结论:\n%s", res2)
	res3, _ := r.Run(d)
	if !res3.Plan.Empty() {
		t.Fatalf("收敛后再次执行应无变更: %s", res3)
	}
}
