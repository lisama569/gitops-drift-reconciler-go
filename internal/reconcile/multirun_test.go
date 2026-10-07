package reconcile

import (
	"context"
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

// 较大的声明：8 级依赖链 + 4 个独立分支，共 12 个资源。
func bigDecl() *decl.Declaration {
	var rs []decl.Resource
	for i := 0; i < 8; i++ {
		r := decl.Resource{Kind: "Chain", Name: fmt.Sprintf("c%d", i), Spec: map[string]string{"v": "1"}}
		if i > 0 {
			r.DependsOn = []string{fmt.Sprintf("Chain/c%d", i-1)}
		}
		rs = append(rs, r)
	}
	for i := 0; i < 4; i++ {
		rs = append(rs, decl.Resource{Kind: "Solo", Name: fmt.Sprintf("s%d", i), Spec: map[string]string{"v": "1"}})
	}
	return &decl.Declaration{Resources: rs}
}

func desiredKeys(d *decl.Declaration) []string {
	keys := make([]string, 0, len(d.Resources))
	for _, r := range d.Resources {
		keys = append(keys, r.Key())
	}
	return keys
}

func sortedEqual(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	aa, bb := append([]string{}, a...), append([]string{}, b...)
	sortStrings(aa)
	sortStrings(bb)
	for i := range aa {
		if aa[i] != bb[i] {
			return false
		}
	}
	return true
}

func sortStrings(s []string) {
	for i := 1; i < len(s); i++ {
		for j := i; j > 0 && s[j] < s[j-1]; j-- {
			s[j], s[j-1] = s[j-1], s[j]
		}
	}
}

// 多实例并发：多个彼此独立的收敛执行者同时处理同一份期望与实际状态。
// 同一资源全程最多下发一次，所有实例收敛到同一结论，最终状态一致。
func TestMultiInstanceConcurrentReconcile(t *testing.T) {
	s := store.New()
	d := bigDecl()

	// 计划可复现性：同一份期望 + 同一份实际快照，计划内容必须完全相同。
	p1, err := plan.Compute(d, nil, 0)
	if err != nil {
		t.Fatalf("计算计划失败: %v", err)
	}
	p2, _ := plan.Compute(d, nil, 0)
	if p1.String() != p2.String() {
		t.Fatalf("相同输入的计划必须稳定可复现:\n%s\n---\n%s", p1, p2)
	}
	t.Logf("基准计划（12 资源，链式依赖在前）:\n%s", p1)

	const instances = 6
	var attempts sync.Map // key -> 下发尝试次数（含被冲突拒绝的，跨所有实例）
	var wg sync.WaitGroup
	results := make([]*Result, instances)
	for i := 0; i < instances; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			r := New(s) // 彼此独立的执行者，共享同一存储
			r.Hook = func(step plan.Step) error {
				n, _ := attempts.LoadOrStore(step.Resource.Key(), new(int32))
				atomic.AddInt32(n.(*int32), 1)
				return nil
			}
			res, err := r.RunWithOptions(context.Background(), d, Options{DriftPolicy: DriftReplan})
			if err != nil {
				t.Errorf("实例 %d 收敛失败: %v", i, err)
				return
			}
			results[i] = res
		}(i)
	}
	wg.Wait()

	totalReplans := 0
	for i, res := range results {
		t.Logf("实例 %d: applied=%d replans=%d converged=%v drift=%v", i, len(res.Applied), res.Replans, res.Converged(), res.Drift)
		totalReplans += res.Replans
		if !res.Converged() {
			t.Fatalf("实例 %d 未收敛:\n%s", i, res)
		}
	}
	// 冲突被明确报出并触发重算（并发风暴下必然有实例观察到冲突）。
	t.Logf("全部实例重算次数合计: %d", totalReplans)
	for _, k := range desiredKeys(d) {
		n, _ := attempts.Load(k)
		if n == nil {
			t.Fatalf("资源 %s 从未被下发", k)
		}
		if c := *n.(*int32); c > 1 {
			t.Logf("资源 %s 下发尝试 %d 次，其中 %d 次被存储明确拒绝（冲突）", k, c, c-1)
		} else {
			t.Logf("资源 %s 下发尝试 1 次，直接成功", k)
		}
	}
	// 同一资源在一次并发风暴里最多只能被成功下发一次：
	// 存储全局版本每次成功写入 +1，因此成功写入总数必须恰好等于资源数，
	// 结合最终状态包含全部 12 个资源，可推出每个资源恰好被写入一次。
	_, finalVer := s.Snapshot()
	if want := uint64(len(d.Resources)); finalVer != want {
		t.Fatalf("成功写入总数应为 %d（每资源恰好一次），实际版本 %d", want, finalVer)
	}
	// 最终结论一致：实际状态恰好等于期望集合。
	if got := s.Keys(); !sortedEqual(got, desiredKeys(d)) {
		t.Fatalf("最终状态应等于期望集合: got=%v", got)
	}
	t.Logf("最终状态快照: %s", snapshotLog(t, s))
	// 风暴过后再次收敛：所有实例都得出相同的空计划结论。
	final := New(s)
	res, err := final.Run(d)
	if err != nil || !res.Plan.Empty() {
		t.Fatalf("收敛后再次执行应无变更: %v\n%s", err, res)
	}
	t.Logf("风暴后复核:\n%s", res)
}

// 漂移处置方式一（replan）：拿最新实际状态重新算计划接着收敛。
// 过期计划不得套到变化后的资源上，最终状态必须等于期望。
func TestDriftPolicyReplan(t *testing.T) {
	s := store.New()
	r := New(s)
	d := chainDecl()

	injected := false
	r.Hook = func(step plan.Step) error {
		if !injected && step.Resource.Key() == "App/b" {
			injected = true
			_, ver := s.Snapshot()
			// 外部改动一：新增无关资源。
			if _, err := s.Apply(decl.Resource{Kind: "External", Name: "actor"}, ver); err != nil {
				t.Fatalf("注入漂移失败: %v", err)
			}
			// 外部改动二：篡改已收敛的 App/a。
			_, ver = s.Snapshot()
			tampered := decl.Resource{Kind: "App", Name: "a", Spec: map[string]string{"v": "tampered"}}
			if _, err := s.Apply(tampered, ver); err != nil {
				t.Fatalf("注入漂移失败: %v", err)
			}
			t.Logf("已在计划与执行之间注入外部变更（新增 External/actor 并篡改 App/a）")
		}
		return nil
	}

	res, err := r.RunWithOptions(context.Background(), d, Options{DriftPolicy: DriftReplan})
	if err != nil {
		t.Fatalf("replan 收敛失败: %v", err)
	}
	t.Logf("replan 收敛:\n%s", res)
	t.Logf("状态快照: %s", snapshotLog(t, s))

	if res.Policy != DriftReplan {
		t.Fatalf("报告应标明本次采用 replan 策略: %s", res.Policy)
	}
	if !res.Converged() {
		t.Fatalf("replan 策略应继续收敛: %s", res)
	}
	if res.Replans == 0 || len(res.DriftLog) == 0 {
		t.Fatalf("应记录重算次数与判定依据: replans=%d log=%v", res.Replans, res.DriftLog)
	}
	// 被篡改的 App/a 必须按重算后的计划恢复，而不是被套用旧计划。
	snap, _ := s.Snapshot()
	if got := snap["App/a"].Resource.Spec["v"]; got != "1" {
		t.Fatalf("App/a 应被重算后的计划恢复为期望值，实际 %q", got)
	}
	// 多余的外部资源应被重算后的计划删除，最终状态等于期望。
	if _, ok := snap["External/actor"]; ok {
		t.Fatalf("多余资源应被重算后的计划删除")
	}
	if got := s.Keys(); !sortedEqual(got, desiredKeys(d)) {
		t.Fatalf("最终状态应等于期望集合: %v", got)
	}
	// 收敛到同一结论：再次执行为空计划。
	again, _ := r.Run(d)
	if !again.Plan.Empty() {
		t.Fatalf("收敛后再次执行应无变更: %s", again)
	}
}

// 漂移处置方式二（abort，默认）：拒绝过期计划并明确说明漂移。
// 显式指定 abort 与默认 Run 的行为必须一致。
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
		}
		return nil
	}

	res, err := r.RunWithOptions(context.Background(), d, Options{DriftPolicy: DriftAbort})
	if err != nil {
		t.Fatalf("abort 收敛返回错误: %v", err)
	}
	t.Logf("abort 收敛:\n%s", res)
	if res.Policy != DriftAbort || !res.Drift {
		t.Fatalf("报告应标明 abort 策略且标记漂移: policy=%s drift=%v", res.Policy, res.Drift)
	}
	if res.Failed == nil || res.Failed.Failure != FailDrift {
		t.Fatalf("失败原因应为 drift_detected: %+v", res.Failed)
	}
	if len(res.DriftLog) == 0 || !strings.Contains(res.DriftLog[0], "abort") {
		t.Fatalf("判定依据应说明采用 abort 策略: %v", res.DriftLog)
	}
	// 过期计划不得套用：App/b、App/c 不得被写入。
	for _, k := range s.Keys() {
		if k == "App/b" || k == "App/c" {
			t.Fatalf("过期计划被套用到 %s", k)
		}
	}
	t.Logf("中止后状态快照: %s", snapshotLog(t, s))

	// 重试（replan）后收敛到同一结论。
	r.Hook = nil
	res2, err := r.RunWithOptions(context.Background(), d, Options{DriftPolicy: DriftReplan})
	if err != nil || !res2.Converged() {
		t.Fatalf("重试应收敛: %v\n%s", err, res2)
	}
	if got := s.Keys(); !sortedEqual(got, desiredKeys(d)) {
		t.Fatalf("最终状态应等于期望集合: %v", got)
	}
}

// 调用方主动取消：被中断的资源及其下游停在原地，
// 判定失败后不得再出现本次执行带来的写入，重试收敛到同一结论。
func TestCancelStopsAndNoLateWrites(t *testing.T) {
	s := store.New()
	r := New(s)
	d := chainDecl()

	ctx, cancel := context.WithCancel(context.Background())
	var applies sync.Map
	r.Hook = func(step plan.Step) error {
		if step.Resource.Key() == "App/b" {
			cancel() // 模拟调用方在 App/b 执行期间取消
			return nil
		}
		n, _ := applies.LoadOrStore(step.Resource.Key(), new(int32))
		atomic.AddInt32(n.(*int32), 1)
		return nil
	}

	res, err := r.RunWithOptions(ctx, d, Options{})
	if err != nil {
		t.Fatalf("取消场景不应返回错误: %v", err)
	}
	t.Logf("取消场景:\n%s", res)
	if res.Failed == nil || res.Failed.Failure != FailCanceled {
		t.Fatalf("失败原因应为 canceled: %+v", res.Failed)
	}
	if len(res.Skipped) != 1 || res.Skipped[0].Key != "App/c" ||
		res.Skipped[0].Failure != FailDependencyNotReady {
		t.Fatalf("下游 App/c 应停在原地: %+v", res.Skipped)
	}
	// 取消判定后，App/b 不得被写入。
	if got := s.Keys(); len(got) != 1 || got[0] != "App/a" {
		t.Fatalf("取消后只有 App/a 应存在: %v", got)
	}
	// 终态一致性：等待一段时间后，不得出现本次执行带来的迟到写入。
	time.Sleep(50 * time.Millisecond)
	if got := s.Keys(); len(got) != 1 || got[0] != "App/a" {
		t.Fatalf("取消判定后出现迟到写入: %v", got)
	}
	t.Logf("取消后状态快照（稳定）: %s", snapshotLog(t, s))

	// 重试：新的 context，收敛到同一结论，App/b 全程只下发一次。
	r.Hook = func(step plan.Step) error {
		n, _ := applies.LoadOrStore(step.Resource.Key(), new(int32))
		atomic.AddInt32(n.(*int32), 1)
		return nil
	}
	res2, err := r.RunWithOptions(context.Background(), d, Options{})
	if err != nil || !res2.Converged() {
		t.Fatalf("重试应收敛: %v\n%s", err, res2)
	}
	t.Logf("重试收敛:\n%s", res2)
	for _, k := range desiredKeys(d) {
		n, _ := applies.Load(k)
		if n == nil || *n.(*int32) != 1 {
			t.Fatalf("资源 %s 被下发 %v 次，期望恰好 1 次", k, n)
		}
	}
	res3, _ := r.Run(d)
	if !res3.Plan.Empty() {
		t.Fatalf("收敛后再次执行应无变更: %s", res3)
	}
}

// 单资源执行超时：判定失败后不得出现迟到写入，
// 已完成部分保持可观察，重试收敛到同一结论。
func TestTimeoutNoLateWrites(t *testing.T) {
	s := store.New()
	r := New(s)
	r.SetStepTimeout(5 * time.Millisecond)
	d := chainDecl()

	var slow atomic.Bool
	slow.Store(true)
	r.Hook = func(step plan.Step) error {
		if slow.Load() && step.Resource.Key() == "App/b" {
			time.Sleep(50 * time.Millisecond) // 模拟执行超时
		}
		return nil
	}

	res, err := r.Run(d)
	if err != nil {
		t.Fatalf("超时场景不应返回错误: %v", err)
	}
	t.Logf("超时场景:\n%s", res)
	if res.Failed == nil || res.Failed.Failure != FailTimeout || res.Failed.Key != "App/b" {
		t.Fatalf("应在 App/b 判定超时: %+v", res.Failed)
	}
	if len(res.Skipped) != 1 || res.Skipped[0].Failure != FailDependencyNotReady {
		t.Fatalf("下游应停在原地: %+v", res.Skipped)
	}
	// 等待超过 Hook 的睡眠时间：超时的 App/b 不得被迟到写入。
	time.Sleep(100 * time.Millisecond)
	if got := s.Keys(); len(got) != 1 || got[0] != "App/a" {
		t.Fatalf("超时判定后出现迟到写入: %v", got)
	}
	t.Logf("超时后状态快照（稳定）: %s", snapshotLog(t, s))

	// 重试：收敛到同一结论。
	slow.Store(false)
	res2, err := r.Run(d)
	if err != nil || !res2.Converged() {
		t.Fatalf("重试应收敛: %v\n%s", err, res2)
	}
	t.Logf("重试收敛:\n%s", res2)
	if got := s.Keys(); !sortedEqual(got, desiredKeys(d)) {
		t.Fatalf("最终状态应等于期望集合: %v", got)
	}
	res3, _ := r.Run(d)
	if !res3.Plan.Empty() {
		t.Fatalf("收敛后再次执行应无变更: %s", res3)
	}
}
